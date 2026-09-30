package ratelimiter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/renatocardosoalves/ratelimiter/storage"
)

var baseTime = time.Date(2025, 2, 6, 14, 0, 0, 0, time.UTC)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: baseTime} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type failingStorage struct{ err error }

func (f failingStorage) Hit(context.Context, string, storage.Policy, time.Time) (storage.Record, error) {
	return storage.Record{}, f.err
}

type closableStorage struct {
	storage.Storage
	closed bool
}

func (c *closableStorage) Close() error {
	c.closed = true
	return nil
}

func newTestLimiter(t *testing.T, clock *fakeClock, opts ...Option) *Limiter {
	t.Helper()
	base := []Option{WithClock(clock.Now), WithLogger(nil)}
	l, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func allow(t *testing.T, l *Limiter, key string) Result {
	t.Helper()
	res, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return res
}

func TestNewDefaults(t *testing.T) {
	l, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	want := storage.Policy{Limit: 100, Window: time.Minute, BlockDuration: time.Minute}
	if got := l.Policy(); got != want {
		t.Errorf("Policy() = %+v, want %+v", got, want)
	}
	if _, ok := l.store.(*storage.Memory); !ok {
		t.Errorf("default storage = %T, want *storage.Memory", l.store)
	}
	if !l.ownsStore {
		t.Error("limiter should own the default storage")
	}
}

func TestNewWithOptions(t *testing.T) {
	store := storage.NewMemory(storage.WithCleanupInterval(0))
	defer store.Close()

	l, err := New(
		WithLimit(10),
		WithWindow(30*time.Second),
		WithBlockDuration(5*time.Minute),
		WithStorage(store),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	want := storage.Policy{Limit: 10, Window: 30 * time.Second, BlockDuration: 5 * time.Minute}
	if got := l.Policy(); got != want {
		t.Errorf("Policy() = %+v, want %+v", got, want)
	}
	if l.store != store || l.ownsStore {
		t.Error("custom storage should be used and not owned")
	}
}

func TestWithRate(t *testing.T) {
	l, err := New(WithRate(100, 10*time.Minute))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	if p := l.Policy(); p.Limit != 100 || p.Window != 10*time.Minute {
		t.Errorf("Policy() = %+v, want 100 requests per 10m", p)
	}
}

func TestNewInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opt  Option
	}{
		{"zero limit", WithLimit(0)},
		{"negative limit", WithLimit(-1)},
		{"zero window", WithWindow(0)},
		{"negative window", WithWindow(-time.Second)},
		{"zero block duration", WithBlockDuration(0)},
		{"negative block duration", WithBlockDuration(-time.Second)},
		{"invalid rate limit", WithRate(0, time.Minute)},
		{"invalid rate window", WithRate(10, 0)},
		{"nil storage", WithStorage(nil)},
		{"nil clock", WithClock(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := New(tt.opt)
			if err == nil {
				t.Fatal("expected an error")
			}
			if l != nil {
				t.Error("expected a nil limiter on error")
			}
		})
	}

	if _, err := New(WithStorage(nil)); !errors.Is(err, ErrNilStorage) {
		t.Errorf("nil storage error = %v, want ErrNilStorage", err)
	}
}

func TestAllowUnderLimit(t *testing.T) {
	clock := newFakeClock()
	l := newTestLimiter(t, clock, WithLimit(3))

	for i := 1; i <= 3; i++ {
		res := allow(t, l, "1.2.3.4")
		if !res.Allowed {
			t.Fatalf("request %d should be allowed", i)
		}
		if res.RequestsMade != i || res.Remaining != 3-i || res.Limit != 3 || res.Key != "1.2.3.4" {
			t.Errorf("request %d: unexpected result %+v", i, res)
		}
		if !res.ResetAt.Equal(baseTime.Add(DefaultWindow)) {
			t.Errorf("ResetAt = %v, want %v", res.ResetAt, baseTime.Add(DefaultWindow))
		}
		if !res.RetryAt.IsZero() || res.RetryAfter != 0 {
			t.Errorf("allowed request must not carry retry info: %+v", res)
		}
	}
}

func TestAllowBlocksAfterLimit(t *testing.T) {
	clock := newFakeClock()
	l := newTestLimiter(t, clock, WithLimit(2), WithBlockDuration(time.Minute))

	allow(t, l, "ip")
	allow(t, l, "ip")

	res := allow(t, l, "ip")
	if res.Allowed {
		t.Fatal("request above the limit should be rejected")
	}
	if res.RequestsMade != 3 || res.Remaining != 0 {
		t.Errorf("unexpected counters: %+v", res)
	}
	if !res.RetryAt.Equal(baseTime.Add(time.Minute)) || res.RetryAfter != time.Minute {
		t.Errorf("retry info = (%v, %v), want (%v, 1m)", res.RetryAt, res.RetryAfter, baseTime.Add(time.Minute))
	}
}

func TestAllowRejectsEveryRequestWhileBlocked(t *testing.T) {
	clock := newFakeClock()
	l := newTestLimiter(t, clock, WithLimit(1), WithBlockDuration(time.Minute))

	allow(t, l, "ip")
	allow(t, l, "ip")

	for i := 0; i < 5; i++ {
		clock.Advance(10 * time.Second)
		res := allow(t, l, "ip")
		if res.Allowed {
			t.Fatalf("request %d during the block should be rejected", i)
		}
		wantRetryAfter := time.Minute - time.Duration(i+1)*10*time.Second
		if res.RetryAfter != wantRetryAfter {
			t.Errorf("RetryAfter = %v, want %v", res.RetryAfter, wantRetryAfter)
		}
		if res.RequestsMade != 3+i {
			t.Errorf("RequestsMade = %d, want %d", res.RequestsMade, 3+i)
		}
	}
}

func TestAllowUnblocksAfterBlockDuration(t *testing.T) {
	clock := newFakeClock()
	l := newTestLimiter(t, clock,
		WithLimit(1),
		WithWindow(time.Hour),
		WithBlockDuration(time.Minute),
	)

	allow(t, l, "ip")
	if allow(t, l, "ip").Allowed {
		t.Fatal("expected block")
	}

	clock.Advance(time.Minute)
	res := allow(t, l, "ip")
	if !res.Allowed || res.RequestsMade != 1 {
		t.Errorf("after the block a new window should start, got %+v", res)
	}
}

func TestAllowWindowReset(t *testing.T) {
	clock := newFakeClock()
	l := newTestLimiter(t, clock, WithLimit(2), WithWindow(10*time.Second))

	allow(t, l, "ip")
	allow(t, l, "ip")

	clock.Advance(10 * time.Second)
	res := allow(t, l, "ip")
	if !res.Allowed || res.RequestsMade != 1 || res.Remaining != 1 {
		t.Errorf("expected a new window, got %+v", res)
	}
}

func TestAllowKeysAreIndependent(t *testing.T) {
	clock := newFakeClock()
	l := newTestLimiter(t, clock, WithLimit(1))

	allow(t, l, "a")
	if allow(t, l, "a").Allowed {
		t.Fatal("key a should be blocked")
	}
	if !allow(t, l, "b").Allowed {
		t.Error("key b should not be affected by key a")
	}
}

func TestAllowStorageError(t *testing.T) {
	wantErr := errors.New("storage down")
	l, err := New(WithStorage(failingStorage{err: wantErr}), WithLogger(nil))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := l.Allow(context.Background(), "ip"); !errors.Is(err, wantErr) {
		t.Errorf("Allow error = %v, want %v", err, wantErr)
	}
}

func TestAllowConcurrent(t *testing.T) {
	clock := newFakeClock()
	l := newTestLimiter(t, clock, WithLimit(100), WithBlockDuration(time.Hour))

	const total = 1000
	var allowed, rejected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := l.Allow(context.Background(), "ip")
			if err != nil {
				t.Error(err)
				return
			}
			if res.Allowed {
				allowed.Add(1)
			} else {
				rejected.Add(1)
			}
		}()
	}
	wg.Wait()

	if allowed.Load() != 100 || rejected.Load() != total-100 {
		t.Errorf("allowed=%d rejected=%d, want 100 and %d", allowed.Load(), rejected.Load(), total-100)
	}
	if res := allow(t, l, "ip"); res.RequestsMade != total+1 {
		t.Errorf("RequestsMade = %d, want %d", res.RequestsMade, total+1)
	}
}

func TestCloseOwnership(t *testing.T) {
	custom := &closableStorage{Storage: storage.NewMemory(storage.WithCleanupInterval(0))}
	l, err := New(WithStorage(custom))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if custom.closed {
		t.Error("limiter must not close a storage it does not own")
	}

	owned, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := owned.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := owned.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("invalid log line %q: %v", line, err)
		}
		lines = append(lines, m)
	}
	return lines
}

func TestLoggingDefaultLevelOnlyWhenLimitExceeded(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	clock := newFakeClock()
	l := newTestLimiter(t, clock, WithLimit(2), WithLogger(logger))

	for i := 0; i < 5; i++ {
		allow(t, l, "10.0.0.1")
	}

	lines := logLines(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1: %s", len(lines), buf.String())
	}
	got := lines[0]
	checks := map[string]any{
		"level":          "WARN",
		"msg":            "rate limit exceeded",
		"key":            "10.0.0.1",
		"requests_made":  float64(3),
		"limit":          float64(2),
		"retry_at":       baseTime.Add(DefaultBlockDuration).Format(time.RFC3339),
		"retry_after":    float64(DefaultBlockDuration),
		"window":         float64(DefaultWindow),
		"block_duration": float64(DefaultBlockDuration),
	}
	for k, want := range checks {
		if got[k] != want {
			t.Errorf("log field %q = %v, want %v", k, got[k], want)
		}
	}
}

func TestLoggingDebugLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	clock := newFakeClock()
	l := newTestLimiter(t, clock, WithLimit(1), WithLogger(logger))

	allow(t, l, "ip")
	allow(t, l, "ip")
	allow(t, l, "ip")

	lines := logLines(t, &buf)
	wantMsgs := []string{"request allowed", "rate limit exceeded", "request rejected: client blocked"}
	if len(lines) != len(wantMsgs) {
		t.Fatalf("got %d log lines, want %d: %s", len(lines), len(wantMsgs), buf.String())
	}
	for i, want := range wantMsgs {
		if lines[i]["msg"] != want {
			t.Errorf("line %d msg = %v, want %q", i, lines[i]["msg"], want)
		}
	}
}

func TestWithLoggerNilDisablesLogging(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(previous)

	clock := newFakeClock()
	l := newTestLimiter(t, clock, WithLimit(1), WithLogger(nil))
	allow(t, l, "ip")
	allow(t, l, "ip")

	if buf.Len() != 0 {
		t.Errorf("expected no logs, got %q", buf.String())
	}
}

func TestDefaultLoggerIsSlogDefault(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(previous)

	l, err := New(WithLimit(1))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()
	allow(t, l, "ip")
	allow(t, l, "ip")

	if !strings.Contains(buf.String(), "rate limit exceeded") {
		t.Errorf("expected the default logger to be used, got %q", buf.String())
	}
}

func BenchmarkAllowParallel(b *testing.B) {
	l, err := New(WithLimit(1_000_000_000), WithLogger(nil))
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = l.Allow(context.Background(), "127.0.0.1")
		}
	})
}
