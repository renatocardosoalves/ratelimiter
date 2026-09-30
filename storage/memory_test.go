package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var (
	testPolicy = Policy{Limit: 3, Window: time.Minute, BlockDuration: 2 * time.Minute}
	baseTime   = time.Date(2025, 2, 6, 14, 0, 0, 0, time.UTC)
)

func newTestMemory(t *testing.T, opts ...MemoryOption) *Memory {
	t.Helper()
	m := NewMemory(append([]MemoryOption{WithCleanupInterval(0)}, opts...)...)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func hit(t *testing.T, m *Memory, key string, now time.Time) Record {
	t.Helper()
	rec, err := m.Hit(context.Background(), key, testPolicy, now)
	if err != nil {
		t.Fatalf("Hit returned error: %v", err)
	}
	return rec
}

func TestRecordAdvance(t *testing.T) {
	tests := []struct {
		name string
		rec  Record
		now  time.Time
		want Record
	}{
		{
			name: "first request starts a window",
			rec:  Record{},
			now:  baseTime,
			want: Record{Count: 1, WindowEnd: baseTime.Add(time.Minute)},
		},
		{
			name: "request inside the window increments the count",
			rec:  Record{Count: 1, WindowEnd: baseTime.Add(time.Minute)},
			now:  baseTime.Add(10 * time.Second),
			want: Record{Count: 2, WindowEnd: baseTime.Add(time.Minute)},
		},
		{
			name: "request reaching the limit is not blocked",
			rec:  Record{Count: 2, WindowEnd: baseTime.Add(time.Minute)},
			now:  baseTime.Add(10 * time.Second),
			want: Record{Count: 3, WindowEnd: baseTime.Add(time.Minute)},
		},
		{
			name: "request exceeding the limit blocks the key",
			rec:  Record{Count: 3, WindowEnd: baseTime.Add(time.Minute)},
			now:  baseTime.Add(10 * time.Second),
			want: Record{
				Count:        4,
				WindowEnd:    baseTime.Add(time.Minute),
				BlockedUntil: baseTime.Add(10*time.Second + 2*time.Minute),
			},
		},
		{
			name: "request while blocked is counted and keeps the block",
			rec:  Record{Count: 4, WindowEnd: baseTime.Add(time.Minute), BlockedUntil: baseTime.Add(2 * time.Minute)},
			now:  baseTime.Add(90 * time.Second),
			want: Record{Count: 5, WindowEnd: baseTime.Add(time.Minute), BlockedUntil: baseTime.Add(2 * time.Minute)},
		},
		{
			name: "request after the window expires starts a new window",
			rec:  Record{Count: 3, WindowEnd: baseTime.Add(time.Minute)},
			now:  baseTime.Add(time.Minute),
			want: Record{Count: 1, WindowEnd: baseTime.Add(2 * time.Minute)},
		},
		{
			name: "request after the block expires starts a new window",
			rec:  Record{Count: 10, WindowEnd: baseTime.Add(time.Hour), BlockedUntil: baseTime.Add(time.Minute)},
			now:  baseTime.Add(time.Minute),
			want: Record{Count: 1, WindowEnd: baseTime.Add(2 * time.Minute)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rec.Advance(testPolicy, tt.now); got != tt.want {
				t.Errorf("Advance() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestRecordBlockedAndExpiresAt(t *testing.T) {
	rec := Record{WindowEnd: baseTime.Add(time.Minute)}
	if rec.Blocked(baseTime) {
		t.Error("record without block reported as blocked")
	}
	if got := rec.ExpiresAt(); !got.Equal(rec.WindowEnd) {
		t.Errorf("ExpiresAt() = %v, want window end %v", got, rec.WindowEnd)
	}

	rec.BlockedUntil = baseTime.Add(2 * time.Minute)
	if !rec.Blocked(baseTime.Add(time.Minute)) {
		t.Error("record should be blocked before BlockedUntil")
	}
	if rec.Blocked(rec.BlockedUntil) {
		t.Error("record should not be blocked at BlockedUntil")
	}
	if got := rec.ExpiresAt(); !got.Equal(rec.BlockedUntil) {
		t.Errorf("ExpiresAt() = %v, want block end %v", got, rec.BlockedUntil)
	}
}

func TestMemoryHitCountsAndBlocks(t *testing.T) {
	m := newTestMemory(t)

	for i := 1; i <= testPolicy.Limit; i++ {
		rec := hit(t, m, "client", baseTime)
		if rec.Count != i || rec.Blocked(baseTime) {
			t.Fatalf("hit %d: got %+v, want count %d and not blocked", i, rec, i)
		}
	}

	rec := hit(t, m, "client", baseTime)
	if !rec.Blocked(baseTime) {
		t.Fatalf("hit above limit should block, got %+v", rec)
	}
	if want := baseTime.Add(testPolicy.BlockDuration); !rec.BlockedUntil.Equal(want) {
		t.Errorf("BlockedUntil = %v, want %v", rec.BlockedUntil, want)
	}
}

func TestMemoryKeysAreIndependent(t *testing.T) {
	m := newTestMemory(t)

	for i := 0; i <= testPolicy.Limit; i++ {
		hit(t, m, "a", baseTime)
	}
	if rec := hit(t, m, "b", baseTime); rec.Count != 1 || rec.Blocked(baseTime) {
		t.Errorf("key b affected by key a: %+v", rec)
	}
	if got := m.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2", got)
	}
}

func TestMemoryBlockExpiration(t *testing.T) {
	m := newTestMemory(t)

	for i := 0; i <= testPolicy.Limit; i++ {
		hit(t, m, "client", baseTime)
	}

	stillBlocked := baseTime.Add(testPolicy.BlockDuration - time.Nanosecond)
	if rec := hit(t, m, "client", stillBlocked); !rec.Blocked(stillBlocked) {
		t.Fatalf("expected key to be blocked, got %+v", rec)
	}

	unblocked := baseTime.Add(testPolicy.BlockDuration)
	if rec := hit(t, m, "client", unblocked); rec.Blocked(unblocked) || rec.Count != 1 {
		t.Errorf("expected a fresh window after the block, got %+v", rec)
	}
}

func TestMemoryHitCanceledContext(t *testing.T) {
	m := newTestMemory(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := m.Hit(ctx, "client", testPolicy, baseTime); !errors.Is(err, context.Canceled) {
		t.Errorf("Hit error = %v, want context.Canceled", err)
	}
	if m.Len() != 0 {
		t.Error("canceled hit must not be stored")
	}
}

func TestMemoryConcurrentHitsAreAtomic(t *testing.T) {
	m := newTestMemory(t, WithShards(4))
	policy := Policy{Limit: 1_000_000, Window: time.Hour, BlockDuration: time.Minute}

	const (
		keys       = 8
		goroutines = 50
		hitsEach   = 200
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < hitsEach; i++ {
				key := fmt.Sprintf("client-%d", i%keys)
				if _, err := m.Hit(context.Background(), key, policy, baseTime); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	want := goroutines * hitsEach / keys
	for k := 0; k < keys; k++ {
		rec, _ := m.Hit(context.Background(), fmt.Sprintf("client-%d", k), policy, baseTime)
		if rec.Count != want+1 {
			t.Errorf("client-%d count = %d, want %d", k, rec.Count-1, want)
		}
	}
}

func TestMemoryConcurrentHitsBlockExactlyAtLimit(t *testing.T) {
	m := newTestMemory(t)
	policy := Policy{Limit: 100, Window: time.Hour, BlockDuration: time.Hour}

	const total = 1000
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allowed int
	)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec, err := m.Hit(context.Background(), "client", policy, baseTime)
			if err != nil {
				t.Error(err)
				return
			}
			if !rec.Blocked(baseTime) {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed != policy.Limit {
		t.Errorf("allowed = %d, want exactly %d", allowed, policy.Limit)
	}
}

func TestMemoryRemoveExpired(t *testing.T) {
	m := newTestMemory(t)

	hit(t, m, "expired", baseTime)
	for i := 0; i <= testPolicy.Limit; i++ {
		hit(t, m, "blocked", baseTime)
	}
	hit(t, m, "active", baseTime.Add(30*time.Second))

	m.removeExpired(baseTime.Add(time.Minute))
	if got := m.Len(); got != 2 {
		t.Fatalf("Len() after first cleanup = %d, want 2 (blocked and active)", got)
	}

	m.removeExpired(baseTime.Add(testPolicy.BlockDuration))
	if got := m.Len(); got != 0 {
		t.Errorf("Len() after second cleanup = %d, want 0", got)
	}
}

func TestMemoryBackgroundCleanup(t *testing.T) {
	m := NewMemory(WithCleanupInterval(10 * time.Millisecond))
	defer m.Close()

	policy := Policy{Limit: 10, Window: time.Millisecond, BlockDuration: time.Millisecond}
	if _, err := m.Hit(context.Background(), "client", policy, time.Now()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for m.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("expired record was not removed by the background cleanup")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMemoryCloseIsIdempotent(t *testing.T) {
	for _, interval := range []time.Duration{0, time.Millisecond} {
		m := NewMemory(WithCleanupInterval(interval))
		if err := m.Close(); err != nil {
			t.Fatalf("first Close: %v", err)
		}
		if err := m.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	}
}

func TestWithShards(t *testing.T) {
	tests := []struct {
		in   int
		want int
	}{
		{in: 0, want: DefaultShards},
		{in: -5, want: DefaultShards},
		{in: 1, want: 1},
		{in: 3, want: 4},
		{in: 64, want: 64},
		{in: 100, want: 128},
	}
	for _, tt := range tests {
		m := NewMemory(WithShards(tt.in), WithCleanupInterval(0))
		if got := len(m.shards); got != tt.want {
			t.Errorf("WithShards(%d): %d shards, want %d", tt.in, got, tt.want)
		}
		_ = m.Close()
	}
}

func BenchmarkMemoryHitParallel(b *testing.B) {
	m := NewMemory(WithCleanupInterval(0))
	defer m.Close()
	policy := Policy{Limit: 100, Window: time.Minute, BlockDuration: time.Minute}
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("10.0.%d.%d", i/256, i%256)
	}
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = m.Hit(context.Background(), keys[i&1023], policy, now)
			i++
		}
	})
}
