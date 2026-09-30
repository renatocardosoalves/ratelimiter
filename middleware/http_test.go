package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/renatocardosoalves/ratelimiter/ratelimiter"
)

var baseTime = time.Date(2025, 2, 6, 14, 29, 0, 0, time.UTC)

type stubLimiter struct {
	res ratelimiter.Result
	err error
	key string
}

func (s *stubLimiter) Allow(_ context.Context, key string) (ratelimiter.Result, error) {
	s.key = key
	return s.res, s.err
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"message":"ok"}`)
	})
}

func newLimiter(t *testing.T, opts ...ratelimiter.Option) *ratelimiter.Limiter {
	t.Helper()
	base := []ratelimiter.Option{
		ratelimiter.WithLogger(nil),
		ratelimiter.WithClock(func() time.Time { return baseTime }),
	}
	l, err := ratelimiter.New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("ratelimiter.New: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func do(h http.Handler, remoteAddr string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = remoteAddr
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func blockedResult() ratelimiter.Result {
	return ratelimiter.Result{
		Key:          "1.2.3.4",
		Allowed:      false,
		Limit:        100,
		RequestsMade: 120,
		ResetAt:      baseTime,
		RetryAt:      time.Date(2025, 2, 6, 14, 30, 0, 0, time.UTC),
		RetryAfter:   60 * time.Second,
	}
}

func TestAllowedRequestPassesThrough(t *testing.T) {
	h := RateLimit(newLimiter(t, ratelimiter.WithLimit(2)))(okHandler())

	rec := do(h, "1.2.3.4:5000")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != `{"message":"ok"}` {
		t.Errorf("body = %q", rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "" {
		t.Error("allowed response must not include Retry-After")
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "2" {
		t.Errorf("X-RateLimit-Limit = %q, want 2", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "1" {
		t.Errorf("X-RateLimit-Remaining = %q, want 1", got)
	}
	wantReset := strconv.FormatInt(baseTime.Add(ratelimiter.DefaultWindow).Unix(), 10)
	if got := rec.Header().Get("X-RateLimit-Reset"); got != wantReset {
		t.Errorf("X-RateLimit-Reset = %q, want %s", got, wantReset)
	}
}

func TestBlockedRequestDefaultResponse(t *testing.T) {
	h := RateLimit(newLimiter(t, ratelimiter.WithLimit(2), ratelimiter.WithBlockDuration(time.Minute)))(okHandler())

	for i := 0; i < 2; i++ {
		if rec := do(h, "1.2.3.4:5000"); rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, rec.Code)
		}
	}

	rec := do(h, "1.2.3.4:5000")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "60" {
		t.Errorf("Retry-After = %q, want 60", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want 0", got)
	}
	wantReset := strconv.FormatInt(baseTime.Add(time.Minute).Unix(), 10)
	if got := rec.Header().Get("X-RateLimit-Reset"); got != wantReset {
		t.Errorf("X-RateLimit-Reset = %q, want %s", got, wantReset)
	}

	var body ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body %q: %v", rec.Body.String(), err)
	}
	want := ErrorResponse{
		Error:        "Rate limit exceeded",
		Limit:        2,
		RequestsMade: 3,
		RetryAfter:   "2025-02-06T14:30:00.000Z",
	}
	if body != want {
		t.Errorf("body = %+v, want %+v", body, want)
	}
}

func TestDefaultBodyMatchesSpecification(t *testing.T) {
	h := RateLimit(&stubLimiter{res: blockedResult()})(okHandler())

	rec := do(h, "1.2.3.4:5000")
	want := `{"error":"Rate limit exceeded","limit":100,"requests_made":120,"retry_after":"2025-02-06T14:30:00.000Z"}`
	if got := rec.Body.String(); got != want {
		t.Errorf("body = %s\nwant %s", got, want)
	}
	if got := rec.Header().Get("Retry-After"); got != "60" {
		t.Errorf("Retry-After = %q, want 60", got)
	}
}

func TestRetryAfterIsConvertedToUTC(t *testing.T) {
	res := blockedResult()
	res.RetryAt = res.RetryAt.In(time.FixedZone("BRT", -3*60*60))
	h := RateLimit(&stubLimiter{res: res})(okHandler())

	var body ErrorResponse
	if err := json.Unmarshal(do(h, "1.2.3.4:1").Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.RetryAfter != "2025-02-06T14:30:00.000Z" {
		t.Errorf("retry_after = %q, want UTC time", body.RetryAfter)
	}
}

func TestRetryAfterHeaderRounding(t *testing.T) {
	tests := []struct {
		in   time.Duration
		want string
	}{
		{in: 60 * time.Second, want: "60"},
		{in: 59*time.Second + time.Millisecond, want: "60"},
		{in: 1500 * time.Millisecond, want: "2"},
		{in: time.Millisecond, want: "1"},
		{in: 0, want: "1"},
	}
	for _, tt := range tests {
		res := blockedResult()
		res.RetryAfter = tt.in
		rec := do(RateLimit(&stubLimiter{res: res})(okHandler()), "1.2.3.4:1")
		if got := rec.Header().Get("Retry-After"); got != tt.want {
			t.Errorf("RetryAfter %v: header = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestWithJSONResponseDisabled(t *testing.T) {
	h := RateLimit(&stubLimiter{res: blockedResult()}, WithJSONResponse(false))(okHandler())

	rec := do(h, "1.2.3.4:5000")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "60" {
		t.Error("Retry-After must be sent even without JSON body")
	}
}

func TestWithErrorMessage(t *testing.T) {
	h := RateLimit(&stubLimiter{res: blockedResult()}, WithErrorMessage("Calma aí!"))(okHandler())

	var body ErrorResponse
	if err := json.Unmarshal(do(h, "1.2.3.4:1").Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error != "Calma aí!" || body.Limit != 100 || body.RequestsMade != 120 {
		t.Errorf("unexpected body %+v", body)
	}
}

func TestWithResponseBody(t *testing.T) {
	type customBody struct {
		Message string `json:"message"`
		Wait    int    `json:"wait_seconds"`
	}
	h := RateLimit(&stubLimiter{res: blockedResult()},
		WithResponseBody(func(res ratelimiter.Result) customBody {
			return customBody{Message: "slow down", Wait: int(res.RetryAfter.Seconds())}
		}),
	)(okHandler())

	rec := do(h, "1.2.3.4:1")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got, want := rec.Body.String(), `{"message":"slow down","wait_seconds":60}`; got != want {
		t.Errorf("body = %s, want %s", got, want)
	}
}

func TestWithResponseBodyRespectsJSONToggle(t *testing.T) {
	h := RateLimit(&stubLimiter{res: blockedResult()},
		WithResponseBody(func(ratelimiter.Result) string { return "custom" }),
		WithJSONResponse(false),
	)(okHandler())

	if rec := do(h, "1.2.3.4:1"); rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
}

func TestWithResponseBodyEncodingError(t *testing.T) {
	h := RateLimit(&stubLimiter{res: blockedResult()},
		WithResponseBody(func(ratelimiter.Result) chan int { return make(chan int) }),
	)(okHandler())

	rec := do(h, "1.2.3.4:1")
	if rec.Code != http.StatusTooManyRequests || rec.Body.Len() != 0 {
		t.Errorf("got status %d body %q, want 429 without body", rec.Code, rec.Body.String())
	}
}

func TestWithDeniedHandler(t *testing.T) {
	var got ratelimiter.Result
	h := RateLimit(&stubLimiter{res: blockedResult()},
		WithDeniedHandler(func(w http.ResponseWriter, _ *http.Request, res ratelimiter.Result) {
			got = res
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "custom")
		}),
	)(okHandler())

	rec := do(h, "1.2.3.4:1")
	if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != "custom" {
		t.Errorf("got %d %q, want the custom response", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "60" {
		t.Error("Retry-After must be set before calling the denied handler")
	}
	if got.RequestsMade != 120 {
		t.Errorf("denied handler received %+v", got)
	}
}

func TestWithRateLimitHeadersDisabled(t *testing.T) {
	h := RateLimit(&stubLimiter{res: blockedResult()}, WithRateLimitHeaders(false))(okHandler())

	rec := do(h, "1.2.3.4:1")
	for _, name := range []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"} {
		if rec.Header().Get(name) != "" {
			t.Errorf("header %s should not be set", name)
		}
	}
	if rec.Header().Get("Retry-After") != "60" {
		t.Error("Retry-After must always be sent")
	}
}

func TestDifferentIPsAreLimitedIndependently(t *testing.T) {
	h := RateLimit(newLimiter(t, ratelimiter.WithLimit(1)))(okHandler())

	if rec := do(h, "1.1.1.1:1000"); rec.Code != http.StatusOK {
		t.Fatalf("first client first request: %d", rec.Code)
	}
	if rec := do(h, "1.1.1.1:2000"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("same IP with another port should share the limit, got %d", rec.Code)
	}
	if rec := do(h, "2.2.2.2:1000"); rec.Code != http.StatusOK {
		t.Errorf("second client should not be blocked, got %d", rec.Code)
	}
}

func TestLimiterErrorUsesErrorHandler(t *testing.T) {
	storageErr := errors.New("storage down")

	rec := do(RateLimit(&stubLimiter{err: storageErr})(okHandler()), "1.2.3.4:1")
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("default error handler status = %d, want 500", rec.Code)
	}

	var got error
	h := RateLimit(&stubLimiter{err: storageErr},
		WithErrorHandler(func(w http.ResponseWriter, _ *http.Request, err error) {
			got = err
			w.WriteHeader(http.StatusBadGateway)
		}),
	)(okHandler())
	rec = do(h, "1.2.3.4:1")
	if rec.Code != http.StatusBadGateway || !errors.Is(got, storageErr) {
		t.Errorf("custom error handler not used: status %d err %v", rec.Code, got)
	}
}

func TestKeyFuncErrorUsesErrorHandler(t *testing.T) {
	stub := &stubLimiter{res: ratelimiter.Result{Allowed: true}}
	var got error
	h := RateLimit(stub,
		WithKeyFunc(KeyByHeader("X-API-Key")),
		WithErrorHandler(func(w http.ResponseWriter, _ *http.Request, err error) {
			got = err
			w.WriteHeader(http.StatusUnauthorized)
		}),
	)(okHandler())

	rec := do(h, "1.2.3.4:1")
	if rec.Code != http.StatusUnauthorized || !errors.Is(got, ErrEmptyKey) {
		t.Errorf("got status %d err %v, want 401 and ErrEmptyKey", rec.Code, got)
	}
	if stub.key != "" {
		t.Error("limiter must not be called when the key cannot be extracted")
	}
}

func TestWithKeyFunc(t *testing.T) {
	stub := &stubLimiter{res: ratelimiter.Result{Allowed: true}}
	h := RateLimit(stub, WithKeyFunc(KeyByHeader("X-API-Key")))(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "token-123")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if stub.key != "token-123" {
		t.Errorf("key = %q, want token-123", stub.key)
	}
}

func TestNilOptionsKeepDefaults(t *testing.T) {
	stub := &stubLimiter{err: errors.New("boom")}
	h := RateLimit(stub,
		WithKeyFunc(nil),
		WithErrorHandler(nil),
		WithResponseBody[string](nil),
	)(okHandler())

	rec := do(h, "9.9.9.9:1")
	if stub.key != "9.9.9.9" {
		t.Errorf("key = %q, want default IP key", stub.key)
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want default error handler 500", rec.Code)
	}
}

func TestKeyByIP(t *testing.T) {
	tests := []struct {
		remoteAddr string
		want       string
		wantErr    bool
	}{
		{remoteAddr: "192.168.0.1:1234", want: "192.168.0.1"},
		{remoteAddr: "[::1]:8080", want: "::1"},
		{remoteAddr: "[2001:db8::1]:443", want: "2001:db8::1"},
		{remoteAddr: "10.0.0.1", want: "10.0.0.1"},
		{remoteAddr: "", wantErr: true},
	}
	for _, tt := range tests {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = tt.remoteAddr
		got, err := KeyByIP(req)
		if (err != nil) != tt.wantErr {
			t.Errorf("KeyByIP(%q) error = %v, wantErr %v", tt.remoteAddr, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("KeyByIP(%q) = %q, want %q", tt.remoteAddr, got, tt.want)
		}
	}
}

func TestKeyByForwardedIP(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    string
	}{
		{name: "first X-Forwarded-For entry", headers: map[string]string{"X-Forwarded-For": " 203.0.113.7 , 10.0.0.1"}, want: "203.0.113.7"},
		{name: "X-Real-IP", headers: map[string]string{"X-Real-IP": "198.51.100.2"}, want: "198.51.100.2"},
		{name: "empty X-Forwarded-For falls back to X-Real-IP", headers: map[string]string{"X-Forwarded-For": " ,1.1.1.1", "X-Real-IP": "198.51.100.2"}, want: "198.51.100.2"},
		{name: "fallback to RemoteAddr", want: "192.0.2.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = "192.0.2.1:1234"
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			got, err := KeyByForwardedIP(req)
			if err != nil || got != tt.want {
				t.Errorf("KeyByForwardedIP() = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestKeyByHeader(t *testing.T) {
	fn := KeyByHeader("User-Agent")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("User-Agent", "hey/0.0.1")
	if got, err := fn(req); err != nil || got != "hey/0.0.1" {
		t.Errorf("KeyByHeader() = %q, %v", got, err)
	}

	req.Header.Del("User-Agent")
	if _, err := fn(req); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("missing header error = %v, want ErrEmptyKey", err)
	}
}

func TestConcurrentRequests(t *testing.T) {
	l, err := ratelimiter.New(ratelimiter.WithLimit(50), ratelimiter.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	srv := httptest.NewServer(RateLimit(l)(okHandler()))
	defer srv.Close()

	const total = 200
	var ok, tooMany atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(srv.URL)
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)
			switch resp.StatusCode {
			case http.StatusOK:
				ok.Add(1)
			case http.StatusTooManyRequests:
				tooMany.Add(1)
			default:
				t.Errorf("unexpected status %d", resp.StatusCode)
			}
		}()
	}
	wg.Wait()

	if ok.Load() != 50 || tooMany.Load() != total-50 {
		t.Errorf("200=%d 429=%d, want 50 and %d", ok.Load(), tooMany.Load(), total-50)
	}
}
