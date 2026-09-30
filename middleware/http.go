// Package middleware adapts the rate limiter to HTTP servers.
//
// RateLimit returns a standard func(http.Handler) http.Handler middleware, so
// it plugs directly into net/http, chi, gorilla/mux and any framework able to
// wrap net/http middlewares (e.g. echo.WrapMiddleware, gin adapters).
package middleware

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/renatocardosoalves/ratelimiter/ratelimiter"
)

// Limiter is the contract required by the middleware. *ratelimiter.Limiter
// implements it.
type Limiter interface {
	Allow(ctx context.Context, key string) (ratelimiter.Result, error)
}

// ErrorResponse is the default JSON body sent with 429 responses.
type ErrorResponse struct {
	Error        string `json:"error"`
	Limit        int    `json:"limit"`
	RequestsMade int    `json:"requests_made"`
	RetryAfter   string `json:"retry_after"`
}

// ISO8601 is the layout used for the retry_after field (UTC, milliseconds).
const ISO8601 = "2006-01-02T15:04:05.000Z07:00"

// RateLimit returns a middleware that rejects requests from clients that
// exceeded the limit with 429 Too Many Requests.
func RateLimit(limiter Limiter, opts ...Option) func(http.Handler) http.Handler {
	cfg := config{
		keyFunc:      KeyByIP,
		jsonResponse: true,
		errorMessage: DefaultErrorMessage,
		onError:      defaultErrorHandler,
		headers:      true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.denied == nil {
		cfg.denied = cfg.defaultDeniedHandler
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, err := cfg.keyFunc(r)
			if err != nil {
				cfg.onError(w, r, err)
				return
			}

			res, err := limiter.Allow(r.Context(), key)
			if err != nil {
				cfg.onError(w, r, err)
				return
			}

			if cfg.headers {
				setRateLimitHeaders(w.Header(), res)
			}

			if res.Allowed {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(res.RetryAfter)))
			cfg.denied(w, r, res)
		})
	}
}

func (c *config) defaultDeniedHandler(w http.ResponseWriter, _ *http.Request, res ratelimiter.Result) {
	if !c.jsonResponse {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	body, err := c.encodeBody(res)
	if err != nil {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(body)
}

func (c *config) encodeBody(res ratelimiter.Result) ([]byte, error) {
	if c.body != nil {
		return c.body(res)
	}
	return json.Marshal(ErrorResponse{
		Error:        c.errorMessage,
		Limit:        res.Limit,
		RequestsMade: res.RequestsMade,
		RetryAfter:   res.RetryAt.UTC().Format(ISO8601),
	})
}

func defaultErrorHandler(w http.ResponseWriter, _ *http.Request, _ error) {
	http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}

func setRateLimitHeaders(h http.Header, res ratelimiter.Result) {
	reset := res.ResetAt
	if !res.Allowed {
		reset = res.RetryAt
	}
	h.Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
	h.Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
	h.Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
}

// retryAfterSeconds rounds d up to whole seconds, never returning less
// than 1 so clients do not retry immediately.
func retryAfterSeconds(d time.Duration) int {
	return max(int(math.Ceil(d.Seconds())), 1)
}
