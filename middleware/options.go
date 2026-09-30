package middleware

import (
	"encoding/json"
	"net/http"

	"github.com/renatocardosoalves/ratelimiter/ratelimiter"
)

// DefaultErrorMessage is the "error" field of the default JSON response.
const DefaultErrorMessage = "Rate limit exceeded"

// DeniedHandler writes the response sent to a client that exceeded the rate
// limit. The Retry-After (and, when enabled, X-RateLimit-*) headers are
// already set when it is called; it must write the status code and body.
type DeniedHandler func(w http.ResponseWriter, r *http.Request, res ratelimiter.Result)

// ErrorHandler writes the response when the client key cannot be extracted
// or the limiter fails (e.g. the storage is unavailable).
type ErrorHandler func(w http.ResponseWriter, r *http.Request, err error)

type config struct {
	keyFunc      KeyFunc
	jsonResponse bool
	errorMessage string
	body         func(ratelimiter.Result) ([]byte, error)
	denied       DeniedHandler
	onError      ErrorHandler
	headers      bool
}

// Option configures the HTTP middleware.
type Option func(*config)

// WithKeyFunc sets how the client is identified. Default: KeyByIP.
func WithKeyFunc(fn KeyFunc) Option {
	return func(c *config) {
		if fn != nil {
			c.keyFunc = fn
		}
	}
}

// WithJSONResponse enables or disables the JSON body of the 429 response.
// When disabled only the status code and headers are sent. Default: true.
func WithJSONResponse(enabled bool) Option {
	return func(c *config) {
		c.jsonResponse = enabled
	}
}

// WithErrorMessage customizes the "error" field of the default JSON body.
// Default: DefaultErrorMessage.
func WithErrorMessage(msg string) Option {
	return func(c *config) {
		c.errorMessage = msg
	}
}

// WithResponseBody replaces the default JSON body with the value returned by
// fn, encoded as JSON. The body is only sent when the JSON response is
// enabled.
func WithResponseBody[T any](fn func(res ratelimiter.Result) T) Option {
	return func(c *config) {
		if fn == nil {
			return
		}
		c.body = func(res ratelimiter.Result) ([]byte, error) {
			return json.Marshal(fn(res))
		}
	}
}

// WithDeniedHandler takes full control of the response sent to blocked
// clients, overriding WithJSONResponse, WithErrorMessage and
// WithResponseBody.
func WithDeniedHandler(h DeniedHandler) Option {
	return func(c *config) {
		c.denied = h
	}
}

// WithErrorHandler sets the handler used when the client key cannot be
// extracted or the limiter fails. Default: responds 500 Internal Server Error.
func WithErrorHandler(h ErrorHandler) Option {
	return func(c *config) {
		if h != nil {
			c.onError = h
		}
	}
}

// WithRateLimitHeaders enables or disables the informative X-RateLimit-Limit,
// X-RateLimit-Remaining and X-RateLimit-Reset headers. Retry-After is always
// sent on 429 responses. Default: true.
func WithRateLimitHeaders(enabled bool) Option {
	return func(c *config) {
		c.headers = enabled
	}
}
