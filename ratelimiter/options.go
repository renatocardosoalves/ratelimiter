package ratelimiter

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/renatocardosoalves/ratelimiter/storage"
)

// Default values used when the corresponding option is not provided.
const (
	DefaultLimit         = 100
	DefaultWindow        = time.Minute
	DefaultBlockDuration = time.Minute
)

// ErrNilStorage is returned by New when WithStorage receives a nil storage.
var ErrNilStorage = errors.New("ratelimiter: storage must not be nil")

// Option configures a Limiter.
type Option func(*Limiter) error

// WithLimit sets the maximum number of requests allowed within the window.
// Default: DefaultLimit (100).
func WithLimit(n int) Option {
	return func(l *Limiter) error {
		if n <= 0 {
			return fmt.Errorf("ratelimiter: limit must be positive, got %d", n)
		}
		l.policy.Limit = n
		return nil
	}
}

// WithWindow sets the period in which requests are counted, e.g.
// 30*time.Second, 10*time.Minute or time.Hour. Default: DefaultWindow (1m).
func WithWindow(d time.Duration) Option {
	return func(l *Limiter) error {
		if d <= 0 {
			return fmt.Errorf("ratelimiter: window must be positive, got %s", d)
		}
		l.policy.Window = d
		return nil
	}
}

// WithRate is a shortcut for WithLimit(limit) and WithWindow(window),
// e.g. WithRate(100, 10*time.Minute) allows 100 requests every 10 minutes.
func WithRate(limit int, window time.Duration) Option {
	return func(l *Limiter) error {
		if err := WithLimit(limit)(l); err != nil {
			return err
		}
		return WithWindow(window)(l)
	}
}

// WithBlockDuration sets how long a client stays blocked after exceeding the
// limit. Default: DefaultBlockDuration (1m).
func WithBlockDuration(d time.Duration) Option {
	return func(l *Limiter) error {
		if d <= 0 {
			return fmt.Errorf("ratelimiter: block duration must be positive, got %s", d)
		}
		l.policy.BlockDuration = d
		return nil
	}
}

// WithStorage sets the persistence adapter. Default: a storage.Memory owned
// by the limiter and released by Limiter.Close. A storage provided through
// this option is not closed by the limiter.
func WithStorage(s storage.Storage) Option {
	return func(l *Limiter) error {
		if s == nil {
			return ErrNilStorage
		}
		l.store = s
		return nil
	}
}

// WithLogger sets the structured logger. Default: slog.Default().
// A nil logger disables logging.
//
// When the limit is exceeded a Warn record is emitted; allowed requests and
// requests rejected while the client is already blocked are logged at Debug
// level, so they only show up when the handler is configured for it.
func WithLogger(logger *slog.Logger) Option {
	return func(l *Limiter) error {
		if logger == nil {
			logger = slog.New(discardHandler{})
		}
		l.logger = logger
		return nil
	}
}

// WithClock overrides the time source. Mostly useful for tests.
// Default: time.Now.
func WithClock(now func() time.Time) Option {
	return func(l *Limiter) error {
		if now == nil {
			return errors.New("ratelimiter: clock must not be nil")
		}
		l.now = now
		return nil
	}
}
