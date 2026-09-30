// Package ratelimiter implements a rate limiter that counts the requests of
// each client (identified by a key, e.g. its IP address) within a time window
// and blocks the client for a configurable period once the limit is exceeded.
//
// The limiter is independent of any transport: HTTP integration is provided
// by the middleware package and persistence by the storage package.
package ratelimiter

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/renatocardosoalves/ratelimiter/storage"
)

// Limiter decides whether the requests of a client are allowed.
// It is safe for concurrent use.
type Limiter struct {
	policy    storage.Policy
	store     storage.Storage
	ownsStore bool
	logger    *slog.Logger
	now       func() time.Time
}

// Result describes the outcome of a request evaluated by the Limiter.
type Result struct {
	// Key identifies the client.
	Key string
	// Allowed reports whether the request may proceed.
	Allowed bool
	// Limit is the maximum number of requests allowed within the window.
	Limit int
	// RequestsMade is the number of requests made by the client in the
	// current window, including rejected ones.
	RequestsMade int
	// Remaining is how many requests the client can still make in the
	// current window.
	Remaining int
	// ResetAt is when the current window ends.
	ResetAt time.Time
	// RetryAt is when a blocked client may try again. Zero when allowed.
	RetryAt time.Time
	// RetryAfter is the time left until RetryAt. Zero when allowed.
	RetryAfter time.Duration
}

// New creates a Limiter. Options that are not provided fall back to the
// defaults: 100 requests per minute, 1 minute of block and in-memory storage.
func New(opts ...Option) (*Limiter, error) {
	l := &Limiter{
		policy: storage.Policy{
			Limit:         DefaultLimit,
			Window:        DefaultWindow,
			BlockDuration: DefaultBlockDuration,
		},
		logger: slog.Default(),
		now:    time.Now,
	}

	for _, opt := range opts {
		if err := opt(l); err != nil {
			return nil, err
		}
	}

	if l.store == nil {
		l.store = storage.NewMemory()
		l.ownsStore = true
	}
	return l, nil
}

// Allow registers a request for key and reports whether it may proceed.
// The request is counted atomically by the underlying storage.
func (l *Limiter) Allow(ctx context.Context, key string) (Result, error) {
	now := l.now()

	rec, err := l.store.Hit(ctx, key, l.policy, now)
	if err != nil {
		return Result{}, err
	}

	res := Result{
		Key:          key,
		Allowed:      !rec.Blocked(now),
		Limit:        l.policy.Limit,
		RequestsMade: rec.Count,
		Remaining:    max(l.policy.Limit-rec.Count, 0),
		ResetAt:      rec.WindowEnd,
	}
	if !res.Allowed {
		res.RetryAt = rec.BlockedUntil
		res.RetryAfter = rec.BlockedUntil.Sub(now)
	}

	l.log(ctx, res)
	return res, nil
}

// Policy returns the rules enforced by the limiter.
func (l *Limiter) Policy() storage.Policy {
	return l.policy
}

// Close releases the resources held by the default in-memory storage.
// Storages provided through WithStorage are left untouched.
func (l *Limiter) Close() error {
	if !l.ownsStore {
		return nil
	}
	if c, ok := l.store.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (l *Limiter) log(ctx context.Context, res Result) {
	switch {
	case res.Allowed:
		if l.logger.Enabled(ctx, slog.LevelDebug) {
			l.logger.LogAttrs(ctx, slog.LevelDebug, "request allowed",
				slog.String("key", res.Key),
				slog.Int("requests_made", res.RequestsMade),
				slog.Int("limit", res.Limit),
				slog.Int("remaining", res.Remaining),
			)
		}
	case res.RequestsMade == res.Limit+1:
		l.logger.LogAttrs(ctx, slog.LevelWarn, "rate limit exceeded",
			l.blockedAttrs(res)...,
		)
	default:
		if l.logger.Enabled(ctx, slog.LevelDebug) {
			l.logger.LogAttrs(ctx, slog.LevelDebug, "request rejected: client blocked",
				l.blockedAttrs(res)...,
			)
		}
	}
}

func (l *Limiter) blockedAttrs(res Result) []slog.Attr {
	return []slog.Attr{
		slog.String("key", res.Key),
		slog.Int("requests_made", res.RequestsMade),
		slog.Int("limit", res.Limit),
		slog.Duration("window", l.policy.Window),
		slog.Duration("block_duration", l.policy.BlockDuration),
		slog.Time("retry_at", res.RetryAt),
		slog.Duration("retry_after", res.RetryAfter),
	}
}
