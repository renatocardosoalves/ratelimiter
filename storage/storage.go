// Package storage defines the persistence contract used by the rate limiter
// and provides the adapters that implement it.
//
// A Storage must apply a hit atomically: reading the current state of a key,
// updating it and returning the new state must happen as a single operation,
// so concurrent requests for the same key never produce inconsistent counts.
// In-process adapters can rely on Record.Advance to implement the algorithm;
// distributed adapters (e.g. Redis) are expected to reproduce the same rules
// server side (e.g. with a Lua script).
package storage

import (
	"context"
	"time"
)

// Policy describes the rules applied by a Storage on every hit.
type Policy struct {
	// Limit is the maximum number of requests allowed within Window.
	Limit int
	// Window is the period in which requests are counted.
	Window time.Duration
	// BlockDuration is how long a key stays blocked after exceeding Limit.
	BlockDuration time.Duration
}

// Record is the state of a key after a hit has been applied.
type Record struct {
	// Count is the number of requests made in the current window, including
	// the ones rejected while the key is blocked.
	Count int
	// WindowEnd is when the current counting window ends.
	WindowEnd time.Time
	// BlockedUntil is when the block expires. It is the zero time when the
	// key has never been blocked in the current window.
	BlockedUntil time.Time
}

// Storage persists the rate limiting state of each key.
type Storage interface {
	// Hit registers a request for key at instant now, applying policy
	// atomically, and returns the resulting record.
	Hit(ctx context.Context, key string, policy Policy, now time.Time) (Record, error)
}

// Blocked reports whether the record is blocked at instant now.
func (r Record) Blocked(now time.Time) bool {
	return now.Before(r.BlockedUntil)
}

// ExpiresAt is the instant after which the record no longer carries any
// information and can be safely discarded.
func (r Record) ExpiresAt() time.Time {
	if r.BlockedUntil.After(r.WindowEnd) {
		return r.BlockedUntil
	}
	return r.WindowEnd
}

// Advance returns the state of the record after one more request at instant
// now, following a fixed window algorithm with blocking:
//
//   - while blocked, the request is counted and the block is kept;
//   - when the window or a previous block has expired, a new window starts;
//   - when the count exceeds the limit, the key is blocked for
//     policy.BlockDuration.
func (r Record) Advance(policy Policy, now time.Time) Record {
	if r.Blocked(now) {
		r.Count++
		return r
	}

	if !r.BlockedUntil.IsZero() || !now.Before(r.WindowEnd) {
		r = Record{WindowEnd: now.Add(policy.Window)}
	}

	r.Count++
	if r.Count > policy.Limit {
		r.BlockedUntil = now.Add(policy.BlockDuration)
	}
	return r
}
