package storage

import (
	"context"
	"hash/maphash"
	"sync"
	"time"
)

const (
	// DefaultShards is the default number of shards used by Memory.
	DefaultShards = 256
	// DefaultCleanupInterval is the default interval between removals of
	// expired records from Memory.
	DefaultCleanupInterval = time.Minute
)

// Memory is an in-memory Storage, safe for concurrent use.
//
// Keys are distributed across independent shards, each protected by its own
// mutex, which keeps lock contention low under a high volume of concurrent
// requests while guaranteeing that every hit is applied atomically.
// Expired records are periodically removed by a background goroutine that is
// stopped by Close.
type Memory struct {
	seed   maphash.Seed
	shards []memoryShard
	mask   uint64

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

type memoryShard struct {
	mu      sync.Mutex
	records map[string]Record
}

type memoryConfig struct {
	shards          int
	cleanupInterval time.Duration
}

// MemoryOption configures a Memory storage.
type MemoryOption func(*memoryConfig)

// WithShards sets the number of shards. The value is rounded up to the next
// power of two. Non-positive values keep the default (DefaultShards).
func WithShards(n int) MemoryOption {
	return func(c *memoryConfig) {
		if n > 0 {
			c.shards = n
		}
	}
}

// WithCleanupInterval sets how often expired records are removed.
// A non-positive value disables the background cleanup.
func WithCleanupInterval(d time.Duration) MemoryOption {
	return func(c *memoryConfig) {
		c.cleanupInterval = d
	}
}

// NewMemory creates an in-memory storage. Call Close to release the
// background cleanup goroutine when the storage is no longer needed.
func NewMemory(opts ...MemoryOption) *Memory {
	cfg := memoryConfig{
		shards:          DefaultShards,
		cleanupInterval: DefaultCleanupInterval,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	n := nextPowerOfTwo(cfg.shards)
	m := &Memory{
		seed:   maphash.MakeSeed(),
		shards: make([]memoryShard, n),
		mask:   uint64(n - 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	for i := range m.shards {
		m.shards[i].records = make(map[string]Record)
	}

	if cfg.cleanupInterval > 0 {
		go m.cleanupLoop(cfg.cleanupInterval)
	} else {
		close(m.done)
	}
	return m
}

// Hit implements Storage.
func (m *Memory) Hit(ctx context.Context, key string, policy Policy, now time.Time) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}

	s := m.shard(key)
	s.mu.Lock()
	rec := s.records[key].Advance(policy, now)
	s.records[key] = rec
	s.mu.Unlock()

	return rec, nil
}

// Len returns the number of records currently stored.
func (m *Memory) Len() int {
	total := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		total += len(s.records)
		s.mu.Unlock()
	}
	return total
}

// Close stops the background cleanup. It is safe to call it multiple times.
func (m *Memory) Close() error {
	m.closeOnce.Do(func() {
		close(m.stop)
		<-m.done
	})
	return nil
}

func (m *Memory) shard(key string) *memoryShard {
	return &m.shards[maphash.String(m.seed, key)&m.mask]
}

func (m *Memory) cleanupLoop(interval time.Duration) {
	defer close(m.done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stop:
			return
		case now := <-ticker.C:
			m.removeExpired(now)
		}
	}
}

func (m *Memory) removeExpired(now time.Time) {
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		for key, rec := range s.records {
			if !now.Before(rec.ExpiresAt()) {
				delete(s.records, key)
			}
		}
		s.mu.Unlock()
	}
}

func nextPowerOfTwo(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
