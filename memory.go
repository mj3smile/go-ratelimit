package ratelimit

import (
	"context"
	"sync"
	"time"
)

type entry struct {
	count     int64
	expiresAt time.Time
}

// MemoryStore is an in-memory Store implementation.
// It is safe for concurrent use and suitable for single-instance deployments
// or testing.
type MemoryStore struct {
	mu      sync.Mutex
	entries map[string]*entry
	now     func() time.Time // for testing
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries: make(map[string]*entry),
		now:     time.Now,
	}
}

// CheckAndIncrement atomically increments the counter for a key by n within
// the given window, but only if the resulting count would not exceed
// window.Rate. If the window has expired, the counter resets before the check.
func (s *MemoryStore) CheckAndIncrement(ctx context.Context, key string, window Window, n int64) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	e, ok := s.entries[key]
	if !ok || now.After(e.expiresAt) {
		if n > window.Rate {
			return 0, false, nil
		}
		s.entries[key] = &entry{
			count:     n,
			expiresAt: now.Add(window.Period),
		}
		return n, true, nil
	}

	if e.count+n > window.Rate {
		return e.count, false, nil
	}
	e.count += n
	return e.count, true, nil
}

// Reset removes the rate limit state for the given key.
func (s *MemoryStore) Reset(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, key)
	return nil
}