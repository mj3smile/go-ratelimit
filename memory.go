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

type slidingEntry struct {
	curr      int64
	prev      int64
	currStart time.Time
}

// MemoryStore is an in-memory Store implementation.
// It is safe for concurrent use and suitable for single-instance deployments
// or testing.
type MemoryStore struct {
	mu      sync.Mutex
	entries map[string]*entry
	sliding map[string]*slidingEntry
	now     func() time.Time // for testing
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries: make(map[string]*entry),
		sliding: make(map[string]*slidingEntry),
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

// CheckAndIncrementSliding atomically applies a 2-bucket sliding window check
// for the given key. See Store.CheckAndIncrementSliding for semantics.
func (s *MemoryStore) CheckAndIncrementSliding(ctx context.Context, key string, window Window, n int64) (int64, int64, time.Duration, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	e, ok := s.sliding[key]
	if !ok {
		if n > window.Rate {
			return 0, 0, 0, false, nil
		}
		s.sliding[key] = &slidingEntry{curr: n, currStart: now}
		return n, 0, 0, true, nil
	}

	// Rotate buckets based on how many periods have elapsed since currStart.
	periods := int64(now.Sub(e.currStart) / window.Period)
	switch {
	case periods >= 2:
		// Both buckets stale; fresh start.
		e.prev = 0
		e.curr = 0
		e.currStart = now
	case periods == 1:
		e.prev = e.curr
		e.curr = 0
		e.currStart = e.currStart.Add(window.Period)
	}
	elapsed := now.Sub(e.currStart)

	weight := float64(window.Period-elapsed) / float64(window.Period)
	if weight < 0 {
		weight = 0
	}
	estimate := int64(float64(e.prev)*weight) + e.curr
	if estimate+n > window.Rate {
		return e.curr, e.prev, elapsed, false, nil
	}
	e.curr += n
	return e.curr, e.prev, elapsed, true, nil
}

// Reset removes the rate limit state for the given key.
func (s *MemoryStore) Reset(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, key)
	delete(s.sliding, key)
	return nil
}
