package ratelimit

import (
	"container/heap"
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

type tokenEntry struct {
	tokens     int64
	lastRefill time.Time
}

type gcraEntry struct {
	tat time.Time
}

// MemoryStore is an in-memory Store implementation.
// It is safe for concurrent use and suitable for single-instance deployments
// or testing.
//
// Expired keys are removed during normal calls, oldest first, using a
// min-heap of expiry times.
type MemoryStore struct {
	mu      sync.Mutex
	entries map[string]*entry
	sliding map[string]*slidingEntry
	tokens  map[string]*tokenEntry
	gcra    map[string]*gcraEntry
	now     func() time.Time // for testing

	eh     expiryHeap
	lookup map[heapKey]*heapItem
}

// Each call removes up to this many expired keys. A call adds at most one
// key, so removing two keeps the store from growing past its busiest point.
const removePerCall = 2

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries: make(map[string]*entry),
		sliding: make(map[string]*slidingEntry),
		tokens:  make(map[string]*tokenEntry),
		gcra:    make(map[string]*gcraEntry),
		eh:      make(expiryHeap, 0),
		lookup:  make(map[heapKey]*heapItem),
		now:     time.Now,
	}
}

// touchKey updates the expiry for a key in the min-heap. Must be called
// while s.mu is held.
func (s *MemoryStore) touchKey(key string, mt mapType, ttl time.Duration) {
	expiry := s.now().Add(ttl)
	hk := heapKey{mt: mt, key: key}
	if item, ok := s.lookup[hk]; ok {
		item.expiry = expiry
		heap.Fix(&s.eh, item.index)
		return
	}
	item := &heapItem{key: key, mt: mt, expiry: expiry}
	s.lookup[hk] = item
	heap.Push(&s.eh, item)
}

// removeExpired deletes up to limit expired keys, oldest first. Must be
// called while s.mu is held.
func (s *MemoryStore) removeExpired(now time.Time, limit int) {
	for range limit {
		if s.eh.Len() == 0 || s.eh[0].expiry.After(now) {
			return
		}
		item := heap.Pop(&s.eh).(*heapItem)
		delete(s.lookup, heapKey{mt: item.mt, key: item.key})
		s.deleteFromMap(item.mt, item.key)
	}
}

// deleteFromMap removes key from the state map identified by mt. Caller must
// hold s.mu.
func (s *MemoryStore) deleteFromMap(mt mapType, key string) {
	switch mt {
	case mapEntries:
		delete(s.entries, key)
	case mapSliding:
		delete(s.sliding, key)
	case mapTokens:
		delete(s.tokens, key)
	case mapGCRA:
		delete(s.gcra, key)
	}
}

// CheckAndIncrement atomically increments the counter for a key by n within
// the given window, but only if the resulting count would not exceed
// window.Rate. If the window has expired, the counter resets before the check.
func (s *MemoryStore) CheckAndIncrement(ctx context.Context, key string, window Window, n int64) (int64, time.Duration, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	s.removeExpired(now, removePerCall)
	e, ok := s.entries[key]
	if !ok || !now.Before(e.expiresAt) {
		if n > window.Rate {
			return 0, window.Period, false, nil
		}
		s.entries[key] = &entry{
			count:     n,
			expiresAt: now.Add(window.Period),
		}
		s.touchKey(key, mapEntries, window.Period)
		return n, window.Period, true, nil
	}

	resetAfter := e.expiresAt.Sub(now)
	if e.count+n > window.Rate {
		return e.count, resetAfter, false, nil
	}
	e.count += n
	return e.count, resetAfter, true, nil
}

// CheckAndIncrementSliding atomically applies a 2-bucket sliding window check
// for the given key. See Store.CheckAndIncrementSliding for semantics.
func (s *MemoryStore) CheckAndIncrementSliding(ctx context.Context, key string, window Window, n int64) (int64, int64, time.Duration, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ttl := 2 * window.Period
	now := s.now()
	s.removeExpired(now, removePerCall)
	e, ok := s.sliding[key]
	if !ok {
		if n > window.Rate {
			return 0, 0, 0, false, nil
		}
		s.sliding[key] = &slidingEntry{curr: n, currStart: now}
		s.touchKey(key, mapSliding, ttl)
		return n, 0, 0, true, nil
	}

	s.touchKey(key, mapSliding, ttl)

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

// CheckAndConsumeTokens atomically applies a token-bucket check for the given
// key. See Store.CheckAndConsumeTokens for semantics.
func (s *MemoryStore) CheckAndConsumeTokens(ctx context.Context, key string, capacity int64, window Window, n int64) (int64, time.Duration, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	s.removeExpired(now, removePerCall)
	emissionInterval := window.EmissionInterval()

	e, ok := s.tokens[key]
	if !ok {
		// Initial state: bucket starts full at capacity.
		if n > capacity {
			return capacity, 0, false, nil
		}
		tokens := capacity - n
		s.tokens[key] = &tokenEntry{
			tokens:     tokens,
			lastRefill: now,
		}
		s.touchKey(key, mapTokens, time.Duration(capacity-tokens)*emissionInterval)
		return tokens, 0, true, nil
	}

	// Lazy advance: credit only complete emission intervals; carry the
	// fractional remainder forward by leaving lastRefill behind. A full
	// bucket earns nothing, so filling it drops the remainder.
	elapsed := now.Sub(e.lastRefill)
	refilled := int64(elapsed / emissionInterval)
	if e.tokens+refilled >= capacity {
		e.tokens = capacity
		e.lastRefill = now
	} else {
		e.tokens += refilled
		e.lastRefill = e.lastRefill.Add(time.Duration(refilled) * emissionInterval)
	}

	applied := false
	if e.tokens >= n {
		e.tokens -= n
		applied = true
	}

	sinceLastRefill := now.Sub(e.lastRefill)
	ttl := time.Duration(capacity-e.tokens)*emissionInterval - sinceLastRefill
	if ttl < 0 {
		ttl = 0
	}
	s.touchKey(key, mapTokens, ttl)

	return e.tokens, sinceLastRefill, applied, nil
}

// CheckAndAdvanceTAT atomically applies a GCRA check for the given key.
// See Store.CheckAndAdvanceTAT for semantics.
func (s *MemoryStore) CheckAndAdvanceTAT(ctx context.Context, key string, burst int64, window Window, n int64) (time.Duration, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	s.removeExpired(now, removePerCall)
	emissionInterval := window.EmissionInterval()
	delayTolerance := time.Duration(burst) * emissionInterval

	tat := now
	e, ok := s.gcra[key]
	if ok && e.tat.After(now) {
		tat = e.tat
	}

	newTAT := tat.Add(time.Duration(n) * emissionInterval)
	delay := newTAT.Sub(now)

	if delay > delayTolerance {
		return delay, false, nil
	}

	if !ok {
		s.gcra[key] = &gcraEntry{tat: newTAT}
	} else {
		e.tat = newTAT
	}
	s.touchKey(key, mapGCRA, delay)
	return delay, true, nil
}

// ResetFixed removes the fixed-window state for the given key.
func (s *MemoryStore) ResetFixed(ctx context.Context, key string) error {
	return s.reset(key, mapEntries)
}

// ResetSliding removes the sliding-window state for the given key.
func (s *MemoryStore) ResetSliding(ctx context.Context, key string) error {
	return s.reset(key, mapSliding)
}

// ResetTokens removes the token-bucket state for the given key.
func (s *MemoryStore) ResetTokens(ctx context.Context, key string) error {
	return s.reset(key, mapTokens)
}

// ResetTAT removes the GCRA state for the given key.
func (s *MemoryStore) ResetTAT(ctx context.Context, key string) error {
	return s.reset(key, mapGCRA)
}

func (s *MemoryStore) reset(key string, mt mapType) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleteFromMap(mt, key)

	hk := heapKey{mt: mt, key: key}
	if item, ok := s.lookup[hk]; ok {
		heap.Remove(&s.eh, item.index)
		delete(s.lookup, hk)
	}
	return nil
}
