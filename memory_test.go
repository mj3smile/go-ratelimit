package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStore_CheckAndIncrement(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	window := Window{Rate: 10, Period: time.Minute}

	count, applied, err := store.CheckAndIncrement(ctx, "key", window, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Error("expected applied=true")
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}

	count, applied, err = store.CheckAndIncrement(ctx, "key", window, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Error("expected applied=true")
	}
	if count != 6 {
		t.Errorf("count = %d, want 6", count)
	}
}

func TestMemoryStore_CheckAndIncrement_Denied(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	window := Window{Rate: 5, Period: time.Minute}

	// n exceeds rate on fresh window
	count, applied, _ := store.CheckAndIncrement(ctx, "key", window, 6)
	if applied {
		t.Error("expected applied=false for n > rate")
	}
	if count != 0 {
		t.Errorf("count = %d, want 0 (no increment applied)", count)
	}

	// Fill quota partially
	store.CheckAndIncrement(ctx, "key", window, 3)

	// Next increment would overflow
	count, applied, _ = store.CheckAndIncrement(ctx, "key", window, 5)
	if applied {
		t.Error("expected applied=false, would exceed limit")
	}
	if count != 3 {
		t.Errorf("count = %d, want 3 (unchanged)", count)
	}

	// Smaller increment fits
	count, applied, _ = store.CheckAndIncrement(ctx, "key", window, 2)
	if !applied {
		t.Error("expected applied=true")
	}
	if count != 5 {
		t.Errorf("count = %d, want 5", count)
	}
}

func TestMemoryStore_Reset(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	window := Window{Rate: 10, Period: time.Minute}

	store.CheckAndIncrement(ctx, "key", window, 1)
	if err := store.Reset(ctx, "key"); err != nil {
		t.Fatalf("unexpected error on reset: %v", err)
	}

	count, _, _ := store.CheckAndIncrement(ctx, "key", window, 1)
	if count != 1 {
		t.Errorf("count after reset = %d, want 1", count)
	}
}

func TestMemoryStore_WindowExpiry(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	window := Window{Rate: 10, Period: time.Second}

	store.CheckAndIncrement(ctx, "key", window, 5)

	now = now.Add(2 * time.Second)

	count, applied, _ := store.CheckAndIncrement(ctx, "key", window, 1)
	if !applied {
		t.Error("expected applied=true after window expiry")
	}
	if count != 1 {
		t.Errorf("count after expiry = %d, want 1", count)
	}
}

func TestMemoryStore_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	const goroutines = 50
	const perGoroutine = 20
	total := int64(goroutines * perGoroutine)
	window := Window{Rate: total, Period: time.Minute}

	done := make(chan struct{})
	applied := make(chan bool, goroutines*perGoroutine)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < perGoroutine; j++ {
				_, ok, _ := store.CheckAndIncrement(ctx, "key", window, 1)
				applied <- ok
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	close(applied)

	var count int64
	for ok := range applied {
		if ok {
			count++
		}
	}
	if count != total {
		t.Errorf("applied count = %d, want %d (race in CheckAndIncrement)", count, total)
	}

	// Quota is fully consumed — next increment must be denied.
	_, ok, _ := store.CheckAndIncrement(ctx, "key", window, 1)
	if ok {
		t.Error("expected denial after limit reached (race may have over-incremented)")
	}
}

func TestMemoryStore_CheckAndIncrementSliding(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	window := Window{Rate: 10, Period: time.Minute}

	curr, prev, elapsed, applied, err := store.CheckAndIncrementSliding(ctx, "k", window, 3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !applied {
		t.Error("expected applied")
	}
	if curr != 3 || prev != 0 {
		t.Errorf("curr=%d prev=%d, want 3,0", curr, prev)
	}
	if elapsed != 0 {
		t.Errorf("elapsed=%v, want 0 on fresh entry", elapsed)
	}

	// Within same bucket: 3+8=11 > 10 → denied, state unchanged.
	curr, prev, _, applied, _ = store.CheckAndIncrementSliding(ctx, "k", window, 8)
	if applied {
		t.Error("3+8=11 > 10, expected denied")
	}
	if curr != 3 || prev != 0 {
		t.Errorf("curr=%d prev=%d, want 3,0 (unchanged after deny)", curr, prev)
	}

	// 3+5=8 ≤ 10 → allowed.
	curr, _, _, applied, _ = store.CheckAndIncrementSliding(ctx, "k", window, 5)
	if !applied {
		t.Error("3+5=8 ≤ 10, expected allowed")
	}
	if curr != 8 {
		t.Errorf("curr=%d, want 8", curr)
	}
}

func TestMemoryStore_CheckAndIncrementSliding_Rotation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	window := Window{Rate: 10, Period: 10 * time.Second}

	store.CheckAndIncrementSliding(ctx, "k", window, 10)

	// t=12s → periods=1, rotation. prev=10, curr=0, elapsed=2s.
	// weight=0.8, estimate=8. +1=9 ≤ 10 → allowed.
	now = now.Add(12 * time.Second)
	curr, prev, elapsed, applied, _ := store.CheckAndIncrementSliding(ctx, "k", window, 1)
	if !applied {
		t.Error("after rotation: estimate=8, +1=9 should fit")
	}
	if prev != 10 {
		t.Errorf("prev=%d, want 10 (rotated old curr)", prev)
	}
	if curr != 1 {
		t.Errorf("curr=%d, want 1", curr)
	}
	if elapsed != 2*time.Second {
		t.Errorf("elapsed=%v, want 2s", elapsed)
	}
}

func TestMemoryStore_CheckAndIncrementSliding_FullElapse(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	window := Window{Rate: 10, Period: 10 * time.Second}

	store.CheckAndIncrementSliding(ctx, "k", window, 10)

	// t=25s → periods=2.5 → ≥2, both buckets stale, fresh start.
	now = now.Add(25 * time.Second)
	curr, prev, elapsed, applied, _ := store.CheckAndIncrementSliding(ctx, "k", window, 10)
	if !applied {
		t.Error("after 2+ periods, full burst should fit")
	}
	if prev != 0 {
		t.Errorf("prev=%d, want 0 (cleared)", prev)
	}
	if curr != 10 {
		t.Errorf("curr=%d, want 10", curr)
	}
	if elapsed != 0 {
		t.Errorf("elapsed=%v, want 0 (fresh start)", elapsed)
	}
}

func TestMemoryStore_CheckAndIncrementSliding_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	const goroutines = 50
	const perGoroutine = 20
	total := int64(goroutines * perGoroutine)
	window := Window{Rate: total, Period: time.Minute}

	done := make(chan struct{})
	applied := make(chan bool, goroutines*perGoroutine)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < perGoroutine; j++ {
				_, _, _, ok, _ := store.CheckAndIncrementSliding(ctx, "k", window, 1)
				applied <- ok
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	close(applied)

	var count int64
	for ok := range applied {
		if ok {
			count++
		}
	}
	if count != total {
		t.Errorf("applied count = %d, want %d (race in CheckAndIncrementSliding)", count, total)
	}

	_, _, _, ok, _ := store.CheckAndIncrementSliding(ctx, "k", window, 1)
	if ok {
		t.Error("expected denial after limit reached")
	}
}

func TestMemoryStore_CheckAndConsumeTokens(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	window := Window{Rate: 10, Period: time.Minute}

	// Fresh key: bucket starts at capacity.
	tokens, _, applied, err := store.CheckAndConsumeTokens(ctx, "k", 10, window, 1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !applied {
		t.Error("expected applied")
	}
	if tokens != 9 {
		t.Errorf("tokens = %d, want 9", tokens)
	}

	// Drain remainder.
	tokens, _, applied, _ = store.CheckAndConsumeTokens(ctx, "k", 10, window, 9)
	if !applied {
		t.Error("expected applied")
	}
	if tokens != 0 {
		t.Errorf("tokens = %d, want 0", tokens)
	}

	// Drained: denied, state unchanged.
	tokens, _, applied, _ = store.CheckAndConsumeTokens(ctx, "k", 10, window, 1)
	if applied {
		t.Error("drained bucket should deny")
	}
	if tokens != 0 {
		t.Errorf("tokens = %d, want 0 (unchanged after deny)", tokens)
	}

	// n > capacity on a fresh key takes the dedicated early-return path:
	// returns tokens=capacity (full bucket reported, no entry created).
	tokens, _, applied, _ = store.CheckAndConsumeTokens(ctx, "fresh-k", 10, window, 11)
	if applied {
		t.Error("n > capacity on fresh key should deny")
	}
	if tokens != 10 {
		t.Errorf("tokens = %d, want 10 (fresh-key path returns capacity)", tokens)
	}
}

func TestMemoryStore_CheckAndConsumeTokens_LazyAdvance(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	window := Window{Rate: 1, Period: time.Second} // emission interval = 1s

	// Drain (capacity 1). lastRefill = t=0.
	store.CheckAndConsumeTokens(ctx, "k", 1, window, 1)

	// t=500ms: half an interval. No refill credited; lastRefill must NOT
	// advance (would lose the fractional time toward the next token).
	now = now.Add(500 * time.Millisecond)
	tokens, sinceLastRefill, applied, _ := store.CheckAndConsumeTokens(ctx, "k", 1, window, 1)
	if applied {
		t.Error("after 500ms (< 1s interval), should deny")
	}
	if tokens != 0 {
		t.Errorf("tokens = %d, want 0", tokens)
	}
	if sinceLastRefill != 500*time.Millisecond {
		t.Errorf("sinceLastRefill = %v, want 500ms (lastRefill stayed at t=0)", sinceLastRefill)
	}

	// t=1s: cumulative 1s elapsed since lastRefill — credit 1 token.
	now = now.Add(500 * time.Millisecond)
	tokens, sinceLastRefill, applied, _ = store.CheckAndConsumeTokens(ctx, "k", 1, window, 1)
	if !applied {
		t.Error("after cumulative 1s, lazy advance should credit 1 token")
	}
	if tokens != 0 {
		t.Errorf("tokens = %d, want 0 (refilled then consumed)", tokens)
	}
	// lastRefill advanced from t=0 to t=1s; now is t=1s → 0 since.
	if sinceLastRefill != 0 {
		t.Errorf("sinceLastRefill = %v, want 0 (lastRefill caught up to now)", sinceLastRefill)
	}
}

func TestMemoryStore_CheckAndConsumeTokens_RefillCappedAtCapacity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	window := Window{Rate: 5, Period: time.Second} // 5/s, interval = 200ms

	// Drain capacity=5.
	store.CheckAndConsumeTokens(ctx, "k", 5, window, 5)

	// Wait 10s — would refill 50 tokens uncapped. Should cap at 5.
	now = now.Add(10 * time.Second)
	tokens, _, applied, _ := store.CheckAndConsumeTokens(ctx, "k", 5, window, 1)
	if !applied {
		t.Error("after 10s, bucket should have tokens to spare")
	}
	if tokens != 4 {
		t.Errorf("tokens = %d, want 4 (capped at 5, then consumed 1)", tokens)
	}
}

func TestMemoryStore_CheckAndConsumeTokens_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	const goroutines = 50
	const perGoroutine = 20
	total := int64(goroutines * perGoroutine)
	// Period=1h: emission interval = 3.6s. Test runs in ms — no refill.
	window := Window{Rate: total, Period: time.Hour}

	done := make(chan struct{})
	applied := make(chan bool, goroutines*perGoroutine)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < perGoroutine; j++ {
				_, _, ok, _ := store.CheckAndConsumeTokens(ctx, "k", total, window, 1)
				applied <- ok
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	close(applied)

	var count int64
	for ok := range applied {
		if ok {
			count++
		}
	}
	if count != total {
		t.Errorf("applied count = %d, want %d (race in CheckAndConsumeTokens)", count, total)
	}

	_, _, ok, _ := store.CheckAndConsumeTokens(ctx, "k", total, window, 1)
	if ok {
		t.Error("expected denial after capacity reached")
	}
}

func TestMemoryStore_CheckAndAdvanceTAT(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	// rate=10/min → emission=6s. burst=10 → tolerance=60s.
	window := Window{Rate: 10, Period: time.Minute}

	// Fresh key: delay = 3*6s = 18s ≤ 60s → allowed.
	delay, applied, err := store.CheckAndAdvanceTAT(ctx, "k", 10, window, 3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !applied {
		t.Error("expected applied")
	}
	if delay != 18*time.Second {
		t.Errorf("delay = %v, want 18s", delay)
	}

	// Same key: TAT is 18s ahead. newTAT = TAT + 8*6s = TAT+48s.
	// delay = 18s + 48s = 66s > 60s → denied.
	delay, applied, _ = store.CheckAndAdvanceTAT(ctx, "k", 10, window, 8)
	if applied {
		t.Error("3+8=11 > burst=10, expected denied")
	}
	if delay != 66*time.Second {
		t.Errorf("delay = %v, want 66s", delay)
	}

	// Smaller request: TAT still 18s ahead. newTAT = TAT + 5*6s = TAT+30s.
	// delay = 18s + 30s = 48s ≤ 60s → allowed.
	delay, applied, _ = store.CheckAndAdvanceTAT(ctx, "k", 10, window, 5)
	if !applied {
		t.Error("3+5=8 ≤ burst=10, expected allowed")
	}
	if delay != 48*time.Second {
		t.Errorf("delay = %v, want 48s", delay)
	}
}

func TestMemoryStore_CheckAndAdvanceTAT_IdleRecovery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	// rate=1/10s → emission=10s. burst=3 → tolerance=30s.
	window := Window{Rate: 1, Period: 10 * time.Second}

	// Drain: 3 requests → TAT = now + 30s.
	store.CheckAndAdvanceTAT(ctx, "k", 3, window, 3)

	// t=15s: TAT = original_now+30s, now = original_now+15s.
	// TAT > now, so base = TAT. newTAT = TAT + 10s = original_now+40s.
	// delay = 40s - 15s = 25s ≤ 30s → allowed.
	now = now.Add(15 * time.Second)
	delay, applied, _ := store.CheckAndAdvanceTAT(ctx, "k", 3, window, 1)
	if !applied {
		t.Error("after 15s idle, 1 request should fit")
	}
	if delay != 25*time.Second {
		t.Errorf("delay = %v, want 25s", delay)
	}

	// t=60s (well past original TAT): now > tat, so base = now.
	// delay = 10s ≤ 30s → allowed, and remaining ≈ burst-1.
	now = now.Add(45 * time.Second)
	delay, applied, _ = store.CheckAndAdvanceTAT(ctx, "k", 3, window, 1)
	if !applied {
		t.Error("after full idle, should be allowed")
	}
	if delay != 10*time.Second {
		t.Errorf("delay = %v, want 10s (fresh start, 1*emission)", delay)
	}
}

func TestMemoryStore_CheckAndAdvanceTAT_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	const goroutines = 50
	const perGoroutine = 20
	total := int64(goroutines * perGoroutine)
	window := Window{Rate: total, Period: time.Minute}

	done := make(chan struct{})
	applied := make(chan bool, goroutines*perGoroutine)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < perGoroutine; j++ {
				_, ok, _ := store.CheckAndAdvanceTAT(ctx, "k", total, window, 1)
				applied <- ok
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	close(applied)

	var count int64
	for ok := range applied {
		if ok {
			count++
		}
	}
	if count != total {
		t.Errorf("applied count = %d, want %d (race in CheckAndAdvanceTAT)", count, total)
	}

	_, ok, _ := store.CheckAndAdvanceTAT(ctx, "k", total, window, 1)
	if ok {
		t.Error("expected denial after burst exhausted")
	}
}

func TestMemoryStore_Expiry(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(WithSweepInterval(0))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	window := Window{Rate: 10, Period: time.Minute}

	store.CheckAndIncrement(ctx, "fw", window, 1)
	store.CheckAndIncrementSliding(ctx, "sw", window, 1)
	store.CheckAndConsumeTokens(ctx, "tb", 10, window, 1)
	store.CheckAndAdvanceTAT(ctx, "gcra", 10, window, 1)

	if store.eh.Len() != 4 {
		t.Fatalf("heap len = %d, want 4", store.eh.Len())
	}

	// Advance past the longest TTL (sliding window's 2 * period).
	now = now.Add(3 * time.Minute)
	store.deleteExpired()

	if store.eh.Len() != 0 {
		t.Errorf("heap len after expiry = %d, want 0", store.eh.Len())
	}
	if len(store.entries) != 0 {
		t.Errorf("entries map len = %d, want 0", len(store.entries))
	}
	if len(store.sliding) != 0 {
		t.Errorf("sliding map len = %d, want 0", len(store.sliding))
	}
	if len(store.tokens) != 0 {
		t.Errorf("tokens map len = %d, want 0", len(store.tokens))
	}
	if len(store.gcra) != 0 {
		t.Errorf("gcra map len = %d, want 0", len(store.gcra))
	}
}

func TestMemoryStore_Expiry_TouchRefreshes(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(WithSweepInterval(0))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	window := Window{Rate: 2, Period: time.Minute} // emission interval = 30s

	store.CheckAndIncrement(ctx, "fw", window, 1)
	store.CheckAndIncrementSliding(ctx, "sw", window, 1)
	store.CheckAndConsumeTokens(ctx, "tb", 2, window, 2)
	store.CheckAndAdvanceTAT(ctx, "gcra", 2, window, 2)

	// Re-access at t=45s refreshes the rolling algorithms' TTL but not fixed
	// window's (its boundary at t=1m is fixed).
	now = now.Add(45 * time.Second)
	store.CheckAndIncrement(ctx, "fw", window, 1)
	store.CheckAndIncrementSliding(ctx, "sw", window, 1)
	store.CheckAndConsumeTokens(ctx, "tb", 2, window, 1)
	store.CheckAndAdvanceTAT(ctx, "gcra", 2, window, 1)

	// At t=1m15s only fixed window has expired.
	now = now.Add(30 * time.Second)
	store.deleteExpired()

	if len(store.entries) != 0 {
		t.Errorf("entries map len = %d, want 0 (fixed window TTL is not refreshed)", len(store.entries))
	}
	if len(store.sliding) != 1 {
		t.Errorf("sliding map len = %d, want 1 (refreshed on access)", len(store.sliding))
	}
	if len(store.tokens) != 1 {
		t.Errorf("tokens map len = %d, want 1 (refreshed on access)", len(store.tokens))
	}
	if len(store.gcra) != 1 {
		t.Errorf("gcra map len = %d, want 1 (refreshed on access)", len(store.gcra))
	}

	now = now.Add(3 * time.Minute)
	store.deleteExpired()

	if len(store.sliding) != 0 {
		t.Errorf("sliding map len = %d, want 0 (expired)", len(store.sliding))
	}
	if len(store.tokens) != 0 {
		t.Errorf("tokens map len = %d, want 0 (expired)", len(store.tokens))
	}
	if len(store.gcra) != 0 {
		t.Errorf("gcra map len = %d, want 0 (expired)", len(store.gcra))
	}
}

func TestMemoryStore_Expiry_ResetCleansHeap(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(WithSweepInterval(0))
	window := Window{Rate: 10, Period: time.Minute}

	store.CheckAndIncrement(ctx, "k", window, 1)
	if store.eh.Len() != 1 {
		t.Fatalf("heap len = %d, want 1", store.eh.Len())
	}

	store.Reset(ctx, "k")
	if store.eh.Len() != 0 {
		t.Errorf("heap len after reset = %d, want 0", store.eh.Len())
	}
	if len(store.lookup) != 0 {
		t.Errorf("lookup len after reset = %d, want 0", len(store.lookup))
	}
}

func TestMemoryStore_Expiry_OnlyExpiredRemoved(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(WithSweepInterval(0))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	shortWindow := Window{Rate: 10, Period: 30 * time.Second}
	longWindow := Window{Rate: 10, Period: 5 * time.Minute}

	store.CheckAndIncrement(ctx, "short", shortWindow, 1)
	store.CheckAndIncrement(ctx, "long", longWindow, 1)

	// Advance 2m — past short's TTL (60s) but before long's TTL (10m).
	now = now.Add(2 * time.Minute)
	store.deleteExpired()

	if len(store.entries) != 1 {
		t.Errorf("entries map len = %d, want 1", len(store.entries))
	}
	if _, ok := store.entries["long"]; !ok {
		t.Error("long-window key should survive")
	}
	if _, ok := store.entries["short"]; ok {
		t.Error("short-window key should be expired")
	}
}
