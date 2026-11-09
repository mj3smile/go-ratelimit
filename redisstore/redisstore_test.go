package redisstore

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/mj3smile/go-ratelimit"
	"github.com/redis/go-redis/v9"
)

func testClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping Redis integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("cannot connect to Redis at %s: %v", addr, err)
	}
	return client
}

func newStore(t *testing.T) (*Store, *redis.Client) {
	t.Helper()
	client := testClient(t)
	client.FlushDB(context.Background())
	return New(client), client
}

func absDiff(a, b time.Duration) time.Duration {
	if a > b {
		return a - b
	}
	return b - a
}

// --- Fixed Window ---

func TestRedisStore_CheckAndIncrement(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 10, Period: time.Minute}

	count, _, applied, err := store.CheckAndIncrement(ctx, "key", window, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Error("expected applied=true")
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}

	count, _, applied, err = store.CheckAndIncrement(ctx, "key", window, 5)
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

func TestRedisStore_CheckAndIncrement_Denied(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 5, Period: time.Minute}

	count, _, applied, _ := store.CheckAndIncrement(ctx, "key", window, 6)
	if applied {
		t.Error("expected applied=false for n > rate")
	}
	if count != 0 {
		t.Errorf("count = %d, want 0 (no increment applied)", count)
	}

	store.CheckAndIncrement(ctx, "key", window, 3)

	count, _, applied, _ = store.CheckAndIncrement(ctx, "key", window, 5)
	if applied {
		t.Error("expected applied=false, would exceed limit")
	}
	if count != 3 {
		t.Errorf("count = %d, want 3 (unchanged)", count)
	}

	count, _, applied, _ = store.CheckAndIncrement(ctx, "key", window, 2)
	if !applied {
		t.Error("expected applied=true")
	}
	if count != 5 {
		t.Errorf("count = %d, want 5", count)
	}
}

func TestRedisStore_CheckAndIncrement_ResetAfter(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 1, Period: time.Minute}

	_, resetAfter, _, err := store.CheckAndIncrement(ctx, "key", window, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resetAfter != time.Minute {
		t.Errorf("new window: resetAfter = %v, want %v", resetAfter, time.Minute)
	}

	time.Sleep(200 * time.Millisecond)

	_, resetAfter, applied, _ := store.CheckAndIncrement(ctx, "key", window, 1)
	if applied {
		t.Fatal("second increment should be denied")
	}
	if d := absDiff(resetAfter, 59800*time.Millisecond); d > 100*time.Millisecond {
		t.Errorf("denied: resetAfter = %v, want about 59.8s (the time left in the window)", resetAfter)
	}
}

// Reset is scoped to one algorithm: it clears that algorithm's key for the
// subject and leaves the other three untouched.
func TestRedisStore_ResetByAlgorithm(t *testing.T) {
	store, client := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 10, Period: time.Minute}

	seed := func() {
		store.CheckAndIncrement(ctx, "key", window, 1)
		store.CheckAndIncrementSliding(ctx, "key", window, 1)
		store.CheckAndConsumeTokens(ctx, "key", 10, window, 1)
		store.CheckAndAdvanceTAT(ctx, "key", 10, window, 1)
	}

	suffixes := []string{"fw", "sw", "tb", "gcra"}
	cases := []struct {
		name    string
		reset   func() error
		cleared string
	}{
		{"ResetFixed", func() error { return store.ResetFixed(ctx, "key") }, "fw"},
		{"ResetSliding", func() error { return store.ResetSliding(ctx, "key") }, "sw"},
		{"ResetTokens", func() error { return store.ResetTokens(ctx, "key") }, "tb"},
		{"ResetTAT", func() error { return store.ResetTAT(ctx, "key") }, "gcra"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client.FlushDB(ctx)
			seed()
			if err := tc.reset(); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			for _, suffix := range suffixes {
				exists := client.Exists(ctx, store.redisKey("key", suffix)).Val()
				if suffix == tc.cleared {
					if exists != 0 {
						t.Errorf("%s should clear the %q key", tc.name, suffix)
					}
				} else if exists != 1 {
					t.Errorf("%s must leave the %q key intact", tc.name, suffix)
				}
			}
		})
	}
}

func TestRedisStore_WindowExpiry(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 10, Period: time.Second}

	store.CheckAndIncrement(ctx, "key", window, 5)

	time.Sleep(1500 * time.Millisecond)

	count, _, applied, _ := store.CheckAndIncrement(ctx, "key", window, 1)
	if !applied {
		t.Error("expected applied=true after window expiry")
	}
	if count != 1 {
		t.Errorf("count after expiry = %d, want 1", count)
	}
}

// --- Sliding Window ---

func TestRedisStore_CheckAndIncrementSliding(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 10, Period: time.Minute}

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

	curr, prev, _, applied, _ = store.CheckAndIncrementSliding(ctx, "k", window, 8)
	if applied {
		t.Error("3+8=11 > 10, expected denied")
	}
	if curr != 3 || prev != 0 {
		t.Errorf("curr=%d prev=%d, want 3,0 (unchanged after deny)", curr, prev)
	}

	curr, _, _, applied, _ = store.CheckAndIncrementSliding(ctx, "k", window, 5)
	if !applied {
		t.Error("3+5=8 ≤ 10, expected allowed")
	}
	if curr != 8 {
		t.Errorf("curr=%d, want 8", curr)
	}
}

func TestRedisStore_CheckAndIncrementSliding_Rotation(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 10, Period: time.Second}

	store.CheckAndIncrementSliding(ctx, "k", window, 10)

	// Wait 1.2s — one period elapsed. prev=10, curr=0.
	// weight ≈ 0.8, estimate ≈ 8. +1=9 ≤ 10 → allowed.
	time.Sleep(1200 * time.Millisecond)
	curr, prev, _, applied, _ := store.CheckAndIncrementSliding(ctx, "k", window, 1)
	if !applied {
		t.Error("after rotation: estimate ~8, +1=9 should fit")
	}
	if prev != 10 {
		t.Errorf("prev=%d, want 10 (rotated old curr)", prev)
	}
	if curr != 1 {
		t.Errorf("curr=%d, want 1", curr)
	}
}

func TestRedisStore_CheckAndIncrementSliding_FullElapse(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 10, Period: time.Second}

	store.CheckAndIncrementSliding(ctx, "k", window, 10)

	// Wait 2.5s — 2+ periods. Both buckets stale, fresh start.
	time.Sleep(2500 * time.Millisecond)
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

// --- Token Bucket ---

func TestRedisStore_CheckAndConsumeTokens(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 10, Period: time.Minute}

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

	tokens, _, applied, _ = store.CheckAndConsumeTokens(ctx, "k", 10, window, 9)
	if !applied {
		t.Error("expected applied")
	}
	if tokens != 0 {
		t.Errorf("tokens = %d, want 0", tokens)
	}

	tokens, _, applied, _ = store.CheckAndConsumeTokens(ctx, "k", 10, window, 1)
	if applied {
		t.Error("drained bucket should deny")
	}
	if tokens != 0 {
		t.Errorf("tokens = %d, want 0 (unchanged after deny)", tokens)
	}

	tokens, _, applied, _ = store.CheckAndConsumeTokens(ctx, "fresh-k", 10, window, 11)
	if applied {
		t.Error("n > capacity on fresh key should deny")
	}
	if tokens != 10 {
		t.Errorf("tokens = %d, want 10 (fresh-key path returns capacity)", tokens)
	}
}

func TestRedisStore_CheckAndConsumeTokens_LazyAdvance(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 2, Period: time.Second} // emission interval = 500ms

	store.CheckAndConsumeTokens(ctx, "k", 2, window, 2) // drain

	// Immediately: no refill yet.
	_, _, applied, _ := store.CheckAndConsumeTokens(ctx, "k", 2, window, 1)
	if applied {
		t.Error("immediately after drain, should deny")
	}

	// Wait 600ms (> 500ms emission interval): 1 token refilled.
	time.Sleep(600 * time.Millisecond)
	tokens, _, applied, _ := store.CheckAndConsumeTokens(ctx, "k", 2, window, 1)
	if !applied {
		t.Error("after 600ms (> 500ms interval), should allow")
	}
	if tokens != 0 {
		t.Errorf("tokens = %d, want 0 (refilled 1, consumed 1)", tokens)
	}
}

func TestRedisStore_CheckAndConsumeTokens_RefillCappedAtCapacity(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 5, Period: time.Second} // interval = 200ms

	store.CheckAndConsumeTokens(ctx, "k", 5, window, 5) // drain

	// Wait 3s — would refill 15 tokens uncapped. Should cap at 5.
	time.Sleep(3 * time.Second)
	tokens, _, applied, _ := store.CheckAndConsumeTokens(ctx, "k", 5, window, 1)
	if !applied {
		t.Error("after 3s, bucket should have tokens to spare")
	}
	if tokens != 4 {
		t.Errorf("tokens = %d, want 4 (capped at 5, then consumed 1)", tokens)
	}
}

// --- GCRA ---

func TestRedisStore_CheckAndAdvanceTAT(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	// rate=10/min → emission=6s. burst=10 → tolerance=60s.
	window := ratelimit.Window{Rate: 10, Period: time.Minute}

	delay, applied, err := store.CheckAndAdvanceTAT(ctx, "k", 10, window, 3)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !applied {
		t.Error("expected applied")
	}
	expected := 18 * time.Second
	if absDiff(delay, expected) > 100*time.Millisecond {
		t.Errorf("delay = %v, want ~%v", delay, expected)
	}

	_, applied, _ = store.CheckAndAdvanceTAT(ctx, "k", 10, window, 8)
	if applied {
		t.Error("3+8=11 > burst=10, expected denied")
	}

	_, applied, _ = store.CheckAndAdvanceTAT(ctx, "k", 10, window, 5)
	if !applied {
		t.Error("3+5=8 ≤ burst=10, expected applied")
	}
}

func TestRedisStore_CheckAndAdvanceTAT_IdleRecovery(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	// rate=2/s → emission=500ms. burst=2 → tolerance=1s.
	window := ratelimit.Window{Rate: 2, Period: time.Second}

	store.CheckAndAdvanceTAT(ctx, "k", 2, window, 2) // drain

	_, applied, _ := store.CheckAndAdvanceTAT(ctx, "k", 2, window, 1)
	if applied {
		t.Error("immediately after drain, should be denied")
	}

	time.Sleep(600 * time.Millisecond)

	_, applied, _ = store.CheckAndAdvanceTAT(ctx, "k", 2, window, 1)
	if !applied {
		t.Error("after 600ms idle (> 500ms emission), should be allowed")
	}
}

// --- Key Expiry (dynamic TTL validation) ---

// Each algorithm sets its own TTL:
//
//	fixed window   = period                          (the key IS the window boundary)
//	sliding window = 2 * period                      (previous bucket must survive)
//	token bucket   = (capacity - tokens) * emission  (time to refill to full)
//	gcra           = new_tat - now                   (time until the TAT decays)
//
// With rate=2/period=1s the emission interval is 500ms, so draining the token
// bucket and GCRA (n=2) gives them a 1s TTL. The longest TTL is the sliding
// window's 2s, so sleeping 2.5s must expire every key.
func TestRedisStore_KeyExpiry(t *testing.T) {
	store, client := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 2, Period: time.Second}

	store.CheckAndIncrement(ctx, "fw", window, 1)
	store.CheckAndIncrementSliding(ctx, "sw", window, 1)
	store.CheckAndConsumeTokens(ctx, "tb", 2, window, 2)
	store.CheckAndAdvanceTAT(ctx, "gcra", 2, window, 2)

	suffixes := []string{"fw", "sw", "tb", "gcra"}

	for _, s := range suffixes {
		rk := store.redisKey(s, s)
		ttl := client.PTTL(ctx, rk).Val()
		if ttl <= 0 {
			t.Fatalf("%s: key should have a TTL, got %v", s, ttl)
		}
	}

	// Longest TTL is the sliding window's 2 * period = 2s.
	time.Sleep(2500 * time.Millisecond)

	for _, s := range suffixes {
		rk := store.redisKey(s, s)
		if client.Exists(ctx, rk).Val() != 0 {
			t.Errorf("%s: key should have expired", s)
		}
	}
}

// --- TTL Refresh on access ---

// Sliding window, token bucket and GCRA re-arm their TTL on every access, so a
// second access pushes their expiry further out than it was a moment earlier.
// Fixed window is deliberately different: its TTL is set once at creation and
// never refreshed, because the key's expiry IS the window boundary — refreshing
// it would let a steady stream of requests hold one window open forever.
func TestRedisStore_TTLExtension(t *testing.T) {
	store, client := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 2, Period: time.Second}

	store.CheckAndIncrement(ctx, "fw", window, 1)
	store.CheckAndIncrementSliding(ctx, "sw", window, 1)
	store.CheckAndConsumeTokens(ctx, "tb", 2, window, 2)
	store.CheckAndAdvanceTAT(ctx, "gcra", 2, window, 2)

	suffixes := []string{"fw", "sw", "tb", "gcra"}

	// Sleep less than every key's TTL (min is 1s) so all four survive to be
	// re-accessed — this tests refresh, not re-creation.
	time.Sleep(600 * time.Millisecond)

	before := make(map[string]time.Duration, len(suffixes))
	for _, s := range suffixes {
		before[s] = client.PTTL(ctx, store.redisKey(s, s)).Val()
	}

	store.CheckAndIncrement(ctx, "fw", window, 1)
	store.CheckAndIncrementSliding(ctx, "sw", window, 1)
	store.CheckAndConsumeTokens(ctx, "tb", 2, window, 1)
	store.CheckAndAdvanceTAT(ctx, "gcra", 2, window, 1)

	after := make(map[string]time.Duration, len(suffixes))
	for _, s := range suffixes {
		after[s] = client.PTTL(ctx, store.redisKey(s, s)).Val()
	}

	// The three rolling algorithms must have a later expiry than a moment ago.
	for _, s := range []string{"sw", "tb", "gcra"} {
		if after[s] <= before[s] {
			t.Errorf("%s: TTL should have been refreshed, before=%v after=%v", s, before[s], after[s])
		}
	}

	// Fixed window must NOT be refreshed: its TTL keeps counting down.
	if after["fw"] > before["fw"] {
		t.Errorf("fw: TTL must not be refreshed (window boundary is fixed), before=%v after=%v", before["fw"], after["fw"])
	}
}

// --- Fractional emission interval ---

// When the rate does not divide the period evenly the per-token emission
// interval is fractional in milliseconds (3/s → 333.33ms). The token bucket and
// GCRA TTL math is built on that interval, so the value handed to PEXPIRE must be
// rounded up to a whole millisecond — Redis rejects a fractional PEXPIRE argument.
func TestRedisStore_FractionalEmission(t *testing.T) {
	store, client := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 3, Period: time.Second} // emission ≈ 333.33ms

	// Token bucket exercises both the fresh-key and existing-key PEXPIRE paths.
	if _, _, _, err := store.CheckAndConsumeTokens(ctx, "tb", 5, window, 1); err != nil {
		t.Fatalf("token bucket fresh key: %v", err)
	}
	if _, _, _, err := store.CheckAndConsumeTokens(ctx, "tb", 5, window, 1); err != nil {
		t.Fatalf("token bucket existing key: %v", err)
	}
	if ttl := client.PTTL(ctx, store.redisKey("tb", "tb")).Val(); ttl <= 0 {
		t.Errorf("tb: expected a positive TTL, got %v", ttl)
	}

	if _, _, err := store.CheckAndAdvanceTAT(ctx, "gcra", 5, window, 1); err != nil {
		t.Fatalf("gcra fresh key: %v", err)
	}
	if _, _, err := store.CheckAndAdvanceTAT(ctx, "gcra", 5, window, 1); err != nil {
		t.Fatalf("gcra existing key: %v", err)
	}
	if ttl := client.PTTL(ctx, store.redisKey("gcra", "gcra")).Val(); ttl <= 0 {
		t.Errorf("gcra: expected a positive TTL, got %v", ttl)
	}
}

// --- Redis clock ---

// Concurrent callers on one host reach Redis in a different order than they
// read their clocks. Scripts that took the time from the caller saw it move
// backwards, so token counts and the sliding window's elapsed time went negative.
func TestRedisStore_ConcurrentCallersNeverGoBackInTime(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	tokenWindow := ratelimit.Window{Rate: 1000, Period: time.Second}
	slidingWindow := ratelimit.Window{Rate: 50, Period: 20 * time.Millisecond}

	var errs, negativeTokens, negativeElapsed atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(500 * time.Millisecond)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				tokens, since, _, err := store.CheckAndConsumeTokens(ctx, "tb", 5, tokenWindow, 1)
				if err != nil {
					errs.Add(1)
				} else if tokens < 0 || since < 0 {
					negativeTokens.Add(1)
				}
				_, _, elapsed, _, err := store.CheckAndIncrementSliding(ctx, "sw", slidingWindow, 1)
				if err != nil {
					errs.Add(1)
				} else if elapsed < 0 {
					negativeElapsed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if n := errs.Load(); n > 0 {
		t.Fatalf("%d store calls failed", n)
	}
	if n := negativeTokens.Load(); n > 0 {
		t.Errorf("token bucket: %d results with negative tokens or time since refill", n)
	}
	if n := negativeElapsed.Load(); n > 0 {
		t.Errorf("sliding window: %d results with negative elapsed time", n)
	}
}

// At 7/s the time between requests is 142.857...ms. Adding that fraction to
// millisecond timestamps rounded at every step, so the last request of a full
// burst could be denied.
func TestRedisStore_CheckAndAdvanceTAT_FullBurst(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	window := ratelimit.Window{Rate: 7, Period: time.Second}

	for k := range 20 {
		key := fmt.Sprintf("key-%d", k)
		for i := range 7 {
			_, applied, err := store.CheckAndAdvanceTAT(ctx, key, 7, window, 1)
			if err != nil {
				t.Fatalf("%s request %d: %v", key, i+1, err)
			}
			if !applied {
				t.Fatalf("%s: request %d of a burst of 7 was denied", key, i+1)
			}
		}
	}
}

// At 600,000/s a token takes 1.67µs. The leaky bucket limiter computed with
// that in nanoseconds while this store used whole microseconds, so a drained
// bucket reported 239,855 remaining and the next denial said retry in 0s.
func TestRedisStore_LeakyBucketLimiterAgrees(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	limiter, err := ratelimit.New(
		ratelimit.WithAlgorithm(ratelimit.LeakyBucket),
		ratelimit.PerSecond(600_000),
		ratelimit.WithStore(store),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	r, err := limiter.AllowN(ctx, "key", 600_000)
	if err != nil {
		t.Fatalf("full burst: %v", err)
	}
	if !r.Allowed || r.Remaining != 0 {
		t.Errorf("full burst: Allowed=%v Remaining=%d, want allowed with 0 remaining", r.Allowed, r.Remaining)
	}

	r, err = limiter.AllowN(ctx, "key", 10_000)
	if err != nil {
		t.Fatalf("AllowN(10000): %v", err)
	}
	if r.Allowed || r.RetryAfter <= 0 {
		t.Errorf("AllowN(10000) after the burst: Allowed=%v RetryAfter=%v, want denied with a positive wait", r.Allowed, r.RetryAfter)
	}
}
