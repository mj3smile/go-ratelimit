package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestTokenBucket_Allow(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		opts          []Option
		requests      int
		wantAllowed   []bool
		wantRemaining []int64
		wantLimit     int64
	}{
		{
			name:          "default burst equals rate",
			opts:          []Option{WithAlgorithm(TokenBucket), WithRate(3, time.Minute)},
			requests:      4,
			wantAllowed:   []bool{true, true, true, false},
			wantRemaining: []int64{2, 1, 0, 0},
			wantLimit:     3,
		},
		{
			name:          "explicit burst smaller than rate",
			opts:          []Option{WithAlgorithm(TokenBucket), WithRate(10, time.Minute), WithBurst(2)},
			requests:      3,
			wantAllowed:   []bool{true, true, false},
			wantRemaining: []int64{1, 0, 0},
			wantLimit:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := New(tt.opts...)
			for i := 0; i < tt.requests; i++ {
				result, err := limiter.Allow(ctx, "test-key")
				if err != nil {
					t.Fatalf("request %d: unexpected error: %v", i, err)
				}
				if result.Allowed != tt.wantAllowed[i] {
					t.Errorf("request %d: Allowed = %v, want %v", i, result.Allowed, tt.wantAllowed[i])
				}
				if result.Remaining != tt.wantRemaining[i] {
					t.Errorf("request %d: Remaining = %d, want %d", i, result.Remaining, tt.wantRemaining[i])
				}
				if result.Limit != tt.wantLimit {
					t.Errorf("request %d: Limit = %d, want %d", i, result.Limit, tt.wantLimit)
				}
			}
		})
	}
}

func TestTokenBucket_AllowN(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		rate        int64
		burst       int64
		calls       []int64
		wantAllowed []bool
	}{
		{
			name:        "weighted requests within capacity",
			rate:        10,
			burst:       10,
			calls:       []int64{3, 5, 2},
			wantAllowed: []bool{true, true, true},
		},
		{
			name:        "denied request preserves tokens",
			rate:        10,
			burst:       10,
			calls:       []int64{7, 5, 3},
			wantAllowed: []bool{true, false, true},
		},
		{
			name:        "n exceeds capacity is denied, then capacity-sized request fits",
			rate:        10,
			burst:       5,
			calls:       []int64{6, 5},
			wantAllowed: []bool{false, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := New(
				WithAlgorithm(TokenBucket),
				WithRate(tt.rate, time.Minute),
				WithBurst(tt.burst),
			)
			for i, n := range tt.calls {
				r, _ := limiter.AllowN(ctx, "k", n)
				if r.Allowed != tt.wantAllowed[i] {
					t.Errorf("call %d (n=%d): Allowed = %v, want %v", i, n, r.Allowed, tt.wantAllowed[i])
				}
			}
		})
	}
}

func TestTokenBucket_RefillAfterDrain(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	// 60/min: emission interval = 1s, capacity = 60.
	limiter := newTokenBucket(Window{Rate: 60, Period: time.Minute}, 60, store, nil)

	// Drain.
	r, _ := limiter.AllowN(ctx, "k", 60)
	if !r.Allowed {
		t.Fatal("setup: AllowN(60) on full bucket should fit")
	}

	// Immediately after drain: empty.
	r, _ = limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("drained bucket should deny")
	}

	// Advance 1s → 1 token refilled, 1 consumed.
	now = now.Add(time.Second)
	r, _ = limiter.Allow(ctx, "k")
	if !r.Allowed {
		t.Fatal("after 1s, 1 token should be available")
	}
	if r.Remaining != 0 {
		t.Errorf("Remaining after 1-token refill+consume = %d, want 0", r.Remaining)
	}

	// Advance 5s → 5 tokens refilled. Consume 3, 2 remain.
	now = now.Add(5 * time.Second)
	r, _ = limiter.AllowN(ctx, "k", 3)
	if !r.Allowed {
		t.Fatal("after 5s, AllowN(3) should fit")
	}
	if r.Remaining != 2 {
		t.Errorf("Remaining after 5-token refill and 3-token consume = %d, want 2", r.Remaining)
	}
}

func TestTokenBucket_LazyAdvancePreservesFractionalTime(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	// 60/min: emission interval = 1s.
	limiter := newTokenBucket(Window{Rate: 60, Period: time.Minute}, 60, store, nil)

	// Drain. lastRefill = t=0.
	limiter.AllowN(ctx, "k", 60)

	// t=500ms: half an emission interval; no token credited.
	// Critically, lastRefill must NOT advance to t=500ms — the fractional
	// time would be lost and the next call at t=1s would see only 500ms.
	now = now.Add(500 * time.Millisecond)
	r, _ := limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("after 500ms (< 1s emission), should still deny")
	}

	// t=1s: cumulative 1s since lastRefill=t=0 → 1 token credited.
	// If lastRefill had eagerly tracked the t=500ms call, only 500ms would
	// be visible here and we'd be denied. Load-bearing for the lazy-advance design.
	now = now.Add(500 * time.Millisecond)
	r, _ = limiter.Allow(ctx, "k")
	if !r.Allowed {
		t.Fatal("after cumulative 1s, 1 token should be credited (lazy advance)")
	}
}

func TestTokenBucket_RetryAfter(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	// rate=1/10s: emission interval = 10s. Capacity=10.
	limiter := newTokenBucket(Window{Rate: 1, Period: 10 * time.Second}, 10, store, nil)

	// Drain.
	limiter.AllowN(ctx, "k", 10)

	// t=0, drained: deficit=1, retry = 1*10s - 0 = 10s.
	r, _ := limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("drained bucket should deny")
	}
	if r.RetryAfter != 10*time.Second {
		t.Errorf("RetryAfter at t=0 = %v, want 10s", r.RetryAfter)
	}

	// t=3s, no refill yet (3s < 10s interval): sinceLastRefill=3s.
	// retry = 10s - 3s = 7s.
	now = now.Add(3 * time.Second)
	r, _ = limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("at t=3s should still deny (no refill yet)")
	}
	if r.RetryAfter != 7*time.Second {
		t.Errorf("RetryAfter at t=3s = %v, want 7s", r.RetryAfter)
	}

	// AllowN(3) at t=3s on drained bucket: deficit=3, retry = 3*10s - 3s = 27s.
	r, _ = limiter.AllowN(ctx, "k", 3)
	if r.Allowed {
		t.Fatal("AllowN(3) on drained bucket should deny")
	}
	if r.RetryAfter != 27*time.Second {
		t.Errorf("RetryAfter for AllowN(3) at t=3s = %v, want 27s", r.RetryAfter)
	}
}

func TestTokenBucket_Reset(t *testing.T) {
	ctx := context.Background()

	limiter := New(WithAlgorithm(TokenBucket), WithRate(1, time.Minute))

	r, _ := limiter.Allow(ctx, "k")
	if !r.Allowed {
		t.Fatal("first request should be allowed")
	}
	r, _ = limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("second request should be denied")
	}

	if err := limiter.Reset(ctx, "k"); err != nil {
		t.Fatalf("reset error: %v", err)
	}

	r, _ = limiter.Allow(ctx, "k")
	if !r.Allowed {
		t.Fatal("after reset, bucket should be full again")
	}
}

func TestTokenBucket_OnResult(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "trace-789")

	var calls int
	var gotKey string
	var gotTrace any
	limiter := New(
		WithAlgorithm(TokenBucket),
		WithRate(1, time.Minute),
		WithOnResult(func(cbCtx context.Context, key string, result Result) {
			calls++
			gotKey = key
			gotTrace = cbCtx.Value(ctxKey{})
		}),
	)
	limiter.Allow(ctx, "k")
	if calls != 1 {
		t.Errorf("OnResult called %d times, want 1", calls)
	}
	if gotKey != "k" {
		t.Errorf("OnResult key = %q, want %q", gotKey, "k")
	}
	if gotTrace != "trace-789" {
		t.Errorf("OnResult ctx value = %v, want %q", gotTrace, "trace-789")
	}
}

func TestTokenBucket_SeparateKeys(t *testing.T) {
	ctx := context.Background()

	limiter := New(WithAlgorithm(TokenBucket), WithRate(1, time.Minute))

	r, _ := limiter.Allow(ctx, "a")
	if !r.Allowed {
		t.Fatal("first request for key 'a' should be allowed")
	}
	r, _ = limiter.Allow(ctx, "a")
	if r.Allowed {
		t.Fatal("second request for key 'a' should be denied")
	}

	r, _ = limiter.Allow(ctx, "b")
	if !r.Allowed {
		t.Fatal("first request for key 'b' should be allowed (independent bucket)")
	}
}