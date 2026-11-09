package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestLeakyBucket_Allow(t *testing.T) {
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
			opts:          []Option{WithAlgorithm(LeakyBucket), WithRate(3, time.Minute)},
			requests:      4,
			wantAllowed:   []bool{true, true, true, false},
			wantRemaining: []int64{2, 1, 0, 0},
			wantLimit:     3,
		},
		{
			name:          "explicit burst smaller than rate",
			opts:          []Option{WithAlgorithm(LeakyBucket), WithRate(10, time.Minute), WithBurst(2)},
			requests:      3,
			wantAllowed:   []bool{true, true, false},
			wantRemaining: []int64{1, 0, 0},
			wantLimit:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := mustNew(t, tt.opts...)
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

func TestLeakyBucket_AllowN(t *testing.T) {
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
			name:        "denied request preserves TAT",
			rate:        10,
			burst:       10,
			calls:       []int64{7, 5, 3},
			wantAllowed: []bool{true, false, true},
		},
		{
			name:        "n exceeds burst is denied, then burst-sized request fits",
			rate:        10,
			burst:       5,
			calls:       []int64{6, 5},
			wantAllowed: []bool{false, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := mustNew(t,
				WithAlgorithm(LeakyBucket),
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

func TestLeakyBucket_DrainAndIdle(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	// 60/min: emission interval = 1s, burst = 60.
	limiter := newLeakyBucket(Window{Rate: 60, Period: time.Minute}, 60, store, nil)

	// Drain: TAT advances 60s into the future.
	r, _ := limiter.AllowN(ctx, "k", 60)
	if !r.Allowed {
		t.Fatal("setup: AllowN(60) on fresh key should fit")
	}

	// Immediately after drain: denied.
	r, _ = limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("drained bucket should deny")
	}

	// Advance 1s: TAT is still 59s ahead, but now is 1s closer.
	// delay for n=1 = (TAT - now) + 1s = 59s + 1s = 60s = tolerance. Fits.
	now = now.Add(time.Second)
	r, _ = limiter.Allow(ctx, "k")
	if !r.Allowed {
		t.Fatal("after 1s, 1 slot should be available")
	}
	if r.Remaining != 0 {
		t.Errorf("Remaining = %d, want 0", r.Remaining)
	}

	// Advance 5s more: 5 slots freed.
	now = now.Add(5 * time.Second)
	r, _ = limiter.AllowN(ctx, "k", 3)
	if !r.Allowed {
		t.Fatal("after 5s, AllowN(3) should fit")
	}
	if r.Remaining != 2 {
		t.Errorf("Remaining = %d, want 2", r.Remaining)
	}

	// Full idle: advance past TAT entirely, full burst available.
	now = now.Add(2 * time.Minute)
	r, _ = limiter.AllowN(ctx, "k", 60)
	if !r.Allowed {
		t.Fatal("after full idle, entire burst should fit")
	}
	if r.Remaining != 0 {
		t.Errorf("Remaining = %d, want 0", r.Remaining)
	}
}

func TestLeakyBucket_RetryAfter(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	// rate=1/10s: emission interval = 10s. Burst=10, tolerance=100s.
	limiter := newLeakyBucket(Window{Rate: 1, Period: 10 * time.Second}, 10, store, nil)

	// Drain: TAT = now + 10*10s = now + 100s.
	limiter.AllowN(ctx, "k", 10)

	// t=0, drained: newTAT = TAT + 10s = now+110s, delay=110s, tolerance=100s.
	// retry = 110s - 100s = 10s.
	r, _ := limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("drained bucket should deny")
	}
	if r.RetryAfter != 10*time.Second {
		t.Errorf("RetryAfter at t=0 = %v, want 10s", r.RetryAfter)
	}

	// t=3s: TAT=now+100s (original), now is 3s later.
	// newTAT = TAT + 10s = original_now+100s+10s, delay = 110s-3s = 107s.
	// retry = 107s - 100s = 7s.
	now = now.Add(3 * time.Second)
	r, _ = limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("at t=3s should still deny")
	}
	if r.RetryAfter != 7*time.Second {
		t.Errorf("RetryAfter at t=3s = %v, want 7s", r.RetryAfter)
	}

	// AllowN(3) at t=3s: newTAT = TAT + 30s = original_now+130s, delay = 127s.
	// retry = 127s - 100s = 27s.
	r, _ = limiter.AllowN(ctx, "k", 3)
	if r.Allowed {
		t.Fatal("AllowN(3) on drained bucket should deny")
	}
	if r.RetryAfter != 27*time.Second {
		t.Errorf("RetryAfter for AllowN(3) at t=3s = %v, want 27s", r.RetryAfter)
	}
}

func TestLeakyBucket_RetryAfterAtLargestBurst(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	// 1/day with the largest burst New accepts: 53,375 days to refill. A second
	// full burst puts the bucket 106,750 days behind, which must fit in a Duration.
	limiter := mustNew(t,
		WithAlgorithm(LeakyBucket),
		WithRate(1, 24*time.Hour),
		WithBurst(53_375),
		WithStore(store),
	)

	if r, _ := limiter.AllowN(ctx, "k", 53_375); !r.Allowed {
		t.Fatal("first full burst should be allowed")
	}
	r, _ := limiter.AllowN(ctx, "k", 53_375)
	if r.Allowed {
		t.Fatal("second full burst should be denied")
	}
	if want := 53_375 * 24 * time.Hour; r.RetryAfter != want {
		t.Errorf("RetryAfter = %v, want %v", r.RetryAfter, want)
	}
}

func TestLeakyBucket_Reset(t *testing.T) {
	ctx := context.Background()

	limiter := mustNew(t, WithAlgorithm(LeakyBucket), WithRate(1, time.Minute))

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
		t.Fatal("after reset, full burst should be available")
	}
}

func TestLeakyBucket_OnResult(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "trace-gcra")

	var calls int
	var gotKey string
	var gotTrace any
	limiter := mustNew(t,
		WithAlgorithm(LeakyBucket),
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
	if gotTrace != "trace-gcra" {
		t.Errorf("OnResult ctx value = %v, want %q", gotTrace, "trace-gcra")
	}
}

func TestLeakyBucket_SeparateKeys(t *testing.T) {
	ctx := context.Background()

	limiter := mustNew(t, WithAlgorithm(LeakyBucket), WithRate(1, time.Minute))

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
		t.Fatal("first request for key 'b' should be allowed (independent TAT)")
	}
}
