package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestSlidingWindow_Allow(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		rate          int64
		requests      int
		wantAllowed   []bool
		wantRemaining []int64
	}{
		{
			name:          "allows requests under limit",
			rate:          3,
			requests:      3,
			wantAllowed:   []bool{true, true, true},
			wantRemaining: []int64{2, 1, 0},
		},
		{
			name:          "blocks requests over limit",
			rate:          2,
			requests:      4,
			wantAllowed:   []bool{true, true, false, false},
			wantRemaining: []int64{1, 0, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := New(WithAlgorithm(SlidingWindow), WithRate(tt.rate, time.Minute))

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
				if result.Limit != tt.rate {
					t.Errorf("request %d: Limit = %d, want %d", i, result.Limit, tt.rate)
				}
			}
		})
	}
}

func TestSlidingWindow_AllowN(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		rate        int64
		calls       []int64
		wantAllowed []bool
	}{
		{
			name:        "weighted requests within limit",
			rate:        10,
			calls:       []int64{3, 5, 2},
			wantAllowed: []bool{true, true, true},
		},
		{
			name:        "denied request preserves quota, next smaller request fits",
			rate:        10,
			calls:       []int64{7, 5, 3},
			wantAllowed: []bool{true, false, true},
		},
		{
			name:        "large request denied on fresh key, quota untouched",
			rate:        5,
			calls:       []int64{6, 3, 2},
			wantAllowed: []bool{false, true, true},
		},
		{
			name:        "repeated denials do not drain quota",
			rate:        5,
			calls:       []int64{10, 10, 10},
			wantAllowed: []bool{false, false, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := New(WithAlgorithm(SlidingWindow), WithRate(tt.rate, time.Minute))
			for i, n := range tt.calls {
				result, _ := limiter.AllowN(ctx, "k", n)
				if result.Allowed != tt.wantAllowed[i] {
					t.Errorf("call %d (n=%d): Allowed = %v, want %v", i, n, result.Allowed, tt.wantAllowed[i])
				}
			}
		})
	}
}

func TestSlidingWindow_BucketRotation(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	limiter := newSlidingWindow(Window{Rate: 10, Period: 10 * time.Second}, store, nil)

	// t=0: fill bucket A with 10 requests.
	for i := 0; i < 10; i++ {
		r, _ := limiter.Allow(ctx, "k")
		if !r.Allowed {
			t.Fatalf("setup: request %d should be allowed", i)
		}
	}

	// t=5s: still in bucket A. estimate = curr = 10. +1 → denied.
	now = now.Add(5 * time.Second)
	r, _ := limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Errorf("at t=5s: estimate=10, +1 should deny")
	}

	// t=15s: rotation happened at t=10s. prev=10, curr=0, elapsed=5s.
	// weight=0.5, estimate=5. +6 denied, +5 fits.
	now = now.Add(10 * time.Second)
	r, _ = limiter.AllowN(ctx, "k", 6)
	if r.Allowed {
		t.Errorf("at t=15s: estimate=5, +6 should deny")
	}
	r, _ = limiter.AllowN(ctx, "k", 5)
	if !r.Allowed {
		t.Errorf("at t=15s: estimate=5, +5 should allow")
	}

	// t=25s: rotation at t=20s. prev=5 (old curr), curr=0, elapsed=5s.
	// weight=0.5, estimate=int64(2.5)=2. +8 fits.
	now = now.Add(10 * time.Second)
	r, _ = limiter.AllowN(ctx, "k", 8)
	if !r.Allowed {
		t.Errorf("at t=25s: estimate=2, +8 should allow")
	}
}

func TestSlidingWindow_RetryAfter(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	limiter := newSlidingWindow(Window{Rate: 10, Period: 10 * time.Second}, store, nil)

	for i := 0; i < 10; i++ {
		limiter.Allow(ctx, "k")
	}

	// t=5s: prev=0, curr=10, n=1. Fallback to untilRotation = 5s.
	now = now.Add(5 * time.Second)
	r, _ := limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("should be denied")
	}
	if r.RetryAfter != 5*time.Second {
		t.Errorf("RetryAfter = %v, want 5s (prev=0 → fallback to rotation)", r.RetryAfter)
	}

	// t=15s: rotation. prev=10, curr=0, elapsed=5s. AllowN(6) denied.
	// budget = 10-6-0 = 4. dt = 10 - 5 - 4*10/10 = 1s.
	now = now.Add(10 * time.Second)
	r, _ = limiter.AllowN(ctx, "k", 6)
	if r.Allowed {
		t.Fatal("should be denied")
	}
	if r.RetryAfter != 1*time.Second {
		t.Errorf("RetryAfter = %v, want 1s (decay solve)", r.RetryAfter)
	}
}

func TestSlidingWindow_Reset(t *testing.T) {
	ctx := context.Background()

	limiter := New(WithAlgorithm(SlidingWindow), WithRate(1, time.Minute))

	limiter.Allow(ctx, "k")
	r, _ := limiter.Allow(ctx, "k")
	if r.Allowed {
		t.Fatal("should be blocked after exhausting limit")
	}

	if err := limiter.Reset(ctx, "k"); err != nil {
		t.Fatalf("reset error: %v", err)
	}

	r, _ = limiter.Allow(ctx, "k")
	if !r.Allowed {
		t.Fatal("should be allowed after reset")
	}
}

func TestSlidingWindow_OnResult(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "trace-456")

	var calls int
	var gotTrace any
	limiter := New(
		WithAlgorithm(SlidingWindow),
		WithRate(1, time.Minute),
		WithOnResult(func(cbCtx context.Context, key string, result Result) {
			calls++
			gotTrace = cbCtx.Value(ctxKey{})
		}),
	)
	limiter.Allow(ctx, "k")
	if calls != 1 {
		t.Errorf("OnResult called %d times, want 1", calls)
	}
	if gotTrace != "trace-456" {
		t.Errorf("ctx value = %v, want %q", gotTrace, "trace-456")
	}
}