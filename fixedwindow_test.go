package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestFixedWindow_Allow(t *testing.T) {
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
		{
			name:          "single request allowed",
			rate:          1,
			requests:      1,
			wantAllowed:   []bool{true},
			wantRemaining: []int64{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := New(WithRate(tt.rate, time.Minute))

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

func TestFixedWindow_AllowN(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		rate          int64
		calls         []int64
		wantAllowed   []bool
		wantRemaining []int64
	}{
		{
			name:          "weighted requests within limit",
			rate:          10,
			calls:         []int64{3, 5, 2},
			wantAllowed:   []bool{true, true, true},
			wantRemaining: []int64{7, 2, 0},
		},
		{
			name:          "denied request preserves quota, next smaller request fits",
			rate:          10,
			calls:         []int64{7, 5, 3},
			wantAllowed:   []bool{true, false, true},
			wantRemaining: []int64{3, 3, 0},
		},
		{
			name:          "large request denied, quota untouched",
			rate:          5,
			calls:         []int64{6, 3, 2},
			wantAllowed:   []bool{false, true, true},
			wantRemaining: []int64{5, 2, 0},
		},
		{
			name:          "mix of small and medium costs",
			rate:          5,
			calls:         []int64{1, 3, 1},
			wantAllowed:   []bool{true, true, true},
			wantRemaining: []int64{4, 1, 0},
		},
		{
			name:          "repeated denials do not drain quota",
			rate:          5,
			calls:         []int64{10, 10, 10},
			wantAllowed:   []bool{false, false, false},
			wantRemaining: []int64{5, 5, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := New(WithRate(tt.rate, time.Minute))

			for i, n := range tt.calls {
				result, err := limiter.AllowN(ctx, "test-key", n)
				if err != nil {
					t.Fatalf("call %d: unexpected error: %v", i, err)
				}
				if result.Allowed != tt.wantAllowed[i] {
					t.Errorf("call %d (n=%d): Allowed = %v, want %v", i, n, result.Allowed, tt.wantAllowed[i])
				}
				if result.Remaining != tt.wantRemaining[i] {
					t.Errorf("call %d (n=%d): Remaining = %d, want %d", i, n, result.Remaining, tt.wantRemaining[i])
				}
			}
		})
	}
}

func TestFixedWindow_RetryAfter(t *testing.T) {
	ctx := context.Background()

	limiter := New(WithRate(1, time.Minute))

	result, err := limiter.Allow(ctx, "test-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RetryAfter != 0 {
		t.Errorf("allowed request: RetryAfter = %v, want 0", result.RetryAfter)
	}

	result, err = limiter.Allow(ctx, "test-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Allowed {
		t.Fatal("second request should be blocked")
	}
	if result.RetryAfter <= 0 {
		t.Error("blocked request: RetryAfter should be positive")
	}
	if result.RetryAfter > time.Minute {
		t.Errorf("blocked request: RetryAfter = %v, should be <= 1m", result.RetryAfter)
	}
}

func TestFixedWindow_SeparateKeys(t *testing.T) {
	ctx := context.Background()

	limiter := New(WithRate(1, time.Minute))

	result, _ := limiter.Allow(ctx, "a")
	if !result.Allowed {
		t.Fatal("first request for key 'a' should be allowed")
	}
	result, _ = limiter.Allow(ctx, "a")
	if result.Allowed {
		t.Fatal("second request for key 'a' should be blocked")
	}

	result, _ = limiter.Allow(ctx, "b")
	if !result.Allowed {
		t.Fatal("first request for key 'b' should be allowed")
	}
}

func TestFixedWindow_WindowExpiry(t *testing.T) {
	ctx := context.Background()

	store := NewMemoryStore()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	limiter := newFixedWindow(Window{Rate: 1, Period: time.Second}, store, nil)
	limiter.now = func() time.Time { return now }

	result, _ := limiter.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("first request should be allowed")
	}
	result, _ = limiter.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("second request should be blocked")
	}

	now = now.Add(2 * time.Second)

	result, _ = limiter.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("request after window expiry should be allowed")
	}
}

func TestFixedWindow_Reset(t *testing.T) {
	ctx := context.Background()

	limiter := New(WithRate(1, time.Minute))

	limiter.Allow(ctx, "key")
	result, _ := limiter.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("should be blocked after exhausting limit")
	}

	if err := limiter.Reset(ctx, "key"); err != nil {
		t.Fatalf("unexpected error on reset: %v", err)
	}

	result, _ = limiter.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("should be allowed after reset")
	}
}

func TestFixedWindow_OnResult(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "trace-123")

	var calls []Result
	var keys []string
	var gotTraces []any
	limiter := New(
		WithRate(2, time.Minute),
		WithOnResult(func(cbCtx context.Context, key string, result Result) {
			calls = append(calls, result)
			keys = append(keys, key)
			gotTraces = append(gotTraces, cbCtx.Value(ctxKey{}))
		}),
	)

	limiter.Allow(ctx, "key")
	limiter.Allow(ctx, "key")
	limiter.Allow(ctx, "key")

	if len(calls) != 3 {
		t.Fatalf("OnResult called %d times, want 3", len(calls))
	}
	if !calls[0].Allowed || !calls[1].Allowed {
		t.Error("first two calls should be allowed")
	}
	if calls[2].Allowed {
		t.Error("third call should be blocked")
	}
	for i, k := range keys {
		if k != "key" {
			t.Errorf("call %d: key = %q, want %q", i, k, "key")
		}
	}
	for i, v := range gotTraces {
		if v != "trace-123" {
			t.Errorf("call %d: ctx value = %v, want %q", i, v, "trace-123")
		}
	}
}