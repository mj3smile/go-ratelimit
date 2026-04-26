package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestNew_Defaults(t *testing.T) {
	limiter := New()
	if limiter == nil {
		t.Fatal("expected non-nil limiter")
	}
}

func TestNew_WithOptions(t *testing.T) {
	store := NewMemoryStore()
	limiter := New(
		WithAlgorithm(FixedWindow),
		WithRate(10, time.Second),
		WithStore(store),
	)
	if limiter == nil {
		t.Fatal("expected non-nil limiter")
	}
}

func TestNew_WithRateString(t *testing.T) {
	limiter := New(WithRateString("50-M"))
	if limiter == nil {
		t.Fatal("expected non-nil limiter")
	}
}

func TestNew_PanicsOnUnimplementedAlgorithm(t *testing.T) {
	tests := []struct {
		name string
		algo Algorithm
	}{
		{"TokenBucket", TokenBucket},
		{"LeakyBucket", LeakyBucket},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic for unimplemented algorithm")
				}
			}()
			New(WithAlgorithm(tt.algo))
		})
	}
}

func TestNew_PanicsOnUnknownAlgorithm(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unknown algorithm")
		}
	}()
	New(WithAlgorithm(Algorithm(99)))
}

func TestParseRateString(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantRate   int64
		wantPeriod time.Duration
	}{
		{"per second", "5-S", 5, time.Second},
		{"per minute", "100-M", 100, time.Minute},
		{"per hour", "1000-H", 1000, time.Hour},
		{"per day", "5000-D", 5000, 24 * time.Hour},
		{"lowercase unit", "10-s", 10, time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := ParseRateString(tt.input)
			if w.Rate != tt.wantRate {
				t.Errorf("Rate = %d, want %d", w.Rate, tt.wantRate)
			}
			if w.Period != tt.wantPeriod {
				t.Errorf("Period = %v, want %v", w.Period, tt.wantPeriod)
			}
		})
	}
}

func TestParseRateString_Panics(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"no separator", "100M"},
		{"invalid count", "abc-M"},
		{"invalid unit", "100-X"},
		{"empty string", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic for input %q", tt.input)
				}
			}()
			ParseRateString(tt.input)
		})
	}
}

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
