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
		{"SlidingWindow", SlidingWindow},
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
	ctx := context.Background()

	var calls []Result
	var keys []string
	limiter := New(
		WithRate(2, time.Minute),
		WithOnResult(func(key string, result Result) {
			calls = append(calls, result)
			keys = append(keys, key)
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
