package ratelimit

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestNew_AcceptsValidSettings(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
	}{
		{"defaults", nil},
		{"period of 1ms", []Option{WithRate(1, time.Millisecond)}},
		{"token bucket at one token per microsecond", []Option{WithAlgorithm(TokenBucket), PerSecond(1_000_000)}},
		{"leaky bucket at one token per microsecond", []Option{WithAlgorithm(LeakyBucket), PerSecond(1_000_000)}},
		{"burst 0 means the rate", []Option{WithAlgorithm(TokenBucket), WithBurst(0)}},
		{"rate string", []Option{WithRateString("50-M")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.opts...); err != nil {
				t.Fatalf("New: %v", err)
			}
		})
	}
}

func TestNew_RejectsInvalidSettings(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
	}{
		{"rate 0", []Option{WithRate(0, time.Second)}},
		{"negative rate", []Option{WithAlgorithm(LeakyBucket), WithRate(-10, time.Minute)}},
		{"period 0", []Option{WithRate(5, 0)}},
		{"negative period", []Option{WithRate(5, -time.Minute)}},
		{"period under 1ms", []Option{WithRate(1, 500*time.Microsecond)}},
		{"period not a whole number of milliseconds", []Option{WithRate(1, 1500*time.Microsecond)}},
		{"negative burst", []Option{WithAlgorithm(TokenBucket), WithBurst(-5)}},
		{"token bucket under 1µs per token", []Option{WithAlgorithm(TokenBucket), PerSecond(2_000_000)}},
		{"leaky bucket under 1µs per token", []Option{WithAlgorithm(LeakyBucket), PerSecond(1_000_001)}},
		{"refill over about 146 years", []Option{WithAlgorithm(LeakyBucket), WithRate(1, 24*time.Hour), WithBurst(53_376)}},
		{"sliding window period over about 146 years", []Option{WithAlgorithm(SlidingWindow), WithRate(1, 200*365*24*time.Hour)}},
		{"unknown algorithm", []Option{WithAlgorithm(Algorithm(99))}},
		{"malformed rate string", []Option{WithRateString("100-X")}},
		{"malformed rate string followed by a valid rate", []Option{WithRateString("abc"), WithRate(5, time.Second)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter, err := New(tt.opts...)
			if err == nil {
				t.Fatal("New returned no error")
			}
			if limiter != nil {
				t.Error("New returned a Limiter along with the error")
			}
		})
	}
}

func TestWindow_EmissionInterval(t *testing.T) {
	tests := []struct {
		window Window
		want   time.Duration
	}{
		{Window{Rate: 10, Period: time.Minute}, 6 * time.Second},
		{Window{Rate: 3, Period: time.Second}, 333334 * time.Microsecond},
		{Window{Rate: 7, Period: time.Second}, 142858 * time.Microsecond},
		{Window{Rate: 600_000, Period: time.Second}, 2 * time.Microsecond},
		{Window{Rate: 1_000_000, Period: time.Second}, time.Microsecond},
	}

	for _, tt := range tests {
		if got := tt.window.EmissionInterval(); got != tt.want {
			t.Errorf("%d per %v: EmissionInterval = %v, want %v", tt.window.Rate, tt.window.Period, got, tt.want)
		}
	}
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
			w, err := ParseRateString(tt.input)
			if err != nil {
				t.Fatalf("ParseRateString(%q): %v", tt.input, err)
			}
			if w.Rate != tt.wantRate {
				t.Errorf("Rate = %d, want %d", w.Rate, tt.wantRate)
			}
			if w.Period != tt.wantPeriod {
				t.Errorf("Period = %v, want %v", w.Period, tt.wantPeriod)
			}
		})
	}
}

func TestParseRateString_Invalid(t *testing.T) {
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
			if _, err := ParseRateString(tt.input); err == nil {
				t.Errorf("ParseRateString(%q) returned no error", tt.input)
			}
		})
	}
}

func TestAllowN_CostRules(t *testing.T) {
	ctx := context.Background()
	algorithms := []struct {
		name string
		algo Algorithm
	}{
		{"fixed window", FixedWindow},
		{"sliding window", SlidingWindow},
		{"token bucket", TokenBucket},
		{"leaky bucket", LeakyBucket},
	}

	for _, a := range algorithms {
		t.Run(a.name, func(t *testing.T) {
			var results []Result
			limiter := mustNew(t,
				WithAlgorithm(a.algo),
				PerMinute(5),
				WithOnResult(func(_ context.Context, _ string, r Result) {
					results = append(results, r)
				}),
			)
			limiter.Allow(ctx, "k")

			for _, n := range []int64{6, math.MaxInt64} {
				r, err := limiter.AllowN(ctx, "k", n)
				if err != nil {
					t.Fatalf("AllowN(%d): %v", n, err)
				}
				if r.Allowed || r.RetryAfter != InfDuration || r.Remaining != 4 {
					t.Errorf("AllowN(%d) = %+v, want denied with RetryAfter InfDuration and Remaining 4", n, r)
				}
			}
			if r, _ := limiter.AllowN(ctx, "k", 4); !r.Allowed {
				t.Fatal("AllowN(4) should get the quota the larger costs left untouched")
			}

			if _, err := limiter.AllowN(ctx, "k", -100); !errors.Is(err, ErrInvalidCost) {
				t.Errorf("AllowN(-100): err = %v, want ErrInvalidCost", err)
			}
			if r, _ := limiter.AllowN(ctx, "k", 0); !r.Allowed || r.Remaining != 0 {
				t.Errorf("AllowN(0) with no quota left = %+v, want allowed with Remaining 0", r)
			}
			if r, _ := limiter.Allow(ctx, "k"); r.Allowed {
				t.Error("Allow after the quota is used should be denied")
			}

			if len(results) != 6 {
				t.Fatalf("OnResult called %d times, want 6 (once per call that returned no error)", len(results))
			}
			if results[1].RetryAfter != InfDuration || results[2].RetryAfter != InfDuration {
				t.Errorf("OnResult for the larger costs got RetryAfter %v and %v, want InfDuration", results[1].RetryAfter, results[2].RetryAfter)
			}
		})
	}
}

// A denied request reports what's actually left, the same for every algorithm,
// since that's what X-RateLimit-Remaining shows.
func TestAllowN_DeniedReportsRemaining(t *testing.T) {
	ctx := context.Background()
	algorithms := []struct {
		name string
		algo Algorithm
	}{
		{"fixed window", FixedWindow},
		{"sliding window", SlidingWindow},
		{"token bucket", TokenBucket},
		{"leaky bucket", LeakyBucket},
	}

	for _, a := range algorithms {
		t.Run(a.name, func(t *testing.T) {
			limiter := mustNew(t, WithAlgorithm(a.algo), PerMinute(5))
			limiter.AllowN(ctx, "k", 4)

			if r, _ := limiter.AllowN(ctx, "k", 2); r.Allowed || r.Remaining != 1 {
				t.Errorf("AllowN(2) with 1 left = %+v, want denied with Remaining 1", r)
			}
		})
	}
}

// Limiters with different settings that share a store and a key keep separate
// state: neither uses up the other's budget or reads the other's window.
func TestLimitersWithDifferentSettingsKeepSeparateState(t *testing.T) {
	ctx := context.Background()
	algorithms := []struct {
		name string
		algo Algorithm
	}{
		{"fixed window", FixedWindow},
		{"sliding window", SlidingWindow},
		{"token bucket", TokenBucket},
		{"leaky bucket", LeakyBucket},
	}

	for _, a := range algorithms {
		t.Run(a.name, func(t *testing.T) {
			store := NewMemoryStore()
			api := mustNew(t, WithAlgorithm(a.algo), WithStore(store), PerMinute(100))
			login := mustNew(t, WithAlgorithm(a.algo), WithStore(store), PerSecond(5))

			api.Allow(ctx, "203.0.113.7")
			if r, _ := login.AllowN(ctx, "203.0.113.7", 5); !r.Allowed {
				t.Errorf("login AllowN(5) after one api request = %+v, want allowed", r)
			}
			if r, _ := api.AllowN(ctx, "203.0.113.7", 99); !r.Allowed {
				t.Errorf("api AllowN(99) after login used its 5 = %+v, want allowed", r)
			}
		})
	}
}
