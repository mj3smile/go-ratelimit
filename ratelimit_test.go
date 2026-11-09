package ratelimit

import (
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
