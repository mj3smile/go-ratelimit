package ratelimit

import (
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