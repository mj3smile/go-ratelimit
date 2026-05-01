package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Algorithm represents a rate limiting algorithm.
type Algorithm int

const (
	// FixedWindow divides time into fixed-duration windows and counts
	// requests within each window.
	FixedWindow Algorithm = iota

	// SlidingWindow uses a sliding time window for smoother rate limiting.
	SlidingWindow

	// TokenBucket allows bursts up to a bucket capacity while maintaining
	// a steady average rate.
	TokenBucket

	// LeakyBucket processes requests at a fixed rate, queuing or rejecting
	// excess requests that arrive faster than the drain rate.
	LeakyBucket
)

// Result holds the outcome of a rate limit check.
type Result struct {
	// Allowed is true if the request is within the rate limit.
	Allowed bool

	// Remaining is the number of requests remaining in the current window.
	Remaining int64

	// RetryAfter indicates how long the caller should wait before retrying.
	// Zero when Allowed is true.
	RetryAfter time.Duration

	// Limit is the maximum number of requests allowed in the window.
	Limit int64
}

// Window describes a rate limit window.
type Window struct {
	// Rate is the maximum number of requests allowed per period.
	Rate int64

	// Period is the duration of the rate limit window.
	Period time.Duration
}

// Limiter checks whether a request identified by key is allowed.
type Limiter interface {
	// Allow checks whether a single request for the given key is within the rate limit.
	// It is equivalent to AllowN(ctx, key, 1).
	Allow(ctx context.Context, key string) (Result, error)

	// AllowN checks whether n requests for the given key are within the rate limit.
	// Use this for weighted requests where some operations cost more than others.
	AllowN(ctx context.Context, key string, n int64) (Result, error)

	// Reset clears the rate limit state for the given key.
	Reset(ctx context.Context, key string) error
}

// Store is the storage backend interface for rate limit state.
type Store interface {
	// CheckAndIncrement atomically increments the counter for a key by n within
	// the given window, but only if the resulting count would not exceed
	// window.Rate. It returns the current count (after increment if applied,
	// otherwise unchanged) and whether the increment was applied.
	CheckAndIncrement(ctx context.Context, key string, window Window, n int64) (count int64, applied bool, err error)

	// CheckAndIncrementSliding atomically applies a 2-bucket sliding window
	// check for the given key. It maintains two consecutive buckets of size
	// window.Period: a current bucket (in progress) and a previous bucket
	// (just completed).
	//
	// The check uses a weighted estimate of requests in the trailing
	// window.Period:
	//
	//	weight   = (window.Period - elapsed) / window.Period
	//	estimate = prev*weight + curr
	//
	// If estimate+n <= window.Rate, the increment is applied to the current
	// bucket and applied=true. Otherwise applied=false and no state changes.
	//
	// curr and prev reflect state after any bucket rotation and after the
	// increment (if applied). elapsed is how far into the current bucket the
	// operation occurred (0 <= elapsed < window.Period).
	CheckAndIncrementSliding(ctx context.Context, key string, window Window, n int64) (curr, prev int64, elapsed time.Duration, applied bool, err error)

	// CheckAndConsumeTokens atomically applies a token-bucket check for the
	// given key. The bucket has capacity tokens and refills at window.Rate
	// tokens per window.Period. If at least n tokens are available after
	// refill, n are consumed and applied=true. Otherwise applied=false and
	// only the refill is applied (no tokens consumed).
	//
	// tokens reflects the post-state token count. sinceLastRefill is the time
	// elapsed since the last credited refill — always less than the emission
	// interval, where the emission interval is the duration needed to
	// produce one token (window.Period / window.Rate). The caller can use it
	// to compute retry-after as:
	// (n-tokens)*emissionInterval - sinceLastRefill.
	//
	// State uses a "lazy advance" approach: lastRefill is only moved forward
	// by integer multiples of the emission interval, preserving fractional
	// refill credit between calls.
	CheckAndConsumeTokens(ctx context.Context, key string, capacity int64, window Window, n int64) (tokens int64, sinceLastRefill time.Duration, applied bool, err error)

	// Reset removes the rate limit state for the given key.
	Reset(ctx context.Context, key string) error
}

// OnResultFunc is called after every rate limit check with the request context,
// key, and result. Use this for metrics, logging, or monitoring.
//
// The context is the same one passed to Allow/AllowN, allowing the callback to
// read request-scoped values (trace IDs, tenant IDs) or attach tracing spans.
type OnResultFunc func(ctx context.Context, key string, result Result)

// Option configures a limiter.
type Option func(*config)

type config struct {
	algorithm Algorithm
	window    Window
	burst     int64
	store     Store
	onResult  OnResultFunc
}

func defaultConfig() config {
	return config{
		algorithm: FixedWindow,
		window: Window{
			Rate:   100,
			Period: time.Minute,
		},
	}
}

// WithAlgorithm sets the rate limiting algorithm.
func WithAlgorithm(algo Algorithm) Option {
	return func(c *config) {
		c.algorithm = algo
	}
}

// WithRate sets the maximum number of requests allowed per period.
func WithRate(rate int64, period time.Duration) Option {
	return func(c *config) {
		c.window = Window{Rate: rate, Period: period}
	}
}

// PerSecond is shorthand for WithRate(n, time.Second).
func PerSecond(n int64) Option { return WithRate(n, time.Second) }

// PerMinute is shorthand for WithRate(n, time.Minute).
func PerMinute(n int64) Option { return WithRate(n, time.Minute) }

// PerHour is shorthand for WithRate(n, time.Hour).
func PerHour(n int64) Option { return WithRate(n, time.Hour) }

// PerDay is shorthand for WithRate(n, 24*time.Hour).
func PerDay(n int64) Option { return WithRate(n, 24*time.Hour) }

// WithRateString sets the rate from a formatted string.
// The format is "<count>-<unit>" where unit is one of:
//   - S: per second
//   - M: per minute
//   - H: per hour
//   - D: per day
//
// Examples: "100-M" (100 per minute), "5-S" (5 per second), "1000-H" (1000 per hour).
//
// WithRateString panics if the format is invalid.
func WithRateString(rate string) Option {
	window := ParseRateString(rate)
	return func(c *config) {
		c.window = window
	}
}

// ParseRateString parses a rate string in the format "<count>-<unit>" into a Window.
// See WithRateString for the supported format.
//
// ParseRateString panics if the format is invalid.
func ParseRateString(rate string) Window {
	parts := strings.SplitN(rate, "-", 2)
	if len(parts) != 2 {
		panic(fmt.Sprintf("ratelimit: invalid rate string format: %q, expected \"<count>-<unit>\"", rate))
	}

	count, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		panic(fmt.Sprintf("ratelimit: invalid rate count in %q: %v", rate, err))
	}

	var period time.Duration
	switch strings.ToUpper(parts[1]) {
	case "S":
		period = time.Second
	case "M":
		period = time.Minute
	case "H":
		period = time.Hour
	case "D":
		period = 24 * time.Hour
	default:
		panic(fmt.Sprintf("ratelimit: invalid rate unit in %q: expected S, M, H, or D", rate))
	}

	return Window{Rate: count, Period: period}
}

// WithBurst sets the token-bucket burst capacity. Only meaningful for the
// TokenBucket algorithm; ignored otherwise.
//
// If unset (or 0), burst defaults to window.Rate, meaning the bucket can hold
// at most one full period's worth of tokens.
func WithBurst(burst int64) Option {
	return func(c *config) {
		c.burst = burst
	}
}

// WithStore sets the storage backend.
func WithStore(store Store) Option {
	return func(c *config) {
		c.store = store
	}
}

// WithOnResult sets a callback that is invoked after every rate limit check.
// Use this to integrate with metrics systems like Prometheus.
func WithOnResult(fn OnResultFunc) Option {
	return func(c *config) {
		c.onResult = fn
	}
}

// New creates a new Limiter with the given options.
// If no store is provided, an in-memory store is used.
// If no algorithm is specified, FixedWindow is used.
//
// New panics if an unsupported algorithm is specified.
func New(opts ...Option) Limiter {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.store == nil {
		cfg.store = NewMemoryStore()
	}

	switch cfg.algorithm {
	case FixedWindow:
		return newFixedWindow(cfg.window, cfg.store, cfg.onResult)
	case SlidingWindow:
		return newSlidingWindow(cfg.window, cfg.store, cfg.onResult)
	case TokenBucket:
		burst := cfg.burst
		if burst == 0 {
			burst = cfg.window.Rate
		}
		return newTokenBucket(cfg.window, burst, cfg.store, cfg.onResult)
	case LeakyBucket:
		panic("ratelimit: LeakyBucket algorithm is not yet implemented")
	default:
		panic(fmt.Sprintf("ratelimit: unknown algorithm: %d", cfg.algorithm))
	}
}
