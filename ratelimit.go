package ratelimit

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Algorithm represents a rate limiting algorithm.
type Algorithm int

const (
	// FixedWindow counts requests in windows of a fixed duration. A key's
	// window starts with its first request and lasts one period.
	FixedWindow Algorithm = iota

	// SlidingWindow uses a sliding time window for smoother rate limiting.
	SlidingWindow

	// TokenBucket allows bursts up to a bucket capacity while maintaining
	// a steady average rate.
	TokenBucket

	// LeakyBucket implements leaky-bucket-as-meter semantics using GCRA
	// (Generic Cell Rate Algorithm). The bucket has a burst tolerance and
	// drains at a steady rate; requests that would exceed the tolerance are
	// denied. Behaviorally equivalent to TokenBucket with the same parameters,
	// but with single-field state (one TAT timestamp instead of token count +
	// lastRefill).
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

// EmissionInterval returns the time it takes to earn one token: Period divided
// by Rate, rounded up to a whole microsecond. The limiters and stores all use
// this value, so a store that counts in microseconds, such as the Redis store,
// agrees with one that counts in nanoseconds. Rounding up keeps the effective
// rate at or below the configured one. Rate must be positive.
func (w Window) EmissionInterval() time.Duration {
	d := w.Period / time.Duration(w.Rate)
	if w.Period%time.Duration(w.Rate) != 0 {
		d++
	}
	if r := d % time.Microsecond; r != 0 {
		d += time.Microsecond - r
	}
	return d
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
	// window.Rate. A key's window starts with its first increment and lasts
	// window.Period. It returns the current count (after increment if applied,
	// otherwise unchanged), the time left until the window ends and the count
	// resets, and whether the increment was applied. When no window is active,
	// resetAfter is window.Period.
	CheckAndIncrement(ctx context.Context, key string, window Window, n int64) (count int64, resetAfter time.Duration, applied bool, err error)

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
	// produce one token (window.EmissionInterval()). The caller can use it
	// to compute retry-after as:
	// (n-tokens)*emissionInterval - sinceLastRefill.
	//
	// State uses a "lazy advance" approach: lastRefill is only moved forward
	// by integer multiples of the emission interval, preserving fractional
	// refill credit between calls.
	CheckAndConsumeTokens(ctx context.Context, key string, capacity int64, window Window, n int64) (tokens int64, sinceLastRefill time.Duration, applied bool, err error)

	// CheckAndAdvanceTAT atomically applies a GCRA (Generic Cell Rate
	// Algorithm) check for the given key. It maintains a single TAT
	// (Theoretical Arrival Time) per key.
	//
	// The update rule:
	//
	//	emissionInterval = window.EmissionInterval()
	//	delayTolerance   = burst * emissionInterval
	//	newTAT           = max(now, tat) + n * emissionInterval
	//	delay            = newTAT - now
	//
	// If delay > delayTolerance, the request is denied and the TAT is
	// unchanged. Otherwise the TAT advances to newTAT and applied=true.
	//
	// delay is always returned (even on deny) so the caller can compute
	// Remaining (when allowed) or RetryAfter (when denied).
	CheckAndAdvanceTAT(ctx context.Context, key string, burst int64, window Window, n int64) (delay time.Duration, applied bool, err error)

	// ResetFixed removes the fixed-window state for the given key.
	ResetFixed(ctx context.Context, key string) error

	// ResetSliding removes the sliding-window state for the given key.
	ResetSliding(ctx context.Context, key string) error

	// ResetTokens removes the token-bucket state for the given key.
	ResetTokens(ctx context.Context, key string) error

	// ResetTAT removes the GCRA (leaky bucket) state for the given key.
	ResetTAT(ctx context.Context, key string) error
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
	err       error
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

// WithRate sets the maximum number of requests allowed per period. See New for
// the values it accepts.
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

// WithRateString sets the rate from a formatted string, such as a limit read
// from configuration. The format is "<count>-<unit>" where unit is one of:
//   - S: per second
//   - M: per minute
//   - H: per hour
//   - D: per day
//
// Examples: "100-M" (100 per minute), "5-S" (5 per second), "1000-H" (1000 per hour).
//
// If the string is malformed, New returns the error.
func WithRateString(rate string) Option {
	window, err := ParseRateString(rate)
	return func(c *config) {
		if err != nil {
			c.err = err
			return
		}
		c.window = window
	}
}

// ParseRateString parses a rate string in the format "<count>-<unit>" into a Window.
// See WithRateString for the supported format.
func ParseRateString(rate string) (Window, error) {
	parts := strings.SplitN(rate, "-", 2)
	if len(parts) != 2 {
		return Window{}, fmt.Errorf("ratelimit: invalid rate string format: %q, expected \"<count>-<unit>\"", rate)
	}

	count, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Window{}, fmt.Errorf("ratelimit: invalid rate count in %q: %w", rate, err)
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
		return Window{}, fmt.Errorf("ratelimit: invalid rate unit in %q: expected S, M, H, or D", rate)
	}

	return Window{Rate: count, Period: period}, nil
}

// WithBurst sets the burst capacity for token-based algorithms
// (TokenBucket, LeakyBucket). Ignored by window-based algorithms.
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
// New returns an error, and no Limiter, for settings it can't enforce:
//   - a rate below 1, or a period that is not a whole number of milliseconds
//     of at least 1ms;
//   - a negative burst;
//   - for TokenBucket and LeakyBucket, a rate faster than one token per
//     microsecond (more than 1,000,000 per second), or a burst that would take
//     more than about 146 years to refill;
//   - for SlidingWindow, a period longer than about 146 years;
//   - an unknown algorithm, or a malformed string passed to WithRateString.
func New(opts ...Option) (Limiter, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	switch cfg.algorithm {
	case FixedWindow:
		return newFixedWindow(cfg.window, cfg.storeOrDefault(), cfg.onResult), nil
	case SlidingWindow:
		return newSlidingWindow(cfg.window, cfg.storeOrDefault(), cfg.onResult), nil
	case TokenBucket:
		return newTokenBucket(cfg.window, cfg.bucketBurst(), cfg.storeOrDefault(), cfg.onResult), nil
	case LeakyBucket:
		return newLeakyBucket(cfg.window, cfg.bucketBurst(), cfg.storeOrDefault(), cfg.onResult), nil
	default:
		return nil, fmt.Errorf("ratelimit: unknown algorithm: %d", cfg.algorithm)
	}
}

const maxDuration = time.Duration(math.MaxInt64)

func (c config) validate() error {
	if c.err != nil {
		return c.err
	}
	w := c.window
	if w.Rate < 1 {
		return fmt.Errorf("ratelimit: rate must be at least 1, got %d", w.Rate)
	}
	if w.Period < time.Millisecond || w.Period%time.Millisecond != 0 {
		return fmt.Errorf("ratelimit: period must be a whole number of milliseconds, at least 1ms, got %v", w.Period)
	}
	if c.burst < 0 {
		return fmt.Errorf("ratelimit: burst must not be negative, got %d", c.burst)
	}

	switch c.algorithm {
	case SlidingWindow:
		if w.Period > maxDuration/2 {
			return fmt.Errorf("ratelimit: period %v is too long for SlidingWindow, which keeps state for twice the period", w.Period)
		}
	case TokenBucket, LeakyBucket:
		if w.Period/time.Duration(w.Rate) < time.Microsecond {
			return fmt.Errorf("ratelimit: %d per %v gives less than 1µs per token; token and leaky buckets allow at most 1000000 per second", w.Rate, w.Period)
		}
		if burst := c.bucketBurst(); burst > int64(maxDuration/2/w.EmissionInterval()) {
			return fmt.Errorf("ratelimit: a burst of %d at %v per token takes more than about 146 years to refill", burst, w.EmissionInterval())
		}
	}
	return nil
}

func (c config) bucketBurst() int64 {
	if c.burst == 0 {
		return c.window.Rate
	}
	return c.burst
}

func (c config) storeOrDefault() Store {
	if c.store != nil {
		return c.store
	}
	return NewMemoryStore()
}
