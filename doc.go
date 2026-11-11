// Package ratelimit provides a pluggable rate limiting library for Go: four
// algorithms and two storage backends behind small interfaces, with
// ready-made net/http and gRPC middleware.
//
// # Quick start
//
// Create a [Limiter] with [New], then check each request with [Limiter.Allow],
// using a key for whoever makes it, such as a user ID or client IP:
//
//	limiter, err := ratelimit.New(ratelimit.PerMinute(100))
//	if err != nil {
//		return err
//	}
//
//	result, err := limiter.Allow(ctx, "user:123")
//	if err != nil {
//		return err // the store failed
//	}
//	if !result.Allowed {
//		// reject the request; it may retry after result.RetryAfter
//	}
//
// New returns an error for settings it can't enforce, such as a rate of 0.
//
// # Algorithms
//
// Choose an algorithm with [WithAlgorithm]:
//
//   - [FixedWindow], the default, counts requests in windows that start with
//     a key's first request.
//   - [SlidingWindow] also counts part of the previous window, which smooths
//     out bursts at window edges.
//   - [TokenBucket] allows bursts of up to [WithBurst] requests (by default,
//     the rate) and refills at the rate.
//   - [LeakyBucket] allows the same requests as the token bucket, but stores a
//     single timestamp per key.
//
// Set the rate with [WithRate], [PerSecond], [PerMinute], [PerHour], [PerDay],
// or a string such as "100-M" with [WithRateString].
//
// # Results
//
// Each check returns a [Result]: whether the request is allowed, its Limit,
// how much is left in Remaining, and, when denied, how long to wait in
// RetryAfter. [Limiter.AllowN] charges a cost instead of 1, for requests that
// count more than others. A cost larger than the limit is denied with
// RetryAfter set to [InfDuration], and a negative cost returns
// [ErrInvalidCost].
//
// # Storage
//
// A limiter keeps its state in memory unless you pass a store with
// [WithStore]. Memory only counts requests within one process; to share
// limits between several instances of your app, use the Redis store from
// [github.com/mj3smile/go-ratelimit/redisstore]. Several limiters can share
// one store; [WithStore] explains how their state is kept apart.
//
// # Middleware
//
// [HTTPMiddleware] checks every request and, by default, answers denied ones
// with 429 Too Many Requests. It keys requests by the client IP from
// RemoteAddr. Behind a reverse proxy that is the proxy's address, so all
// clients would share one limit; key by the forwarded client IP instead with
// [IPKeyFunc] and [WithTrustedProxies]:
//
//	mw := ratelimit.HTTPMiddleware(limiter, ratelimit.WithKeyFunc(ratelimit.IPKeyFunc(
//		ratelimit.WithTrustedProxies("X-Forwarded-For", "10.0.0.0/8"),
//	)))
//
// When the store fails, [FailClosed], the default, answers 500 Internal
// Server Error, and [FailOpen] lets the request through; choose one with
// [WithErrorHandler].
//
// For gRPC servers, use [github.com/mj3smile/go-ratelimit/grpcratelimit].
//
// # Metrics
//
// [WithOnResult] calls a function after every check, for metrics or logging.
package ratelimit
