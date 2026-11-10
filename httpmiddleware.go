package ratelimit

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
)

// KeyFunc extracts a rate limit key from an HTTP request.
type KeyFunc func(r *http.Request) string

// ErrorHandler is called when the rate limiter returns an error. Use this to
// control fail-open vs fail-closed behavior. The next parameter is the
// downstream handler — call it to fail open (let the request through). It
// isn't called when the request's context has already ended: the middleware
// then answers 499 if the client went away, or 503 if the request's deadline
// passed.
type ErrorHandler func(next http.Handler, w http.ResponseWriter, r *http.Request, err error)

// FailClosed returns 500 Internal Server Error when the rate limiter errors.
// This is the default — it makes errors visible during development and is the
// safe choice when correctness matters more than availability.
func FailClosed(_ http.Handler, w http.ResponseWriter, _ *http.Request, _ error) {
	http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}

// FailOpen calls the next handler as if the request was allowed when the rate
// limiter errors. Use this in production when availability matters more than
// rate-limit enforcement — e.g. when the store is a remote Redis that can
// have transient failures.
func FailOpen(next http.Handler, w http.ResponseWriter, r *http.Request, _ error) {
	next.ServeHTTP(w, r)
}

// MiddlewareOption configures the HTTP middleware.
type MiddlewareOption func(*middlewareConfig)

type middlewareConfig struct {
	keyFunc       KeyFunc
	deniedHandler http.Handler
	errorHandler  ErrorHandler
}

// WithKeyFunc sets the function used to extract a rate limit key from each
// request. The default is DefaultKeyFunc. It panics if fn is nil.
func WithKeyFunc(fn KeyFunc) MiddlewareOption {
	if fn == nil {
		panic("ratelimit: WithKeyFunc requires a function")
	}
	return func(c *middlewareConfig) {
		c.keyFunc = fn
	}
}

// WithDeniedHandler sets a custom handler invoked when a request is denied.
// The rate-limit headers, including Retry-After when there is one, are already
// set when it runs. The default responds with 429 and a JSON body. It panics
// if h is nil.
func WithDeniedHandler(h http.Handler) MiddlewareOption {
	if h == nil {
		panic("ratelimit: WithDeniedHandler requires a handler")
	}
	return func(c *middlewareConfig) {
		c.deniedHandler = h
	}
}

// WithErrorHandler sets the function called when the rate limiter returns an
// error. The default is FailClosed (500). Use FailOpen to let requests
// through on error, or provide a custom handler for logging/metrics. It
// panics if fn is nil.
func WithErrorHandler(fn ErrorHandler) MiddlewareOption {
	if fn == nil {
		panic("ratelimit: WithErrorHandler requires a function")
	}
	return func(c *middlewareConfig) {
		c.errorHandler = fn
	}
}

// HTTPMiddleware returns an http.Handler middleware that rate-limits requests
// using the given Limiter. Every response includes X-RateLimit-Limit and
// X-RateLimit-Remaining headers. Denied responses additionally include a
// Retry-After header and are handled by the denied handler (default: 429
// with a JSON body). It panics if limiter is nil.
func HTTPMiddleware(limiter Limiter, opts ...MiddlewareOption) func(http.Handler) http.Handler {
	cfg := applyDefaults(limiter, opts)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := cfg.keyFunc(r)
			result, err := limiter.Allow(r.Context(), key)
			if err != nil {
				cfg.limiterFailed(next, w, r, err)
				return
			}

			writeRateLimitHeaders(w, result)
			if !result.Allowed {
				cfg.deniedHandler.ServeHTTP(w, r)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func defaultDeniedHandler(w http.ResponseWriter, _ *http.Request) {
	body := map[string]string{"error": "rate limit exceeded"}
	if retryAfter := w.Header().Get("Retry-After"); retryAfter != "" {
		body["retry_after"] = retryAfter + "s"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	json.NewEncoder(w).Encode(body)
}

// CostFunc determines the cost of an HTTP request for weighted rate limiting.
type CostFunc func(r *http.Request) int64

// HTTPMiddlewareN returns a middleware that uses AllowN with a per-request
// cost. This is useful when different endpoints have different weights. A
// request whose cost is larger than the limit is denied without a Retry-After
// header, since it can never be allowed; a negative cost is passed to the
// error handler as an error wrapping ErrInvalidCost. It panics if limiter or
// costFn is nil.
func HTTPMiddlewareN(limiter Limiter, costFn CostFunc, opts ...MiddlewareOption) func(http.Handler) http.Handler {
	if costFn == nil {
		panic("ratelimit: the middleware requires a cost function")
	}
	cfg := applyDefaults(limiter, opts)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := cfg.keyFunc(r)
			n := costFn(r)
			result, err := limiter.AllowN(r.Context(), key, n)
			if err != nil {
				cfg.limiterFailed(next, w, r, err)
				return
			}

			writeRateLimitHeaders(w, result)
			if !result.Allowed {
				cfg.deniedHandler.ServeHTTP(w, r)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func applyDefaults(limiter Limiter, opts []MiddlewareOption) middlewareConfig {
	if limiter == nil {
		panic("ratelimit: the middleware requires a limiter")
	}
	cfg := middlewareConfig{
		keyFunc:       DefaultKeyFunc,
		deniedHandler: http.HandlerFunc(defaultDeniedHandler),
		errorHandler:  FailClosed,
	}
	for _, opt := range opts {
		if opt == nil {
			panic("ratelimit: middleware options must not be nil")
		}
		opt(&cfg)
	}
	return cfg
}

// statusClientClosedRequest is nginx's non-standard 499, for a request whose
// client went away before it was answered.
const statusClientClosedRequest = 499

// limiterFailed answers a request whose limiter check failed. If the request
// has already ended, it answers why (499 when the client went away, 503 when
// the deadline passed); otherwise the error handler decides.
func (c middlewareConfig) limiterFailed(next http.Handler, w http.ResponseWriter, r *http.Request, err error) {
	ctxErr := r.Context().Err()
	switch {
	case ctxErr == nil:
		c.errorHandler(next, w, r, err)
	case errors.Is(ctxErr, context.DeadlineExceeded):
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
	default:
		w.WriteHeader(statusClientClosedRequest)
	}
}

func writeRateLimitHeaders(w http.ResponseWriter, result Result) {
	w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(result.Limit, 10))
	w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(result.Remaining, 10))
	if !result.Allowed && result.RetryAfter != InfDuration {
		retryAfterSec := math.Ceil(result.RetryAfter.Seconds())
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfterSec)))
	}
}
