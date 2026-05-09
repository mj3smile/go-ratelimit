package ratelimit

import (
	"encoding/json"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// KeyFunc extracts a rate limit key from an HTTP request.
type KeyFunc func(r *http.Request) string

// ErrorHandler is called when the rate limiter returns an error. Use this to
// control fail-open vs fail-closed behavior. The next parameter is the
// downstream handler — call it to fail open (let the request through).
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
// request. The default extracts the client IP from X-Forwarded-For (first
// entry) or RemoteAddr.
func WithKeyFunc(fn KeyFunc) MiddlewareOption {
	return func(c *middlewareConfig) {
		c.keyFunc = fn
	}
}

// WithDeniedHandler sets a custom handler invoked when a request is denied.
// The default responds with 429, a Retry-After header, and a JSON body.
func WithDeniedHandler(h http.Handler) MiddlewareOption {
	return func(c *middlewareConfig) {
		c.deniedHandler = h
	}
}

// WithErrorHandler sets the function called when the rate limiter returns an
// error. The default is FailClosed (500). Use FailOpen to let requests
// through on error, or provide a custom handler for logging/metrics.
func WithErrorHandler(fn ErrorHandler) MiddlewareOption {
	return func(c *middlewareConfig) {
		c.errorHandler = fn
	}
}

// HTTPMiddleware returns an http.Handler middleware that rate-limits requests
// using the given Limiter. Every response includes X-RateLimit-Limit and
// X-RateLimit-Remaining headers. Denied responses additionally include a
// Retry-After header and are handled by the denied handler (default: 429
// with a JSON body).
func HTTPMiddleware(limiter Limiter, opts ...MiddlewareOption) func(http.Handler) http.Handler {
	cfg := applyDefaults(opts)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := cfg.keyFunc(r)
			result, err := limiter.Allow(r.Context(), key)
			if err != nil {
				cfg.errorHandler(next, w, r, err)
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

// DefaultKeyFunc extracts the client IP from X-Forwarded-For (first entry) or
// falls back to the IP portion of RemoteAddr.
func DefaultKeyFunc(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if ip, _, ok := strings.Cut(xff, ","); ok {
			return strings.TrimSpace(ip)
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func defaultDeniedHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	json.NewEncoder(w).Encode(map[string]string{
		"error":       "rate limit exceeded",
		"retry_after": w.Header().Get("Retry-After") + "s",
	})
}

// CostFunc determines the cost of an HTTP request for weighted rate limiting.
type CostFunc func(r *http.Request) int64

// HTTPMiddlewareN returns a middleware that uses AllowN with a per-request
// cost. This is useful when different endpoints have different weights.
func HTTPMiddlewareN(limiter Limiter, costFn CostFunc, opts ...MiddlewareOption) func(http.Handler) http.Handler {
	cfg := applyDefaults(opts)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := cfg.keyFunc(r)
			n := costFn(r)
			result, err := limiter.AllowN(r.Context(), key, n)
			if err != nil {
				cfg.errorHandler(next, w, r, err)
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

func applyDefaults(opts []MiddlewareOption) middlewareConfig {
	cfg := middlewareConfig{
		keyFunc:       DefaultKeyFunc,
		deniedHandler: http.HandlerFunc(defaultDeniedHandler),
		errorHandler:  FailClosed,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

func writeRateLimitHeaders(w http.ResponseWriter, result Result) {
	w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(result.Limit, 10))
	w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(result.Remaining, 10))
	if !result.Allowed {
		retryAfterSec := math.Ceil(result.RetryAfter.Seconds())
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfterSec)))
	}
}
