package grpcratelimit

import (
	"context"
	"math"
	"strconv"

	ratelimit "github.com/mj3smile/go-ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// KeyFunc extracts a rate limit key from the RPC context and method info.
type KeyFunc func(ctx context.Context, info *grpc.UnaryServerInfo) string

// ErrorHandler controls fail-open vs fail-closed behavior when the rate
// limiter returns an error. Return nil to fail open (allow the RPC), or
// return an error to fail closed. It isn't called when the RPC's context has
// already ended: the client has given up, so the RPC ends with codes.Canceled
// or codes.DeadlineExceeded.
type ErrorHandler func(ctx context.Context, info *grpc.UnaryServerInfo, err error) error

// FailClosed returns a codes.Internal error when the rate limiter fails.
// This is the default.
func FailClosed(_ context.Context, _ *grpc.UnaryServerInfo, _ error) error {
	return status.Error(codes.Internal, "rate limiter error")
}

// FailOpen returns nil when the rate limiter fails, allowing the RPC through.
func FailOpen(_ context.Context, _ *grpc.UnaryServerInfo, _ error) error {
	return nil
}

// Option configures the gRPC interceptor.
type Option func(*config)

type config struct {
	keyFunc      KeyFunc
	errorHandler ErrorHandler
}

// WithKeyFunc sets the function used to extract a rate limit key from each
// RPC. The default is DefaultKeyFunc. It panics if fn is nil.
func WithKeyFunc(fn KeyFunc) Option {
	if fn == nil {
		panic("grpcratelimit: WithKeyFunc requires a function")
	}
	return func(c *config) {
		c.keyFunc = fn
	}
}

// WithErrorHandler sets the function called when the rate limiter returns an
// error. The default is FailClosed. Use FailOpen to allow RPCs through on
// error. It panics if fn is nil.
func WithErrorHandler(fn ErrorHandler) Option {
	if fn == nil {
		panic("grpcratelimit: WithErrorHandler requires a function")
	}
	return func(c *config) {
		c.errorHandler = fn
	}
}

// UnaryServerInterceptor returns a grpc.UnaryServerInterceptor that
// rate-limits incoming unary RPCs using the given Limiter. Every response
// includes x-ratelimit-limit and x-ratelimit-remaining trailers. Denied
// RPCs additionally include a retry-after trailer (seconds) and return
// codes.ResourceExhausted. It panics if limiter is nil.
func UnaryServerInterceptor(limiter ratelimit.Limiter, opts ...Option) grpc.UnaryServerInterceptor {
	cfg := applyDefaults(limiter, opts)

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		key := cfg.keyFunc(ctx, info)
		result, err := limiter.Allow(ctx, key)
		if err != nil {
			if handlerErr := cfg.limiterFailed(ctx, info, err); handlerErr != nil {
				return nil, handlerErr
			}
			return handler(ctx, req)
		}

		setTrailers(ctx, result)

		if !result.Allowed {
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}

		return handler(ctx, req)
	}
}

func applyDefaults(limiter ratelimit.Limiter, opts []Option) config {
	if limiter == nil {
		panic("grpcratelimit: the interceptor requires a limiter")
	}
	cfg := config{
		keyFunc:      DefaultKeyFunc,
		errorHandler: FailClosed,
	}
	for _, opt := range opts {
		if opt == nil {
			panic("grpcratelimit: options must not be nil")
		}
		opt(&cfg)
	}
	return cfg
}

// limiterFailed decides how an RPC ends when the limiter fails: with the
// reason its context ended if the client has already given up, otherwise
// with what the error handler returns (nil lets the RPC through).
func (c config) limiterFailed(ctx context.Context, info *grpc.UnaryServerInfo, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	return c.errorHandler(ctx, info, err)
}

func setTrailers(ctx context.Context, result ratelimit.Result) {
	grpc.SetTrailer(ctx, metadata.Pairs(
		"x-ratelimit-limit", strconv.FormatInt(result.Limit, 10),
		"x-ratelimit-remaining", strconv.FormatInt(result.Remaining, 10),
	))
	if !result.Allowed && result.RetryAfter != ratelimit.InfDuration {
		retryAfterSec := math.Ceil(result.RetryAfter.Seconds())
		grpc.SetTrailer(ctx, metadata.Pairs("retry-after", strconv.Itoa(int(retryAfterSec))))
	}
}

// CostFunc determines the cost of an RPC for weighted rate limiting.
type CostFunc func(ctx context.Context, info *grpc.UnaryServerInfo) int64

// UnaryServerInterceptorN returns a grpc.UnaryServerInterceptor that uses
// AllowN with a per-RPC cost. An RPC whose cost is larger than the limit is
// denied without a retry-after trailer, since it can never be allowed; a
// negative cost is passed to the error handler as an error wrapping
// ratelimit.ErrInvalidCost. It panics if limiter or costFn is nil.
func UnaryServerInterceptorN(limiter ratelimit.Limiter, costFn CostFunc, opts ...Option) grpc.UnaryServerInterceptor {
	if costFn == nil {
		panic("grpcratelimit: the interceptor requires a cost function")
	}
	cfg := applyDefaults(limiter, opts)

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		key := cfg.keyFunc(ctx, info)
		n := costFn(ctx, info)
		result, err := limiter.AllowN(ctx, key, n)
		if err != nil {
			if handlerErr := cfg.limiterFailed(ctx, info, err); handlerErr != nil {
				return nil, handlerErr
			}
			return handler(ctx, req)
		}

		setTrailers(ctx, result)

		if !result.Allowed {
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a grpc.StreamServerInterceptor that
// rate-limits incoming streaming RPCs. The limiter is charged once, when the
// stream opens; messages on an open stream aren't counted. To limit messages,
// call the limiter from the stream handler. It panics if limiter is nil.
func StreamServerInterceptor(limiter ratelimit.Limiter, opts ...Option) grpc.StreamServerInterceptor {
	cfg := applyDefaults(limiter, opts)

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		uInfo := streamToUnaryInfo(srv, info)
		key := cfg.keyFunc(ctx, uInfo)
		result, err := limiter.Allow(ctx, key)
		if err != nil {
			if handlerErr := cfg.limiterFailed(ctx, uInfo, err); handlerErr != nil {
				return handlerErr
			}
			return handler(srv, ss)
		}

		setTrailers(ctx, result)

		if !result.Allowed {
			return status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}

		return handler(srv, ss)
	}
}

// StreamServerInterceptorN returns a grpc.StreamServerInterceptor that uses
// AllowN with a per-RPC cost, with the same handling of costs as
// UnaryServerInterceptorN. Like StreamServerInterceptor, it charges once per
// stream, when the stream opens. It panics if limiter or costFn is nil.
func StreamServerInterceptorN(limiter ratelimit.Limiter, costFn CostFunc, opts ...Option) grpc.StreamServerInterceptor {
	if costFn == nil {
		panic("grpcratelimit: the interceptor requires a cost function")
	}
	cfg := applyDefaults(limiter, opts)

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		uInfo := streamToUnaryInfo(srv, info)
		key := cfg.keyFunc(ctx, uInfo)
		n := costFn(ctx, uInfo)
		result, err := limiter.AllowN(ctx, key, n)
		if err != nil {
			if handlerErr := cfg.limiterFailed(ctx, uInfo, err); handlerErr != nil {
				return handlerErr
			}
			return handler(srv, ss)
		}

		setTrailers(ctx, result)

		if !result.Allowed {
			return status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}

		return handler(srv, ss)
	}
}

func streamToUnaryInfo(srv any, info *grpc.StreamServerInfo) *grpc.UnaryServerInfo {
	return &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: info.FullMethod,
	}
}
