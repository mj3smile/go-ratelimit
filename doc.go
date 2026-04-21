// Package ratelimit provides a pluggable, production-ready rate limiting library for Go.
//
// It supports multiple algorithms (Fixed Window, Sliding Window, Token Bucket)
// and multiple storage backends (in-memory, Redis) behind clean interfaces.
//
// Basic usage:
//
//	limiter := ratelimit.New(
//	    ratelimit.WithRate(100, time.Minute),
//	)
//
//	result, err := limiter.Allow(ctx, "user:123")
//	if !result.Allowed {
//	    // rate limited — retry after result.RetryAfter
//	}
//
// With Redis backend:
//
//	store := redis.NewStore(redisClient)
//	limiter := ratelimit.New(
//	    ratelimit.WithAlgorithm(ratelimit.SlidingWindow),
//	    ratelimit.WithRate(100, time.Minute),
//	    ratelimit.WithStore(store),
//	)
//
// As net/http middleware:
//
//	import ratelimithttp "github.com/mj3smile/go-ratelimit/middleware/http"
//
//	mux.Handle("/api/", ratelimithttp.Middleware(limiter)(apiHandler))
//
// As gRPC unary interceptor:
//
//	import ratelimitgrpc "github.com/mj3smile/go-ratelimit/middleware/grpc"
//
//	server := grpc.NewServer(
//	    grpc.UnaryInterceptor(ratelimitgrpc.UnaryServerInterceptor(limiter)),
//	)
package ratelimit
