package grpcratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/mj3smile/go-ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnaryServerInterceptor_Allow(t *testing.T) {
	limiter := mustNew(t, ratelimit.WithRate(5, time.Minute))
	interceptor := UnaryServerInterceptor(limiter)

	ctx := trailerCtx()
	resp, err := interceptor(ctx, "req", testInfo, testHandler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Errorf("resp = %v, want %q", resp, "ok")
	}

	trailers := getTrailers(ctx)
	if got := trailers.Get("x-ratelimit-limit"); len(got) == 0 || got[0] != "5" {
		t.Errorf("x-ratelimit-limit = %v, want [5]", got)
	}
	if got := trailers.Get("x-ratelimit-remaining"); len(got) == 0 || got[0] != "4" {
		t.Errorf("x-ratelimit-remaining = %v, want [4]", got)
	}
}

func TestUnaryServerInterceptor_Deny(t *testing.T) {
	limiter := mustNew(t, ratelimit.WithRate(1, time.Minute))
	interceptor := UnaryServerInterceptor(limiter)

	ctx := trailerCtx()
	interceptor(ctx, "req", testInfo, testHandler) // consume

	ctx = trailerCtx()
	_, err := interceptor(ctx, "req", testInfo, testHandler)
	if err == nil {
		t.Fatal("expected error on denied RPC")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted", st.Code())
	}

	trailers := getTrailers(ctx)
	if got := trailers.Get("retry-after"); len(got) == 0 {
		t.Error("missing retry-after trailer on denial")
	}
}

func TestUnaryServerInterceptor_CustomKeyFunc(t *testing.T) {
	limiter := mustNew(t, ratelimit.WithRate(1, time.Minute))
	interceptor := UnaryServerInterceptor(limiter, WithKeyFunc(func(_ context.Context, info *grpc.UnaryServerInfo) string {
		return info.FullMethod
	}))

	ctx := trailerCtx()
	_, err := interceptor(ctx, "req", testInfo, testHandler)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Same method key: denied.
	ctx = trailerCtx()
	_, err = interceptor(ctx, "req", testInfo, testHandler)
	if err == nil {
		t.Fatal("second call with same method key should be denied")
	}

	// Different method: allowed (independent key).
	otherInfo := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Other"}
	ctx = trailerCtx()
	_, err = interceptor(ctx, "req", otherInfo, testHandler)
	if err != nil {
		t.Fatalf("different method should be allowed: %v", err)
	}
}

func TestUnaryServerInterceptor_FailClosed(t *testing.T) {
	limiter := newErrLimiter(errors.New("redis: connection refused"))
	interceptor := UnaryServerInterceptor(limiter)

	ctx := trailerCtx()
	_, err := interceptor(ctx, "req", testInfo, testHandler)
	if err == nil {
		t.Fatal("expected error with FailClosed")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", st.Code())
	}
}

func TestUnaryServerInterceptor_FailOpen(t *testing.T) {
	limiter := newErrLimiter(errors.New("redis: connection refused"))
	interceptor := UnaryServerInterceptor(limiter, WithErrorHandler(FailOpen))

	ctx := trailerCtx()
	resp, err := interceptor(ctx, "req", testInfo, testHandler)
	if err != nil {
		t.Fatalf("FailOpen should not error: %v", err)
	}
	if resp != "ok" {
		t.Errorf("resp = %v, want %q", resp, "ok")
	}
}

func TestUnaryServerInterceptor_CustomErrorHandler(t *testing.T) {
	var gotErr error
	limiter := newErrLimiter(errors.New("redis: connection refused"))
	interceptor := UnaryServerInterceptor(limiter, WithErrorHandler(func(_ context.Context, _ *grpc.UnaryServerInfo, err error) error {
		gotErr = err
		return nil
	}))

	ctx := trailerCtx()
	resp, err := interceptor(ctx, "req", testInfo, testHandler)
	if err != nil {
		t.Fatalf("custom handler (fail open): %v", err)
	}
	if resp != "ok" {
		t.Errorf("resp = %v, want %q", resp, "ok")
	}
	if gotErr == nil || gotErr.Error() != "redis: connection refused" {
		t.Errorf("custom handler err = %v, want redis error", gotErr)
	}
}

func TestStreamServerInterceptor_Allow(t *testing.T) {
	limiter := mustNew(t, ratelimit.WithRate(5, time.Minute))
	interceptor := StreamServerInterceptor(limiter)

	ctx := trailerCtx()
	ss := &fakeServerStream{ctx: ctx}
	err := interceptor(nil, ss, testStreamInfo, testStreamHandler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	trailers := getTrailers(ctx)
	if got := trailers.Get("x-ratelimit-limit"); len(got) == 0 || got[0] != "5" {
		t.Errorf("x-ratelimit-limit = %v, want [5]", got)
	}
	if got := trailers.Get("x-ratelimit-remaining"); len(got) == 0 || got[0] != "4" {
		t.Errorf("x-ratelimit-remaining = %v, want [4]", got)
	}
}

func TestStreamServerInterceptor_Deny(t *testing.T) {
	limiter := mustNew(t, ratelimit.WithRate(1, time.Minute))
	interceptor := StreamServerInterceptor(limiter)

	ctx := trailerCtx()
	ss := &fakeServerStream{ctx: ctx}
	interceptor(nil, ss, testStreamInfo, testStreamHandler) // consume

	ctx = trailerCtx()
	ss = &fakeServerStream{ctx: ctx}
	err := interceptor(nil, ss, testStreamInfo, testStreamHandler)
	if err == nil {
		t.Fatal("expected error on denied stream")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted", st.Code())
	}

	trailers := getTrailers(ctx)
	if got := trailers.Get("retry-after"); len(got) == 0 {
		t.Error("missing retry-after trailer on denial")
	}
}

func TestStreamServerInterceptorN(t *testing.T) {
	limiter := mustNew(t, ratelimit.WithRate(10, time.Minute))
	costFn := func(_ context.Context, info *grpc.UnaryServerInfo) int64 {
		if info.FullMethod == "/test.Service/HeavyStream" {
			return 5
		}
		return 1
	}
	interceptor := StreamServerInterceptorN(limiter, costFn)

	heavyStreamInfo := &grpc.StreamServerInfo{FullMethod: "/test.Service/HeavyStream"}

	for i := 0; i < 2; i++ {
		ctx := trailerCtx()
		ss := &fakeServerStream{ctx: ctx}
		err := interceptor(nil, ss, heavyStreamInfo, testStreamHandler)
		if err != nil {
			t.Fatalf("heavy call %d: %v", i+1, err)
		}
	}

	ctx := trailerCtx()
	ss := &fakeServerStream{ctx: ctx}
	err := interceptor(nil, ss, testStreamInfo, testStreamHandler)
	if err == nil {
		t.Fatal("light after exhaustion should be denied")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted", st.Code())
	}
}

func TestUnaryServerInterceptorN(t *testing.T) {
	limiter := mustNew(t, ratelimit.WithRate(10, time.Minute))
	costFn := func(_ context.Context, info *grpc.UnaryServerInfo) int64 {
		if info.FullMethod == "/test.Service/Heavy" {
			return 5
		}
		return 1
	}
	interceptor := UnaryServerInterceptorN(limiter, costFn)

	heavyInfo := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Heavy"}

	// Two heavy calls (cost 5 each, total 10): both allowed.
	for i := 0; i < 2; i++ {
		ctx := trailerCtx()
		_, err := interceptor(ctx, "req", heavyInfo, testHandler)
		if err != nil {
			t.Fatalf("heavy call %d: %v", i+1, err)
		}
	}

	// Light call (cost 1, total would be 11): denied.
	ctx := trailerCtx()
	_, err := interceptor(ctx, "req", testInfo, testHandler)
	if err == nil {
		t.Fatal("light after exhaustion should be denied")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted", st.Code())
	}
}
