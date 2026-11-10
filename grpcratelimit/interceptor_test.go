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

func TestUnaryServerInterceptorN_CostAboveLimit(t *testing.T) {
	limiter := mustNew(t, ratelimit.WithRate(10, time.Minute))
	interceptor := UnaryServerInterceptorN(limiter, func(context.Context, *grpc.UnaryServerInfo) int64 { return 11 })

	ctx := trailerCtx()
	_, err := interceptor(ctx, "req", testInfo, testHandler)
	if code := status.Code(err); code != codes.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted", code)
	}
	if got := getTrailers(ctx).Get("retry-after"); len(got) != 0 {
		t.Errorf("retry-after = %v, want none: the RPC can never be allowed", got)
	}
}

// A nil argument must stop the server from starting. Called later, a nil
// function panics inside an RPC, and gRPC doesn't recover that panic, so the
// whole server process would crash.
func TestInterceptors_NilArgumentsPanicAtSetup(t *testing.T) {
	limiter := mustNew(t, ratelimit.WithRate(5, time.Minute))
	cost := func(context.Context, *grpc.UnaryServerInfo) int64 { return 1 }
	cases := []struct {
		name  string
		setup func()
	}{
		{"WithKeyFunc(nil)", func() { WithKeyFunc(nil) }},
		{"WithErrorHandler(nil)", func() { WithErrorHandler(nil) }},
		{"nil option", func() { UnaryServerInterceptor(limiter, nil) }},
		{"UnaryServerInterceptor without a limiter", func() { UnaryServerInterceptor(nil) }},
		{"UnaryServerInterceptorN without a limiter", func() { UnaryServerInterceptorN(nil, cost) }},
		{"UnaryServerInterceptorN without a cost function", func() { UnaryServerInterceptorN(limiter, nil) }},
		{"StreamServerInterceptor without a limiter", func() { StreamServerInterceptor(nil) }},
		{"StreamServerInterceptorN without a limiter", func() { StreamServerInterceptorN(nil, cost) }},
		{"StreamServerInterceptorN without a cost function", func() { StreamServerInterceptorN(limiter, nil) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic at setup")
				}
			}()
			tc.setup()
		})
	}
}

// When the RPC's context has already ended, the limiter error comes from the
// client giving up, so the RPC ends with the matching code and the error
// handler isn't asked to let it through.
func TestInterceptors_EndedRPCReportsWhy(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()

	for _, tc := range []struct {
		name string
		ctx  context.Context
		want codes.Code
	}{
		{"canceled", canceled, codes.Canceled},
		{"deadline passed", expired, codes.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handlerCalls := 0
			failOpen := WithErrorHandler(func(context.Context, *grpc.UnaryServerInfo, error) error {
				handlerCalls++
				return nil
			})
			limiter := newErrLimiter(tc.ctx.Err())
			cost := func(context.Context, *grpc.UnaryServerInfo) int64 { return 1 }
			stream := &fakeServerStream{ctx: tc.ctx}

			_, unaryErr := UnaryServerInterceptor(limiter, failOpen)(tc.ctx, "req", testInfo, testHandler)
			_, unaryNErr := UnaryServerInterceptorN(limiter, cost, failOpen)(tc.ctx, "req", testInfo, testHandler)
			streamErr := StreamServerInterceptor(limiter, failOpen)(nil, stream, testStreamInfo, testStreamHandler)
			streamNErr := StreamServerInterceptorN(limiter, cost, failOpen)(nil, stream, testStreamInfo, testStreamHandler)

			for name, err := range map[string]error{"unary": unaryErr, "unary N": unaryNErr, "stream": streamErr, "stream N": streamNErr} {
				if code := status.Code(err); code != tc.want {
					t.Errorf("%s: code = %v, want %v", name, code, tc.want)
				}
			}
			if handlerCalls != 0 {
				t.Errorf("the error handler ran %d times for RPCs whose client had already given up", handlerCalls)
			}
		})
	}
}
