package grpcratelimit

import (
	"context"

	ratelimit "github.com/mj3smile/go-ratelimit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var testInfo = &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}
var testStreamInfo = &grpc.StreamServerInfo{FullMethod: "/test.Service/StreamMethod"}

func testHandler(_ context.Context, _ any) (any, error) { return "ok", nil }

func testStreamHandler(_ any, _ grpc.ServerStream) error { return nil }

type errLimiter struct{ err error }

func (e errLimiter) Allow(context.Context, string) (ratelimit.Result, error) {
	return ratelimit.Result{}, e.err
}
func (e errLimiter) AllowN(context.Context, string, int64) (ratelimit.Result, error) {
	return ratelimit.Result{}, e.err
}
func (e errLimiter) Reset(context.Context, string) error { return e.err }

func newErrLimiter(err error) ratelimit.Limiter { return errLimiter{err: err} }

type fakeTransportStream struct {
	trailers metadata.MD
}

func (f *fakeTransportStream) Method() string               { return "/test.Service/Method" }
func (f *fakeTransportStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeTransportStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeTransportStream) SetTrailer(md metadata.MD) error {
	if f.trailers == nil {
		f.trailers = metadata.MD{}
	}
	for k, v := range md {
		f.trailers[k] = append(f.trailers[k], v...)
	}
	return nil
}

func trailerCtx() context.Context {
	return grpc.NewContextWithServerTransportStream(
		context.Background(),
		&fakeTransportStream{},
	)
}

func getTrailers(ctx context.Context) metadata.MD {
	stream := grpc.ServerTransportStreamFromContext(ctx)
	if s, ok := stream.(*fakeTransportStream); ok {
		return s.trailers
	}
	return nil
}

type fakeServerStream struct {
	ctx context.Context
}

func (f *fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) Context() context.Context     { return f.ctx }
func (f *fakeServerStream) SendMsg(any) error            { return nil }
func (f *fakeServerStream) RecvMsg(any) error            { return nil }