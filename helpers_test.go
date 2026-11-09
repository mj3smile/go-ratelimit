package ratelimit

import (
	"context"
	"testing"
)

type errLimiter struct{ err error }

func (e errLimiter) Allow(context.Context, string) (Result, error)         { return Result{}, e.err }
func (e errLimiter) AllowN(context.Context, string, int64) (Result, error) { return Result{}, e.err }
func (e errLimiter) Reset(context.Context, string) error                   { return e.err }

func newErrLimiter(err error) Limiter { return errLimiter{err: err} }

func mustNew(t *testing.T, opts ...Option) Limiter {
	t.Helper()
	limiter, err := New(opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return limiter
}
