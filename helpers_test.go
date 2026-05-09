package ratelimit

import "context"

type errLimiter struct{ err error }

func (e errLimiter) Allow(context.Context, string) (Result, error)         { return Result{}, e.err }
func (e errLimiter) AllowN(context.Context, string, int64) (Result, error) { return Result{}, e.err }
func (e errLimiter) Reset(context.Context, string) error                   { return e.err }

func newErrLimiter(err error) Limiter { return errLimiter{err: err} }
