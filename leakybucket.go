package ratelimit

import (
	"context"
	"time"
)

// leakyBucket implements the Limiter interface using GCRA (Generic Cell Rate
// Algorithm). It maintains a single TAT (Theoretical Arrival Time) per key.
// Each request advances the TAT by n * emissionInterval; if the resulting TAT
// would exceed now + delayTolerance (burst * emissionInterval), the request is
// denied and the TAT is unchanged.
//
// Behaviorally equivalent to tokenBucket with the same parameters, but with
// single-field state — one timestamp instead of token count + lastRefill.
type leakyBucket struct {
	window    Window
	burst     int64
	emission  time.Duration
	tolerance time.Duration
	store     Store
	onResult  OnResultFunc
}

func newLeakyBucket(window Window, burst int64, store Store, onResult OnResultFunc) *leakyBucket {
	emission := window.EmissionInterval()
	return &leakyBucket{
		window:    window,
		burst:     burst,
		emission:  emission,
		tolerance: time.Duration(burst) * emission,
		store:     store,
		onResult:  onResult,
	}
}

func (lb *leakyBucket) Allow(ctx context.Context, key string) (Result, error) {
	return lb.AllowN(ctx, key, 1)
}

func (lb *leakyBucket) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	return allowN(ctx, key, n, lb.burst, lb.check, lb.onResult)
}

func (lb *leakyBucket) check(ctx context.Context, key string, n int64) (Result, error) {
	delay, applied, err := lb.store.CheckAndAdvanceTAT(ctx, key, lb.burst, lb.window, n)
	if err != nil {
		return Result{}, err
	}

	result := Result{
		Allowed: applied,
		Limit:   lb.burst,
	}
	if applied {
		result.Remaining = int64((lb.tolerance - delay) / lb.emission)
	} else {
		result.RetryAfter = delay - lb.tolerance
		if result.RetryAfter < 0 {
			result.RetryAfter = 0
		}
	}
	return result, nil
}

func (lb *leakyBucket) Reset(ctx context.Context, key string) error {
	return lb.store.ResetTAT(ctx, key)
}
