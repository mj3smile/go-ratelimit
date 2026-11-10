package ratelimit

import (
	"context"
	"time"
)

// slidingWindow implements the Limiter interface using a 2-bucket sliding
// window counter. It keeps the count of the current and previous buckets of
// length Window.Period and computes a weighted estimate of how many requests
// happened in the trailing Period:
//
//	weight   = (Period - elapsed) / Period
//	estimate = prev*weight + curr
//
// This smooths out the "burst at the window boundary" problem of fixed window
// at the cost of a small approximation: it assumes requests in the previous
// bucket were evenly distributed across that bucket.
type slidingWindow struct {
	window   Window
	store    Store
	onResult OnResultFunc
}

func newSlidingWindow(window Window, store Store, onResult OnResultFunc) *slidingWindow {
	return &slidingWindow{
		window:   window,
		store:    store,
		onResult: onResult,
	}
}

func (sw *slidingWindow) Allow(ctx context.Context, key string) (Result, error) {
	return sw.AllowN(ctx, key, 1)
}

func (sw *slidingWindow) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	return allowN(ctx, key, n, sw.window.Rate, sw.check, sw.onResult)
}

func (sw *slidingWindow) check(ctx context.Context, key string, n int64) (Result, error) {
	curr, prev, elapsed, applied, err := sw.store.CheckAndIncrementSliding(ctx, key, sw.window, n)
	if err != nil {
		return Result{}, err
	}

	weight := float64(sw.window.Period-elapsed) / float64(sw.window.Period)
	if weight < 0 {
		weight = 0
	}
	estimate := int64(float64(prev)*weight) + curr

	remaining := sw.window.Rate - estimate
	if remaining < 0 {
		remaining = 0
	}

	result := Result{
		Allowed:   applied,
		Remaining: remaining,
		Limit:     sw.window.Rate,
	}
	if !applied {
		result.RetryAfter = sw.retryAfter(curr, prev, elapsed, n)
	}
	return result, nil
}

func (sw *slidingWindow) Reset(ctx context.Context, key string) error {
	return sw.store.ResetSliding(ctx, key)
}

// retryAfter approximates how long until a request of cost n would fit.
//
// The weighted estimate drops as elapsed grows: prev*(Period-elapsed-dt)/Period.
// Solving prev*(Period-elapsed-dt)/Period + curr + n <= Rate yields:
//
//	dt = Period - elapsed - (Rate - n - curr)*Period/prev
//
// When prev is zero or curr+n already exceeds Rate, the weighted decay alone
// can't help and the caller must at least wait for the current bucket to rotate.
func (sw *slidingWindow) retryAfter(curr, prev int64, elapsed time.Duration, n int64) time.Duration {
	untilRotation := sw.window.Period - elapsed
	if prev <= 0 {
		return untilRotation
	}
	budget := sw.window.Rate - n - curr
	if budget < 0 {
		return untilRotation
	}
	dt := sw.window.Period - elapsed - time.Duration(float64(budget)*float64(sw.window.Period)/float64(prev))
	if dt < 0 {
		dt = 0
	}
	if dt > untilRotation {
		dt = untilRotation
	}
	return dt
}
