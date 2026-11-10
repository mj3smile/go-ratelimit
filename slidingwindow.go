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
//	estimate = floor(prev*weight) + curr
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

// retryAfter returns how long until a request of cost n would fit.
//
// As the current bucket goes on, the previous bucket's requests fade out of
// the count, one every timePerRequest. room is how many of them can still be
// counted while this request fits. The count is rounded down, so a request
// that has started to fade no longer counts: once mustFade = count-(room+1)
// have faded completely, the request fits a moment later.
//
// If the current bucket has room, prev is the bucket fading, and it started
// when the current bucket did, elapsed ago. If curr+n alone is over Rate,
// nothing fits until the bucket rotates: wait untilRotation, then curr becomes
// the previous bucket and fades the same way.
func (sw *slidingWindow) retryAfter(curr, prev int64, elapsed time.Duration, n int64) time.Duration {
	// +1µs because the request is still denied at exactly mustFade*timePerRequest,
	// another +1µs for floating-point rounding
	const extraWait = 2 * time.Microsecond

	period, rate := sw.window.Period, sw.window.Rate

	if room := rate - curr - n; room >= 0 {
		mustFade := float64(prev - (room + 1))
		timePerRequest := float64(period) / float64(prev)
		return max(time.Duration(mustFade*timePerRequest)+extraWait-elapsed, 0)
	}

	untilRotation := period - elapsed
	room := rate - n
	mustFade := float64(curr - (room + 1)) // request that must fade
	timePerRequest := float64(period) / float64(curr)
	return untilRotation + time.Duration(mustFade*timePerRequest) + extraWait
}
