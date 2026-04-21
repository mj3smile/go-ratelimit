package ratelimit

import (
	"context"
	"time"
)

// fixedWindow implements the Limiter interface using the fixed window algorithm.
// It divides time into discrete windows of a fixed duration and counts requests
// within each window.
type fixedWindow struct {
	window   Window
	store    Store
	onResult OnResultFunc
	now      func() time.Time // for testing
}

func newFixedWindow(window Window, store Store, onResult OnResultFunc) *fixedWindow {
	return &fixedWindow{
		window:   window,
		store:    store,
		onResult: onResult,
		now:      time.Now,
	}
}

func (fw *fixedWindow) Allow(ctx context.Context, key string) (Result, error) {
	return fw.AllowN(ctx, key, 1)
}

func (fw *fixedWindow) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	count, applied, err := fw.store.CheckAndIncrement(ctx, key, fw.window, n)
	if err != nil {
		return Result{}, err
	}

	result := Result{
		Allowed:   applied,
		Remaining: fw.window.Rate - count,
		Limit:     fw.window.Rate,
	}
	if !applied {
		result.RetryAfter = fw.retryAfter()
	}

	if fw.onResult != nil {
		fw.onResult(key, result)
	}

	return result, nil
}

func (fw *fixedWindow) Reset(ctx context.Context, key string) error {
	return fw.store.Reset(ctx, key)
}

// retryAfter calculates the time remaining until the current window expires.
func (fw *fixedWindow) retryAfter() time.Duration {
	now := fw.now()
	windowStart := now.Truncate(fw.window.Period)
	windowEnd := windowStart.Add(fw.window.Period)
	return windowEnd.Sub(now)
}
