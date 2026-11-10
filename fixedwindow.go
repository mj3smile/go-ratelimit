package ratelimit

import "context"

// fixedWindow implements the Limiter interface using the fixed window algorithm.
// It counts requests per key in windows of a fixed duration, each starting with
// the key's first request.
type fixedWindow struct {
	window   Window
	store    Store
	onResult OnResultFunc
}

func newFixedWindow(window Window, store Store, onResult OnResultFunc) *fixedWindow {
	return &fixedWindow{
		window:   window,
		store:    store,
		onResult: onResult,
	}
}

func (fw *fixedWindow) Allow(ctx context.Context, key string) (Result, error) {
	return fw.AllowN(ctx, key, 1)
}

func (fw *fixedWindow) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	return allowN(ctx, key, n, fw.window.Rate, fw.check, fw.onResult)
}

func (fw *fixedWindow) check(ctx context.Context, key string, n int64) (Result, error) {
	count, resetAfter, applied, err := fw.store.CheckAndIncrement(ctx, key, fw.window, n)
	if err != nil {
		return Result{}, err
	}

	result := Result{
		Allowed:   applied,
		Remaining: fw.window.Rate - count,
		Limit:     fw.window.Rate,
	}
	if !applied {
		result.RetryAfter = resetAfter
	}
	return result, nil
}

func (fw *fixedWindow) Reset(ctx context.Context, key string) error {
	return fw.store.ResetFixed(ctx, key)
}
