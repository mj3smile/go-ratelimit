package ratelimit

import (
	"context"
	"time"
)

// tokenBucket implements the Limiter interface using a classic token-bucket
// algorithm. The bucket holds up to `capacity` tokens and refills at
// window.Rate tokens per window.Period. Each request consumes n tokens; if
// fewer than n are available, the request is denied and no tokens are
// consumed.
//
// State is held in the Store and updated atomically. The store uses a
// "lazy advance" approach: lastRefill is only moved forward by integer
// multiples of the emission interval (Period / Rate), so fractional refill
// credit accumulates between calls instead of being lost to integer rounding.
type tokenBucket struct {
	window   Window
	capacity int64
	store    Store
	onResult OnResultFunc
}

func newTokenBucket(window Window, capacity int64, store Store, onResult OnResultFunc) *tokenBucket {
	return &tokenBucket{
		window:   window,
		capacity: capacity,
		store:    store,
		onResult: onResult,
	}
}

func (tb *tokenBucket) Allow(ctx context.Context, key string) (Result, error) {
	return tb.AllowN(ctx, key, 1)
}

func (tb *tokenBucket) AllowN(ctx context.Context, key string, n int64) (Result, error) {
	tokens, sinceLastRefill, applied, err := tb.store.CheckAndConsumeTokens(ctx, key, tb.capacity, tb.window, n)
	if err != nil {
		return Result{}, err
	}

	result := Result{
		Allowed:   applied,
		Remaining: tokens,
		Limit:     tb.capacity,
	}
	if !applied {
		result.RetryAfter = tb.retryAfter(tokens, sinceLastRefill, n)
	}

	if tb.onResult != nil {
		tb.onResult(ctx, key, result)
	}
	return result, nil
}

func (tb *tokenBucket) Reset(ctx context.Context, key string) error {
	return tb.store.ResetTokens(ctx, key)
}

// retryAfter computes how long until n tokens become available given the
// current token count and the time elapsed since the last credited refill.
//
// Each token takes T = Period/Rate to refill. We need (n - tokens) more
// tokens, but already have sinceLastRefill toward the next one:
//
//	retry = (n - tokens) * T - sinceLastRefill
func (tb *tokenBucket) retryAfter(tokens int64, sinceLastRefill time.Duration, n int64) time.Duration {
	deficit := n - tokens
	if deficit <= 0 {
		return 0
	}
	emissionInterval := tb.window.Period / time.Duration(tb.window.Rate)
	retry := time.Duration(deficit)*emissionInterval - sinceLastRefill
	if retry < 0 {
		retry = 0
	}
	return retry
}
