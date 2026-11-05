package redisstore

import (
	"context"
	"fmt"
	"time"

	ratelimit "github.com/mj3smile/go-ratelimit"
	"github.com/redis/go-redis/v9"
)

var _ ratelimit.Store = (*Store)(nil)

// Store is a Redis-backed Store implementation. Each operation is atomic
// via a Lua script, making it safe for distributed deployments.
type Store struct {
	client redis.Cmdable
	prefix string
}

// Option configures a Store.
type Option func(*Store)

// WithPrefix sets a key prefix for all Redis keys. Useful when sharing a
// Redis instance with other applications.
func WithPrefix(prefix string) Option {
	return func(s *Store) {
		s.prefix = prefix
	}
}

// New creates a new Redis-backed store. The client can be a *redis.Client,
// *redis.ClusterClient, or any type implementing redis.Cmdable.
func New(client redis.Cmdable, opts ...Option) *Store {
	s := &Store{client: client}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Store) redisKey(key, suffix string) string {
	return s.prefix + key + ":" + suffix
}

func (s *Store) CheckAndIncrement(ctx context.Context, key string, window ratelimit.Window, n int64) (int64, bool, error) {
	rk := s.redisKey(key, "fw")
	periodMS := window.Period.Milliseconds()

	res, err := fixedWindowScript.Run(ctx, s.client, []string{rk},
		window.Rate, n, periodMS,
	).Int64Slice()
	if err != nil {
		return 0, false, fmt.Errorf("redisstore: fixed window script: %w", err)
	}

	return res[0], res[1] == 1, nil
}

func (s *Store) CheckAndIncrementSliding(ctx context.Context, key string, window ratelimit.Window, n int64) (int64, int64, time.Duration, bool, error) {
	rk := s.redisKey(key, "sw")
	periodMS := window.Period.Milliseconds()
	nowMS := time.Now().UnixMilli()

	res, err := slidingWindowScript.Run(ctx, s.client, []string{rk},
		window.Rate, n, periodMS, nowMS,
	).Int64Slice()
	if err != nil {
		return 0, 0, 0, false, fmt.Errorf("redisstore: sliding window script: %w", err)
	}

	elapsed := time.Duration(res[2]) * time.Millisecond
	return res[0], res[1], elapsed, res[3] == 1, nil
}

func (s *Store) CheckAndConsumeTokens(ctx context.Context, key string, capacity int64, window ratelimit.Window, n int64) (int64, time.Duration, bool, error) {
	rk := s.redisKey(key, "tb")
	periodMS := window.Period.Milliseconds()
	nowMS := time.Now().UnixMilli()

	res, err := tokenBucketScript.Run(ctx, s.client, []string{rk},
		capacity, window.Rate, n, periodMS, nowMS,
	).Int64Slice()
	if err != nil {
		return 0, 0, false, fmt.Errorf("redisstore: token bucket script: %w", err)
	}

	sinceLastRefill := time.Duration(res[1]) * time.Millisecond
	return res[0], sinceLastRefill, res[2] == 1, nil
}

func (s *Store) CheckAndAdvanceTAT(ctx context.Context, key string, burst int64, window ratelimit.Window, n int64) (time.Duration, bool, error) {
	rk := s.redisKey(key, "gcra")
	periodMS := window.Period.Milliseconds()
	nowMS := time.Now().UnixMilli()

	res, err := gcraScript.Run(ctx, s.client, []string{rk},
		burst, window.Rate, n, periodMS, nowMS,
	).Int64Slice()
	if err != nil {
		return 0, false, fmt.Errorf("redisstore: gcra script: %w", err)
	}

	delay := time.Duration(res[0]) * time.Millisecond
	return delay, res[1] == 1, nil
}

func (s *Store) ResetFixed(ctx context.Context, key string) error {
	return s.reset(ctx, key, "fw")
}

func (s *Store) ResetSliding(ctx context.Context, key string) error {
	return s.reset(ctx, key, "sw")
}

func (s *Store) ResetTokens(ctx context.Context, key string) error {
	return s.reset(ctx, key, "tb")
}

func (s *Store) ResetTAT(ctx context.Context, key string) error {
	return s.reset(ctx, key, "gcra")
}

func (s *Store) reset(ctx context.Context, key, suffix string) error {
	if err := s.client.Del(ctx, s.redisKey(key, suffix)).Err(); err != nil {
		return fmt.Errorf("redisstore: reset: %w", err)
	}
	return nil
}

var fixedWindowScript = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
local period_ms = tonumber(ARGV[3])

local count = tonumber(redis.call('GET', key)) or 0

if count + n > rate then
    return {count, 0}
end

local new_count = redis.call('INCRBY', key, n)
if redis.call('PTTL', key) == -1 then
    redis.call('PEXPIRE', key, period_ms)
end

return {new_count, 1}
`)

var slidingWindowScript = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
local period_ms = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'curr', 'prev', 'curr_start')
local curr = tonumber(data[1]) or 0
local prev = tonumber(data[2]) or 0
local curr_start = tonumber(data[3])

if not curr_start then
    if n > rate then
        return {0, 0, 0, 0}
    end
    redis.call('HSET', key, 'curr', n, 'prev', 0, 'curr_start', now_ms)
    redis.call('PEXPIRE', key, period_ms * 2)
    return {n, 0, 0, 1}
end

local elapsed_since_start = now_ms - curr_start
local periods = math.floor(elapsed_since_start / period_ms)

if periods >= 2 then
    prev = 0
    curr = 0
    curr_start = now_ms
elseif periods == 1 then
    prev = curr
    curr = 0
    curr_start = curr_start + period_ms
end

local elapsed = now_ms - curr_start

local weight = (period_ms - elapsed) / period_ms
if weight < 0 then weight = 0 end
local estimate = math.floor(prev * weight) + curr

if estimate + n > rate then
    redis.call('HSET', key, 'curr', curr, 'prev', prev, 'curr_start', curr_start)
    redis.call('PEXPIRE', key, period_ms * 2)
    return {curr, prev, elapsed, 0}
end

curr = curr + n
redis.call('HSET', key, 'curr', curr, 'prev', prev, 'curr_start', curr_start)
redis.call('PEXPIRE', key, period_ms * 2)
return {curr, prev, elapsed, 1}
`)

var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local period_ms = tonumber(ARGV[4])
local now_ms = tonumber(ARGV[5])

local emission_interval_ms = period_ms / rate

local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if not tokens then
    if n > capacity then
        return {capacity, 0, 0}
    end
    redis.call('HSET', key, 'tokens', capacity - n, 'last_refill', now_ms)
    redis.call('PEXPIRE', key, math.ceil(n * emission_interval_ms))
    return {capacity - n, 0, 1}
end

local elapsed = now_ms - last_refill
local refilled = math.floor(elapsed / emission_interval_ms)
tokens = tokens + refilled
if tokens > capacity then
    tokens = capacity
end
last_refill = last_refill + refilled * emission_interval_ms

local applied = 0
if tokens >= n then
    tokens = tokens - n
    applied = 1
end

local since_last_refill = now_ms - last_refill

redis.call('HSET', key, 'tokens', tokens, 'last_refill', last_refill)
local ttl = (capacity - tokens) * emission_interval_ms - since_last_refill
if ttl < 0 then
    ttl = 0
end
redis.call('PEXPIRE', key, math.ceil(ttl))
return {tokens, since_last_refill, applied}
`)

var gcraScript = redis.NewScript(`
local key = KEYS[1]
local burst = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local period_ms = tonumber(ARGV[4])
local now_ms = tonumber(ARGV[5])

local emission_interval_ms = period_ms / rate
local delay_tolerance_ms = burst * emission_interval_ms

local tat_ms = tonumber(redis.call('GET', key))

local base = now_ms
if tat_ms and tat_ms > now_ms then
    base = tat_ms
end

local new_tat = base + n * emission_interval_ms
local delay = new_tat - now_ms

if delay > delay_tolerance_ms then
    return {delay, 0}
end

redis.call('SET', key, new_tat)
redis.call('PEXPIRE', key, math.ceil(delay))
return {delay, 1}
`)
