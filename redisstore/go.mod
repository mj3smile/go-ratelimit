module github.com/mj3smile/go-ratelimit/redisstore

go 1.25.1

require (
	github.com/mj3smile/go-ratelimit v0.0.0
	github.com/redis/go-redis/v9 v9.9.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)

replace github.com/mj3smile/go-ratelimit => ../
