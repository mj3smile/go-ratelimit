package redisstore_test

import (
	"context"
	"fmt"
	"log"

	ratelimit "github.com/mj3smile/go-ratelimit"
	"github.com/mj3smile/go-ratelimit/redisstore"
	"github.com/redis/go-redis/v9"
)

func Example() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	limiter, err := ratelimit.New(
		ratelimit.WithAlgorithm(ratelimit.SlidingWindow),
		ratelimit.PerMinute(100),
		ratelimit.WithStore(redisstore.New(client, redisstore.WithPrefix("myapp:"))),
	)
	if err != nil {
		log.Fatal(err)
	}

	result, err := limiter.Allow(context.Background(), "user:123")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Allowed)
}
