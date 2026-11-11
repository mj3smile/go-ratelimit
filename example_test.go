package ratelimit_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	ratelimit "github.com/mj3smile/go-ratelimit"
)

func ExampleNew() {
	limiter, err := ratelimit.New(ratelimit.PerMinute(2))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for range 3 {
		result, err := limiter.Allow(ctx, "user:123")
		if err != nil {
			log.Fatal(err)
		}
		if result.Allowed {
			fmt.Println("allowed,", result.Remaining, "left")
		} else {
			fmt.Println("denied, retry after", result.RetryAfter.Round(time.Second))
		}
	}
	// Output:
	// allowed, 1 left
	// allowed, 0 left
	// denied, retry after 1m0s
}

func ExampleHTTPMiddleware() {
	limiter, err := ratelimit.New(ratelimit.PerMinute(1))
	if err != nil {
		log.Fatal(err)
	}
	handler := ratelimit.HTTPMiddleware(limiter)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "hello")
	}))

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/", nil))
	fmt.Println(first.Code, first.Body.String())

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/", nil))
	fmt.Println(second.Code, "Retry-After:", second.Header().Get("Retry-After"))
	// Output:
	// 200 hello
	// 429 Retry-After: 60
}

// Behind a reverse proxy, RemoteAddr is the proxy's address. Trusting the
// proxy's X-Forwarded-For header gives each client its own limit.
func ExampleIPKeyFunc() {
	limiter, err := ratelimit.New(ratelimit.PerMinute(1))
	if err != nil {
		log.Fatal(err)
	}
	keyFunc := ratelimit.IPKeyFunc(ratelimit.WithTrustedProxies("X-Forwarded-For", "10.0.0.0/8"))
	handler := ratelimit.HTTPMiddleware(limiter, ratelimit.WithKeyFunc(keyFunc))(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
	)

	for _, client := range []string{"203.0.113.7", "203.0.113.7", "198.51.100.4"} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = "10.0.0.5:4321" // the proxy
		r.Header.Set("X-Forwarded-For", client)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		fmt.Println(client, w.Code)
	}
	// Output:
	// 203.0.113.7 200
	// 203.0.113.7 429
	// 198.51.100.4 200
}
