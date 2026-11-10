package ratelimit

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
}

func TestHTTPMiddleware_AllowAndHeaders(t *testing.T) {
	limiter := mustNew(t, WithRate(5, time.Minute))
	mw := HTTPMiddleware(limiter)
	handler := mw(newTestHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "5" {
		t.Errorf("X-RateLimit-Limit = %q, want %q", got, "5")
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "4" {
		t.Errorf("X-RateLimit-Remaining = %q, want %q", got, "4")
	}
}

func TestHTTPMiddleware_Deny(t *testing.T) {
	limiter := mustNew(t, WithRate(1, time.Minute))
	mw := HTTPMiddleware(limiter)
	handler := mw(newTestHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:1234"

	// First request: allowed.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first request: status = %d, want 200", rec.Code)
	}

	// Second request: denied.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("second request: status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got == "" || got == "0" {
		t.Errorf("Retry-After = %q, want non-zero", got)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode JSON body: %v", err)
	}
	if body["error"] != "rate limit exceeded" {
		t.Errorf("body error = %q, want %q", body["error"], "rate limit exceeded")
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
}

func TestHTTPMiddlewareN_CostAboveLimit(t *testing.T) {
	limiter := mustNew(t, WithRate(10, time.Minute))
	handler := HTTPMiddlewareN(limiter, func(*http.Request) int64 { return 11 })(newTestHandler())

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Errorf("Retry-After = %q, want none: the request can never be allowed", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "10" {
		t.Errorf("X-RateLimit-Remaining = %q, want %q", got, "10")
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode JSON body: %v", err)
	}
	if got, ok := body["retry_after"]; ok {
		t.Errorf("body retry_after = %q, want none", got)
	}
}

func TestHTTPMiddleware_CustomDeniedHandler(t *testing.T) {
	limiter := mustNew(t, WithRate(1, time.Minute))
	custom := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("custom"))
	})
	mw := HTTPMiddleware(limiter, WithDeniedHandler(custom))
	handler := mw(newTestHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:1234"

	handler.ServeHTTP(httptest.NewRecorder(), req) // consume
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if rec.Body.String() != "custom" {
		t.Errorf("body = %q, want %q", rec.Body.String(), "custom")
	}
}

func TestHTTPMiddleware_CustomKeyFunc(t *testing.T) {
	limiter := mustNew(t, WithRate(1, time.Minute))
	mw := HTTPMiddleware(limiter, WithKeyFunc(func(r *http.Request) string {
		return r.Header.Get("X-API-Key")
	}))
	handler := mw(newTestHandler())

	// Two requests from different API keys: both allowed.
	for _, key := range []string{"key-a", "key-b"} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-API-Key", key)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("key %s: status = %d, want 200", key, rec.Code)
		}
	}

	// Second request for key-a: denied.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "key-a")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("key-a second request: status = %d, want 429", rec.Code)
	}
}

func TestHTTPMiddlewareN(t *testing.T) {
	limiter := mustNew(t, WithRate(10, time.Minute))
	costFn := func(r *http.Request) int64 {
		if r.URL.Path == "/heavy" {
			return 5
		}
		return 1
	}
	mw := HTTPMiddlewareN(limiter, costFn)
	handler := mw(newTestHandler())

	// /heavy costs 5, should work first time (remaining=5).
	req := httptest.NewRequest(http.MethodGet, "/heavy", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/heavy first: status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "5" {
		t.Errorf("X-RateLimit-Remaining = %q, want %q", got, "5")
	}

	// Second /heavy costs another 5 (total=10), allowed.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/heavy second: status = %d, want 200", rec.Code)
	}

	// /light costs 1 but quota is exhausted: denied.
	req = httptest.NewRequest(http.MethodGet, "/light", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("/light after exhaustion: status = %d, want 429", rec.Code)
	}
}

func TestHTTPMiddleware_FailClosed(t *testing.T) {
	limiter := newErrLimiter(errors.New("redis: connection refused"))
	mw := HTTPMiddleware(limiter)
	handler := mw(newTestHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("FailClosed: status = %d, want 500", rec.Code)
	}
}

func TestHTTPMiddleware_FailOpen(t *testing.T) {
	limiter := newErrLimiter(errors.New("redis: connection refused"))
	mw := HTTPMiddleware(limiter, WithErrorHandler(FailOpen))
	handler := mw(newTestHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("FailOpen: status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("FailOpen: body = %q, want %q", rec.Body.String(), "ok")
	}
}

func TestHTTPMiddleware_CustomErrorHandler(t *testing.T) {
	var gotErr error
	limiter := newErrLimiter(errors.New("redis: connection refused"))
	mw := HTTPMiddleware(limiter, WithErrorHandler(func(next http.Handler, w http.ResponseWriter, r *http.Request, err error) {
		gotErr = err
		next.ServeHTTP(w, r)
	}))
	handler := mw(newTestHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("custom error handler: status = %d, want 200", rec.Code)
	}
	if gotErr == nil || gotErr.Error() != "redis: connection refused" {
		t.Errorf("custom error handler: err = %v, want redis error", gotErr)
	}
}
