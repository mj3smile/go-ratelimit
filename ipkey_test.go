package ratelimit

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDefaultKeyFunc(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		want       string
	}{
		{
			name:       "remote addr with port",
			remoteAddr: "192.168.1.1:8080",
			want:       "192.168.1.1",
		},
		{
			name:       "remote addr without port",
			remoteAddr: "192.168.1.1",
			want:       "192.168.1.1",
		},
		{
			name:       "x-forwarded-for ignored",
			remoteAddr: "10.0.0.1:1234",
			xff:        "203.0.113.50",
			want:       "10.0.0.1",
		},
		{
			name:       "ipv6 grouped by /64",
			remoteAddr: "[2001:db8:1:2::abcd]:8080",
			want:       "2001:db8:1:2::/64",
		},
		{
			name:       "ipv4-mapped ipv6 keyed as ipv4",
			remoteAddr: "[::ffff:203.0.113.7]:8080",
			want:       "203.0.113.7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			got := DefaultKeyFunc(req)
			if got != tt.want {
				t.Errorf("DefaultKeyFunc = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIPKeyFunc_TrustedProxies(t *testing.T) {
	keyFn := IPKeyFunc(WithTrustedProxies("X-Forwarded-For", "10.0.0.0/8"))

	tests := []struct {
		name       string
		remoteAddr string
		xff        []string
		want       string
	}{
		{
			name:       "untrusted peer ignores header",
			remoteAddr: "198.51.100.1:1234",
			xff:        []string{"203.0.113.7"},
			want:       "198.51.100.1",
		},
		{
			name:       "entries left of the client are ignored",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"192.0.2.99, 203.0.113.7"},
			want:       "203.0.113.7",
		},
		{
			name:       "trusted hops are skipped",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"203.0.113.7, 10.0.0.2"},
			want:       "203.0.113.7",
		},
		{
			name:       "all entries trusted uses leftmost",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"10.0.0.3, 10.0.0.2"},
			want:       "10.0.0.3",
		},
		{
			name:       "invalid entry falls back to peer",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"203.0.113.7, not-an-ip"},
			want:       "10.0.0.1",
		},
		{
			name:       "missing header falls back to peer",
			remoteAddr: "10.0.0.1:1234",
			want:       "10.0.0.1",
		},
		{
			name:       "repeated headers are read as one list",
			remoteAddr: "10.0.0.1:1234",
			xff:        []string{"192.0.2.99", "203.0.113.7"},
			want:       "203.0.113.7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			for _, v := range tt.xff {
				req.Header.Add("X-Forwarded-For", v)
			}
			if got := keyFn(req); got != tt.want {
				t.Errorf("key = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIPKeyFunc_UnixSocketProxy(t *testing.T) {
	tests := []struct {
		name         string
		sources      []string
		unixListener bool
		want         string
	}{
		{
			name:         "trusted unix socket reads header",
			sources:      []string{"unix:"},
			unixListener: true,
			want:         "203.0.113.7",
		},
		{
			name:         "unix socket needs explicit trust",
			sources:      []string{"10.0.0.0/8"},
			unixListener: true,
			want:         "@",
		},
		{
			name:         "non-ip remote addr alone is not a unix socket",
			sources:      []string{"unix:"},
			unixListener: false,
			want:         "@",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = "@"
			req.Header.Set("X-Forwarded-For", "203.0.113.7")
			if tt.unixListener {
				local := &net.UnixAddr{Name: "/run/app.sock", Net: "unix"}
				req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, local))
			}
			keyFn := IPKeyFunc(WithTrustedProxies("X-Forwarded-For", tt.sources...))
			if got := keyFn(req); got != tt.want {
				t.Errorf("key = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIPKeyFunc_TrustedSingleAddress(t *testing.T) {
	keyFn := IPKeyFunc(WithTrustedProxies("X-Forwarded-For", "10.0.0.1"))

	tests := []struct {
		name       string
		remoteAddr string
		want       string
	}{
		{
			name:       "listed address reads header",
			remoteAddr: "10.0.0.1:1234",
			want:       "203.0.113.7",
		},
		{
			name:       "neighbouring address ignores header",
			remoteAddr: "10.0.0.2:1234",
			want:       "10.0.0.2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			req.Header.Set("X-Forwarded-For", "203.0.113.7")
			if got := keyFn(req); got != tt.want {
				t.Errorf("key = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIPKeyFunc_IPv6Prefix(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "[2001:db8:1:2ff::1]:1234"

	got := IPKeyFunc(WithIPv6Prefix(56))(req)
	if want := "2001:db8:1:200::/56"; got != want {
		t.Errorf("key = %q, want %q", got, want)
	}
}

func TestIPKeyOptions_InvalidPanics(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
	}{
		{"empty header", func() { WithTrustedProxies("", "10.0.0.0/8") }},
		{"no trusted sources", func() { WithTrustedProxies("X-Forwarded-For") }},
		{"hostname instead of address", func() { WithTrustedProxies("X-Forwarded-For", "proxy.internal") }},
		{"ipv6 prefix zero", func() { WithIPv6Prefix(0) }},
		{"ipv6 prefix above 128", func() { WithIPv6Prefix(129) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic for %s", tt.name)
				}
			}()
			tt.fn()
		})
	}
}
