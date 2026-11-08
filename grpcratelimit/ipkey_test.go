package grpcratelimit

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func withPeer(addr net.Addr) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{Addr: addr})
}

func TestDefaultKeyFunc(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{
			name: "tcp ipv4 drops port",
			ctx:  withPeer(&net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 50051}),
			want: "203.0.113.7",
		},
		{
			name: "tcp ipv6 grouped by /64",
			ctx:  withPeer(&net.TCPAddr{IP: net.ParseIP("2001:db8:1:2::abcd"), Port: 50051}),
			want: "2001:db8:1:2::/64",
		},
		{
			name: "unix socket",
			ctx:  withPeer(&net.UnixAddr{Name: "@", Net: "unix"}),
			want: "@",
		},
		{
			name: "peer without address",
			ctx:  withPeer(nil),
			want: "unknown",
		},
		{
			name: "no peer",
			ctx:  context.Background(),
			want: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DefaultKeyFunc(tt.ctx, testInfo); got != tt.want {
				t.Errorf("DefaultKeyFunc = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIPKeyFunc_TrustedProxies(t *testing.T) {
	tcp := func(ip string) net.Addr {
		return &net.TCPAddr{IP: net.ParseIP(ip), Port: 50051}
	}
	unix := &net.UnixAddr{Name: "@", Net: "unix"}

	tests := []struct {
		name    string
		sources []string
		peer    net.Addr
		xff     string
		want    string
	}{
		{
			name:    "trusted peer reads metadata",
			sources: []string{"10.0.0.0/8"},
			peer:    tcp("10.0.0.1"),
			xff:     "192.0.2.99, 203.0.113.7",
			want:    "203.0.113.7",
		},
		{
			name:    "untrusted peer ignores metadata",
			sources: []string{"10.0.0.0/8"},
			peer:    tcp("198.51.100.1"),
			xff:     "203.0.113.7",
			want:    "198.51.100.1",
		},
		{
			name:    "missing metadata falls back to peer",
			sources: []string{"10.0.0.0/8"},
			peer:    tcp("10.0.0.1"),
			want:    "10.0.0.1",
		},
		{
			name:    "trusted unix socket reads metadata",
			sources: []string{"unix:"},
			peer:    unix,
			xff:     "203.0.113.7",
			want:    "203.0.113.7",
		},
		{
			name:    "unix socket needs explicit trust",
			sources: []string{"10.0.0.0/8"},
			peer:    unix,
			xff:     "203.0.113.7",
			want:    "@",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := withPeer(tt.peer)
			if tt.xff != "" {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-forwarded-for", tt.xff))
			}
			keyFn := IPKeyFunc(WithTrustedProxies("X-Forwarded-For", tt.sources...))
			if got := keyFn(ctx, testInfo); got != tt.want {
				t.Errorf("key = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIPKeyFunc_IPv6Prefix(t *testing.T) {
	ctx := withPeer(&net.TCPAddr{IP: net.ParseIP("2001:db8:1:2ff::1"), Port: 50051})

	got := IPKeyFunc(WithIPv6Prefix(56))(ctx, testInfo)
	if want := "2001:db8:1:200::/56"; got != want {
		t.Errorf("key = %q, want %q", got, want)
	}
}

func TestIPKeyOptions_InvalidPanics(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
	}{
		{"empty metadata key", func() { WithTrustedProxies("", "10.0.0.0/8") }},
		{"hostname instead of address", func() { WithTrustedProxies("x-forwarded-for", "proxy.internal") }},
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
