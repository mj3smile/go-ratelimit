package ratelimit

import (
	"net"
	"net/http"

	"github.com/mj3smile/go-ratelimit/internal/clientip"
)

// UnixSocket is the WithTrustedProxies source
// for a proxy that connects over a Unix domain socket.
const UnixSocket = clientip.UnixSocket

// IPKeyOption configures IPKeyFunc.
type IPKeyOption func(*ipKeyConfig)

type ipKeyConfig struct {
	header   string
	resolver clientip.Resolver
}

func newIPKeyConfig() ipKeyConfig {
	return ipKeyConfig{resolver: clientip.Resolver{IPv6Prefix: clientip.DefaultIPv6Prefix}}
}

// WithTrustedProxies reads the client IP from header for requests that come
// from a trusted proxy. Each source is an IP address such as "127.0.0.1", a
// CIDR prefix such as "10.0.0.0/8", or UnixSocket for a proxy that connects
// over a Unix domain socket. Requests from anywhere else are keyed by
// RemoteAddr.
//
// The header is read from right to left, skipping trusted addresses, and the
// first untrusted address is the client. This covers single-address headers
// such as X-Real-IP or CF-Connecting-IP as well as X-Forwarded-For, where
// proxies append to whatever the client sent. If every entry is trusted, the
// leftmost one is used; if the header is missing or holds an entry that is not
// an IP address, the request is keyed by RemoteAddr.
//
// WithTrustedProxies panics if header is empty, no source is given, or a
// source is invalid.
func WithTrustedProxies(header string, sources ...string) IPKeyOption {
	if header == "" {
		panic("ratelimit: WithTrustedProxies requires a header name")
	}
	proxies, err := clientip.ParseProxies(sources)
	if err != nil {
		panic("ratelimit: WithTrustedProxies: " + err.Error())
	}
	header = http.CanonicalHeaderKey(header)
	return func(c *ipKeyConfig) {
		c.header = header
		c.resolver.Proxies = proxies
	}
}

// WithIPv6Prefix sets how many leading bits of an IPv6 address identify one
// client. The default is 64: a single host usually controls a whole /64 and
// could otherwise rotate addresses to get a fresh limit. ISPs often delegate a
// /56 or /48 per customer; a shorter prefix groups those. IPv4 addresses are
// always keyed in full.
//
// WithIPv6Prefix panics if bits is outside 1-128.
func WithIPv6Prefix(bits int) IPKeyOption {
	if err := clientip.CheckIPv6Prefix(bits); err != nil {
		panic("ratelimit: WithIPv6Prefix: " + err.Error())
	}
	return func(c *ipKeyConfig) {
		c.resolver.IPv6Prefix = bits
	}
}

// IPKeyFunc returns a KeyFunc that keys requests by client IP. With no options
// it behaves like DefaultKeyFunc.
func IPKeyFunc(opts ...IPKeyOption) KeyFunc {
	cfg := newIPKeyConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg.key
}

// DefaultKeyFunc keys requests by the client IP from RemoteAddr, grouping IPv6
// clients by /64. Headers such as X-Forwarded-For are ignored because any
// client can set them; behind a reverse proxy RemoteAddr is the proxy's
// address, so use IPKeyFunc with WithTrustedProxies. Requests without an IP
// address in RemoteAddr, such as those arriving over a Unix domain socket,
// are keyed by RemoteAddr as-is.
func DefaultKeyFunc(r *http.Request) string {
	return newIPKeyConfig().key(r)
}

func (c ipKeyConfig) key(r *http.Request) string {
	return c.resolver.Key(r.RemoteAddr,
		func() bool {
			_, ok := r.Context().Value(http.LocalAddrContextKey).(*net.UnixAddr)
			return ok
		},
		func() []string { return r.Header[c.header] },
	)
}
