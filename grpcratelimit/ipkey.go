package grpcratelimit

import (
	"context"
	"net"
	"strings"

	"github.com/mj3smile/go-ratelimit/internal/clientip"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// UnixSocket is the WithTrustedProxies source
// for a proxy that connects over a Unix domain socket.
const UnixSocket = clientip.UnixSocket

// IPKeyOption configures IPKeyFunc.
type IPKeyOption func(*ipKeyConfig)

type ipKeyConfig struct {
	metadataKey string
	resolver    clientip.Resolver
}

func newIPKeyConfig() ipKeyConfig {
	return ipKeyConfig{resolver: clientip.Resolver{IPv6Prefix: clientip.DefaultIPv6Prefix}}
}

// WithTrustedProxies reads the client IP from the metadata key for RPCs whose
// peer is a trusted proxy. Proxies forward it in HTTP/2 headers, which gRPC
// exposes as metadata, so the key is usually "x-forwarded-for". Each source is
// an IP address such as "127.0.0.1", a CIDR prefix such as "10.0.0.0/8", or
// UnixSocket for a proxy that connects over a Unix domain socket. RPCs from
// any other peer are keyed by the peer address.
//
// The metadata values are read from right to left, skipping trusted
// addresses, and the first untrusted address is the client. If every entry is
// trusted, the leftmost one is used; if the key is missing or holds an entry
// that is not an IP address, the RPC is keyed by the peer address.
//
// WithTrustedProxies panics if key is empty, no source is given, or a source
// is invalid.
func WithTrustedProxies(key string, sources ...string) IPKeyOption {
	if key == "" {
		panic("grpcratelimit: WithTrustedProxies requires a metadata key")
	}
	proxies, err := clientip.ParseProxies(sources)
	if err != nil {
		panic("grpcratelimit: WithTrustedProxies: " + err.Error())
	}
	key = strings.ToLower(key)
	return func(c *ipKeyConfig) {
		c.metadataKey = key
		c.resolver.Proxies = proxies
	}
}

// WithIPv6Prefix sets how many leading bits of an IPv6 address identify one
// client. The default is 64: a single host usually controls a whole /64 and
// could otherwise rotate addresses to get a fresh limit. IPv4 addresses are
// always keyed in full.
//
// WithIPv6Prefix panics if bits is outside 1-128.
func WithIPv6Prefix(bits int) IPKeyOption {
	if err := clientip.CheckIPv6Prefix(bits); err != nil {
		panic("grpcratelimit: WithIPv6Prefix: " + err.Error())
	}
	return func(c *ipKeyConfig) {
		c.resolver.IPv6Prefix = bits
	}
}

// IPKeyFunc returns a KeyFunc that keys RPCs by client IP. With no options it
// behaves like DefaultKeyFunc.
func IPKeyFunc(opts ...IPKeyOption) KeyFunc {
	cfg := newIPKeyConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return func(ctx context.Context, _ *grpc.UnaryServerInfo) string {
		return cfg.key(ctx)
	}
}

// DefaultKeyFunc keys RPCs by the peer's IP address without the port, so every
// connection from the same host shares one limit, and groups IPv6 peers by
// /64. Peers without an IP address, such as Unix socket clients, are keyed by
// their full address.
func DefaultKeyFunc(ctx context.Context, _ *grpc.UnaryServerInfo) string {
	return newIPKeyConfig().key(ctx)
}

func (c ipKeyConfig) key(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "unknown"
	}
	return c.resolver.Key(p.Addr.String(),
		func() bool {
			_, ok := p.Addr.(*net.UnixAddr)
			return ok
		},
		func() []string { return metadata.ValueFromIncomingContext(ctx, c.metadataKey) },
	)
}
