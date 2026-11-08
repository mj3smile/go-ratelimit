// Package clientip finds the client IP address of a request that may have
// passed through trusted reverse proxies and turns it into a rate limit key.
package clientip

import (
	"errors"
	"fmt"
	"net/netip"
	"strings"
)

// DefaultIPv6Prefix is the number of leading bits that identify one IPv6
// client unless configured otherwise.
const DefaultIPv6Prefix = 64

// UnixSocket is the trusted proxy source for a peer connected over a Unix
// domain socket, spelled like nginx's "set_real_ip_from unix:".
const UnixSocket = "unix:"

// CheckIPv6Prefix reports an error if bits is not a usable IPv6 prefix length.
func CheckIPv6Prefix(bits int) error {
	if bits < 1 || bits > 128 {
		return fmt.Errorf("IPv6 prefix length %d out of range 1-128", bits)
	}
	return nil
}

// Proxies is a set of trusted proxy sources.
type Proxies struct {
	prefixes []netip.Prefix
	unix     bool
}

// ParseProxies parses trusted proxy sources, each an IP address, a CIDR prefix
// or UnixSocket. An IP address trusts that address alone.
func ParseProxies(sources []string) (Proxies, error) {
	if len(sources) == 0 {
		return Proxies{}, errors.New("at least one trusted proxy source is required")
	}
	var p Proxies
	for _, s := range sources {
		if s == UnixSocket {
			p.unix = true
			continue
		}
		prefix, ok := parsePrefix(s)
		if !ok {
			return Proxies{}, fmt.Errorf("invalid trusted proxy source %q: want an IP address, a CIDR prefix or %q", s, UnixSocket)
		}
		p.prefixes = append(p.prefixes, prefix)
	}
	return p, nil
}

// Resolver turns a connection's peer address, and the forwarding header of
// connections from trusted proxies, into a rate limit key.
type Resolver struct {
	IPv6Prefix int
	Proxies    Proxies
}

// Key returns the rate limit key for a connection from peer. overUnix reports
// whether the connection arrived over a Unix domain socket, and forwarded
// returns the values of the forwarding header; each is called only when
// needed.
//
// A peer that is a trusted proxy is keyed by the client address in the
// forwarding header. Any other peer, or a trusted one whose header is missing
// or invalid, is keyed by its own address.
func (r Resolver) Key(peer string, overUnix func() bool, forwarded func() []string) string {
	host, ip, isIP := parseHost(peer)
	var trusted bool
	if isIP {
		trusted = r.Proxies.trustsIP(ip)
	} else {
		trusted = r.Proxies.unix && overUnix()
	}
	if trusted {
		if client, ok := r.Proxies.client(forwarded()); ok {
			return ipKey(client, r.IPv6Prefix)
		}
	}
	if !isIP {
		return peer
	}
	if ip.Is4() {
		return host
	}
	return ipKey(ip, r.IPv6Prefix)
}

// trustsIP reports whether a peer at ip is a trusted proxy.
func (p Proxies) trustsIP(ip netip.Addr) bool {
	ip = ip.Unmap().WithZone("")
	for _, prefix := range p.prefixes {
		if prefix.Contains(ip) {
			return true
		}
	}
	return false
}

// client returns the client address from the values of a forwarding header.
// Entries are read from right to left across all values, skipping trusted
// proxies, and the first untrusted address is the client. If every entry is
// trusted, the leftmost is returned. ok is false if there are no entries or
// one of them is not an IP address.
func (p Proxies) client(values []string) (netip.Addr, bool) {
	var leftmost netip.Addr
	for i := len(values) - 1; i >= 0; i-- {
		rest := values[i]
		for rest != "" {
			entry := rest
			rest = ""
			if j := strings.LastIndexByte(entry, ','); j >= 0 {
				entry, rest = entry[j+1:], entry[:j]
			}
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			_, ip, ok := parseHost(entry)
			if !ok {
				return netip.Addr{}, false
			}
			if !p.trustsIP(ip) {
				return ip, true
			}
			leftmost = ip
		}
	}
	return leftmost, leftmost.IsValid()
}

// parsePrefix parses s as a CIDR prefix, or as an IP address that stands for
// a prefix containing only that address.
func parsePrefix(s string) (netip.Prefix, bool) {
	if strings.Contains(s, "/") {
		prefix, err := netip.ParsePrefix(s)
		return prefix.Masked(), err == nil
	}
	ip, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Prefix{}, false
	}
	ip = ip.Unmap().WithZone("")
	return netip.PrefixFrom(ip, ip.BitLen()), true
}

// parseHost parses s as an IP address with an optional port, such as
// "203.0.113.7", "203.0.113.7:80", "2001:db8::1" or "[2001:db8::1]:80". It
// also returns the address part of s as written.
func parseHost(s string) (string, netip.Addr, bool) {
	host := s
	if strings.HasPrefix(s, "[") {
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return "", netip.Addr{}, false
		}
		host = s[1:end]
	} else if i := strings.IndexByte(s, ':'); i >= 0 && strings.IndexByte(s[i+1:], ':') < 0 {
		host = s[:i]
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return "", netip.Addr{}, false
	}
	return host, ip, true
}

// ipKey returns the rate limit key for ip: the address itself for IPv4, and
// the enclosing prefix of ipv6Prefix bits for IPv6.
func ipKey(ip netip.Addr, ipv6Prefix int) string {
	ip = ip.Unmap()
	if ip.Is4() {
		return ip.String()
	}
	prefix, _ := ip.Prefix(ipv6Prefix)
	return prefix.String()
}
