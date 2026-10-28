// Package destination enforces outbound destination grants at dial time.
package destination

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"
)

// ErrDeniedAddress identifies a destination blocked by the grants policy.
var ErrDeniedAddress = errors.New("destination contains a denied address; configure grants for this purpose")

var deniedV4 = mustPrefixes(
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
	"169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24",
	"192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24",
	"203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
)
var deniedV6 = mustPrefixes("::/128", "::1/128", "fc00::/7", "fe80::/10", "ff00::/8", "2001::/23", "2001:db8::/32", "2002::/16")
var publicV6 = netip.MustParsePrefix("2000::/3")

func mustPrefixes(raw ...string) []netip.Prefix {
	result := make([]netip.Prefix, 0, len(raw))
	for _, item := range raw {
		result = append(result, netip.MustParsePrefix(item))
	}
	return result
}

func Public(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() {
		return false
	}
	if ip.Is4() {
		for _, prefix := range deniedV4 {
			if prefix.Contains(ip) {
				return false
			}
		}
		return true
	}
	if !publicV6.Contains(ip) {
		return false
	}
	for _, prefix := range deniedV6 {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}

// Policy grants purpose-specific IP destinations and CIDR subnets.
// Keep separate instances for runners, webhooks and discovery.
type Policy struct {
	Purpose string
	Grants  map[netip.AddrPort]struct{} // Port zero permits any port.
	Lookup  func(context.Context, string) ([]net.IPAddr, error)
	subnets []netip.Prefix
}

// New accepts literal IP addresses, IP:port grants, or CIDR subnets.
// A bare IP or subnet permits any port.
func New(purpose string, grants []string) (Policy, error) {
	p := Policy{Purpose: purpose, Grants: map[netip.AddrPort]struct{}{}}
	for _, raw := range grants {
		if prefix, err := netip.ParsePrefix(raw); err == nil && !prefix.Addr().Is4In6() {
			p.subnets = append(p.subnets, prefix)
			continue
		}
		// A slash denotes CIDR syntax even when ParseAddr accepts it in a zone.
		slash := strings.Contains(raw, "/")
		if ip, err := netip.ParseAddr(raw); err == nil && !slash {
			p.Grants[netip.AddrPortFrom(ip.Unmap(), 0)] = struct{}{}
			continue
		}
		endpoint, err := netip.ParseAddrPort(raw)
		if err != nil || endpoint.Port() == 0 || slash {
			return Policy{}, fmt.Errorf("invalid %s destination grant: expected an IP address, CIDR subnet, or IP:port with port between 1 and 65535", purpose)
		}
		p.Grants[netip.AddrPortFrom(endpoint.Addr().Unmap(), endpoint.Port())] = struct{}{}
	}
	return p, nil
}

func (p Policy) granted(addr string) bool {
	endpoint, err := netip.ParseAddrPort(addr)
	if err != nil {
		return false
	}
	ip := endpoint.Addr().Unmap()
	_, exact := p.Grants[netip.AddrPortFrom(ip, endpoint.Port())]
	_, anyPort := p.Grants[netip.AddrPortFrom(ip, 0)]
	return exact || anyPort || slices.ContainsFunc(p.subnets, func(subnet netip.Prefix) bool { return subnet.Contains(ip) })
}

// ParseURL parses a string at an input boundary and validates the destination.
func ParseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("destination must be an absolute HTTP or HTTPS URL")
	}
	if err := ValidateURL(u); err != nil {
		return nil, err
	}
	return u, nil
}

// ValidateURL checks a required URL, including nil and empty values. Address
// policy, where used, is applied at every connection after DNS resolution.
func ValidateURL(u *url.URL) error {
	if u == nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("destination must be an absolute HTTP or HTTPS URL")
	}
	return nil
}

func (p Policy) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.New("destination has invalid host or port")
	}
	lookup := p.Lookup
	if lookup == nil {
		lookup = net.DefaultResolver.LookupIPAddr
	}
	var ips []netip.Addr
	if literal, err := netip.ParseAddr(host); err == nil {
		ips = []netip.Addr{literal.Unmap()}
	} else {
		resolved, err := lookup(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("%s DNS lookup failed: %w", p.Purpose, err)
		}
		for _, item := range resolved {
			ip, ok := netip.AddrFromSlice(item.IP)
			if ok {
				ips = append(ips, ip.Unmap())
			}
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("%s destination has no addresses", p.Purpose)
	}
	for _, ip := range ips {
		if !Public(ip) && !p.granted(net.JoinHostPort(ip.String(), port)) {
			return nil, fmt.Errorf("%s %w", p.Purpose, ErrDeniedAddress)
		}
	}
	dialer := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	var last error
	for _, ip := range ips {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		last = err
	}
	return nil, fmt.Errorf("%s connection failed: %w", p.Purpose, last)
}

func (p Policy) Transport(headerTimeout time.Duration) *http.Transport {
	return &http.Transport{
		Proxy:                 nil,
		DialContext:           p.DialContext,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		ResponseHeaderTimeout: headerTimeout,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
	}
}

func (p Policy) Client() *http.Client {
	return &http.Client{
		Transport: p.Transport(15 * time.Second),
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("too many redirects")
			}
			req.Header.Del("Authorization")
			req.Header.Del("Proxy-Authorization")
			if req.URL != nil {
				req.URL.User = nil
			}
			return nil
		},
	}
}
