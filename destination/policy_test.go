package destination

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublic(t *testing.T) {
	p, err := New("runner", nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // An enforcement regression must not open outbound connections.
	for _, raw := range []string{"127.0.0.1", "10.1.1.1", "169.254.169.254", "100.64.1.1", "192.168.1.1", "192.88.99.1", "::1", "fc00::1", "fe80::1", "2001:db8::1", "2002::1", "::ffff:127.0.0.1"} {
		require.False(t, Public(netip.MustParseAddr(raw)), raw)
		_, err := p.DialContext(ctx, "tcp", net.JoinHostPort(raw, "443"))
		require.ErrorContains(t, err, "denied address", raw)
	}
	for _, raw := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		require.True(t, Public(netip.MustParseAddr(raw)), raw)
	}
}

func TestRedirectChecksDestinationAndDropsAuthorization(t *testing.T) {
	headers := make(chan http.Header, 1)
	var connections atomic.Int32
	var requests atomic.Int32
	final := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	final.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	final.Start()
	defer final.Close()
	credentialURL := strings.Replace(final.URL, "http://", "http://user:password@", 1)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		target := credentialURL
		if r.URL.Path == "/loop" {
			target = r.URL.String()
		}
		http.Redirect(w, r, target, http.StatusFound)
	}))
	defer redirect.Close()
	policy, err := New("runner", []string{strings.TrimPrefix(redirect.URL, "http://")})
	require.NoError(t, err)
	request, err := http.NewRequest(http.MethodGet, redirect.URL, nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "private-token")
	request.Header.Set("Proxy-Authorization", "private-proxy-token")
	client := policy.Client()
	client.Timeout = time.Second
	defer client.CloseIdleConnections()
	_, err = client.Do(request)
	require.ErrorContains(t, err, "denied address")
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"} {
		t.Setenv(name, final.URL)
	}
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	for _, address := range []string{"http://10.1.2.3", "https://10.1.2.3"} {
		_, err = client.Get(address)
		require.ErrorContains(t, err, "denied address")
	}
	require.Zero(t, connections.Load(), "redirect and proxy targets must remain unconnected")
	policy, err = New("runner", []string{strings.TrimPrefix(redirect.URL, "http://"), strings.TrimPrefix(final.URL, "http://")})
	require.NoError(t, err)
	client = policy.Client()
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
	received := <-headers
	require.Empty(t, received.Get("Authorization"))
	require.Empty(t, received.Get("Proxy-Authorization"))
	require.Nil(t, response.Request.URL.User)
	requests.Store(0)
	response, err = client.Get(redirect.URL + "/loop")
	require.ErrorContains(t, err, "too many redirects")
	if response != nil {
		_ = response.Body.Close()
	}
	require.Equal(t, int32(10), requests.Load())
}

func TestHTTPSRejectsUntrustedCertificate(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("untrusted certificate must fail before sending a request")
	}))
	defer server.Close()
	policy, err := New("runner", []string{"127.0.0.1"})
	require.NoError(t, err)
	client := policy.Client()
	defer client.CloseIdleConnections()
	_, err = client.Get(server.URL)
	var certificateError *tls.CertificateVerificationError
	require.ErrorAs(t, err, &certificateError)
}

func TestInvalidGrants(t *testing.T) {
	for _, raw := range []string{"", "localhost", "localhost:443", "127.0.0.1:", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:abc", "[::1]", "[::1]:0", "[::1]:65536", "127.0.0.*", "127.0.0.1,::1", "http://127.0.0.1:9000", "https://user:password@127.0.0.1", "127.0.0.1/path",
		"10.0.0.0/33", "::1/129", "10.0.0.0/24:9000", "[::1/128]:9000", "fe80::%en0/64", "[fe80::1%en0/64]:9000",
		"::ffff:192.168.1.0/24", "::ffff:192.168.1.0/120"} {
		_, err := New("discovery", []string{raw})
		require.ErrorContains(t, err, "expected an IP address, CIDR subnet, or IP:port", raw)
		require.NotContains(t, err.Error(), "password")
	}
}

func TestGrants(t *testing.T) {
	p, err := New("runner", []string{
		"127.0.0.1", "127.0.0.2:09000", "::ffff:127.0.0.3", "[::ffff:127.0.0.4]:9000",
		"0:0:0:0:0:0:0:1", "[::2]:9000", "fe80::1%en0", "[fe80::2%en0]:9000",
		"192.168.0.42/24", "fc00::/7",
	})
	require.NoError(t, err)
	p.Lookup = func(_ context.Context, host string) ([]net.IPAddr, error) {
		ip := map[string]string{
			"localhost": "127.0.0.1", "exact.local": "127.0.0.2",
			"subnet.local": "192.168.0.1", "ipv6.local": "fd00::1",
		}[host]
		return []net.IPAddr{{IP: net.ParseIP(ip)}}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Check enforcement without opening outbound connections.
	for address, allowed := range map[string]bool{
		"127.0.0.1:9000": true, "127.0.0.1:9001": true, "[::ffff:127.0.0.1]:9000": true,
		"127.0.0.2:9000": true, "127.0.0.2:9001": false, "[::ffff:127.0.0.2]:9000": true,
		"127.0.0.3:9000": true, "127.0.0.4:9000": true, "127.0.0.4:9001": false,
		"[::1]:9000": true, "[::1]:9001": true, "[::2]:9000": true, "[::2]:9001": false,
		"[fe80::1%en0]:9000": true, "[fe80::1%en1]:9000": false, "[fe80::1]:9000": false,
		"[fe80::2%en0]:9000": true, "[fe80::2%en1]:9000": false, "[fe80::2%en0]:9001": false,
		"8.8.8.8:443": true, "127.0.0.5:9000": false, "localhost:9000": true, "localhost:9001": true,
		"exact.local:9000": true, "exact.local:9001": false, "subnet.local:9000": true, "ipv6.local:9000": true,
		"192.168.0.1:9000": true, "192.168.0.1:9001": true, "192.168.1.1:9000": false,
		"[::ffff:192.168.0.255]:9000": true, "[fd00::1]:9000": true, "[fe00::1]:9000": false, "[fd00::1%en0]:9000": false,
	} {
		_, err := p.DialContext(ctx, "tcp", address)
		if allowed {
			require.ErrorIs(t, err, context.Canceled, address)
		} else {
			require.ErrorContains(t, err, "denied address", address)
		}
	}
}

func TestDNSAnswersRespectGrants(t *testing.T) {
	p, err := New("runner", []string{"127.0.0.1", "fc00::/7"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, ips := range [][]net.IPAddr{
		{{IP: net.ParseIP("127.0.0.2")}}, {{IP: net.ParseIP("fe80::1")}},
		{{IP: net.ParseIP("8.8.8.8")}, {IP: net.ParseIP("10.1.1.1")}},
		{{IP: net.ParseIP("127.0.0.1")}, {IP: net.ParseIP("10.1.1.1")}},
		{{IP: net.ParseIP("fc00::1")}, {IP: net.ParseIP("fe80::1")}},
	} {
		p.Lookup = func(context.Context, string) ([]net.IPAddr, error) { return ips, nil }
		_, err := p.DialContext(ctx, "tcp", "orch.example:443")
		require.ErrorContains(t, err, "denied address")
	}
}

func TestDialContextRechecksDNSOnEachConnection(t *testing.T) {
	p, err := New("signer-discovery", []string{"172.16.0.0/12"})
	require.NoError(t, err)
	lookups := 0
	p.Lookup = func(context.Context, string) ([]net.IPAddr, error) {
		lookups++
		if lookups == 1 {
			return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}, {IP: net.ParseIP("172.21.0.3")}}, nil
		}
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Permit allowed addresses through policy without connecting to them.
	_, err = p.DialContext(ctx, "tcp", "orch.example:443")
	require.ErrorIs(t, err, context.Canceled)
	_, err = p.DialContext(ctx, "tcp", "orch.example:443")
	require.ErrorContains(t, err, "denied address")
	require.Equal(t, 2, lookups)
}

func TestValidateRequiredURL(t *testing.T) {
	for _, endpoint := range []*url.URL{nil, {}, {Scheme: "https"}, {Host: "example.com"}, {Scheme: "ftp", Host: "example.com"}} {
		require.Error(t, ValidateURL(endpoint))
	}
	require.NoError(t, ValidateURL(&url.URL{Scheme: "https", Host: "example.com"}))
	_, err := ParseURL("")
	require.Error(t, err)
}
