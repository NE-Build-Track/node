package orchestrator

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/node/destination"
	"github.com/stretchr/testify/require"
)

func TestRegressionUnauthenticatedSlowBodiesCannotStarveDiscovery(t *testing.T) {
	address := fmt.Sprintf("127.0.0.1:%d", freeTCPPort(t))
	p := Params{Listen: netip.MustParseAddrPort(address), MetricsListen: netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", freeTCPPort(t))), ServiceURL: boa.Text[*url.URL]{Value: mustURL(t, "http://"+address)}, BootstrapSecret: "bootstrap", HeartbeatInterval: time.Second, HeartbeatTTL: time.Minute}
	require.NoError(t, p.Validate())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, p, io.Discard) }()
	var connections []net.Conn
	t.Cleanup(func() {
		for _, c := range connections {
			_ = c.Close()
		}
		cancel()
		require.NoError(t, <-done)
	})
	client := &http.Client{Timeout: time.Second}
	discoveryStatus := func() int {
		r, e := client.Get(p.ServiceURL.String() + "/discovery")
		if e != nil {
			return 0
		}
		defer r.Body.Close()
		return r.StatusCode
	}
	require.Eventually(t, func() bool { return discoveryStatus() == 200 }, 3*time.Second, 10*time.Millisecond)
	for range 256 {
		c, err := net.DialTimeout("tcp", address, time.Second)
		require.NoError(t, err)
		connections = append(connections, c)
		_, err = fmt.Fprintf(c, "POST /runners/heartbeat HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: 1048576\r\n\r\n{", address)
		require.NoError(t, err)
	}
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 200, discoveryStatus(), "unauthenticated incomplete control bodies still occupy every slot beyond ReadHeaderTimeout")
	c, err := net.DialTimeout("tcp", address, time.Second)
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(7*time.Second)))
	_, err = fmt.Fprintf(c, "POST /runners/heartbeat HTTP/1.1\r\nHost: %s\r\nAuthorization: bootstrap\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{", address)
	require.NoError(t, err)
	require.Equal(t, 200, discoveryStatus())
	response, err := http.ReadResponse(bufio.NewReader(c), nil)
	require.NoError(t, err, "authenticated incomplete control body must time out")
	defer response.Body.Close()
	require.Equal(t, http.StatusBadRequest, response.StatusCode)

}

func reviewServer(t *testing.T, upstream string) (*Registry, *Server) {
	t.Helper()
	p, err := destination.New("runner", []string{strings.TrimPrefix(upstream, "http://")})
	require.NoError(t, err)
	reg := NewRegistry("bootstrap", "https://orch.example", time.Second, time.Minute)
	return reg, NewServer(reg, p, p, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestRegressionProxySendsContinueBeforeBody(t *testing.T) {
	// codifying receive of 100 Continue after headers
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	defer upstream.Close()
	registry, server := reviewServer(t, upstream.URL)
	defer server.Close()
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "one", RunnerURL: upstream.URL, App: "one", Mode: "single-shot", Capacity: 1}))
	orch := httptest.NewServer(server)
	defer orch.Close()
	address := orch.Listener.Addr().String()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
	// Advertise a body, but send no bytes: continuation must arrive first.
	_, err = fmt.Fprintf(conn, "POST /apps/one/app HTTP/1.1\r\nHost: %s\r\nExpect: 100-continue\r\nContent-Length: 1\r\n\r\n", address)
	require.NoError(t, err)
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusContinue, response.StatusCode)
}

func TestRegressionOffchainSingleShotCapacityAndHeaders(t *testing.T) {
	seen := make(chan http.Header, 2)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen <- r.Header.Clone(); <-release; w.WriteHeader(200) }))
	defer upstream.Close()
	defer close(release)
	reg, s := reviewServer(t, upstream.URL)
	require.NoError(t, reg.AddStatic(StaticRunner{ID: "one", RunnerURL: upstream.URL, App: "one", Mode: "single-shot", Capacity: 1}))
	go s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/apps/one/app", nil))
	header := <-seen
	t.Run("session_headers", func(t *testing.T) {
		require.NotEmpty(t, header.Get("Livepeer-Session-Id"), "off-chain single-shot lost its session control headers")
	})
	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest("GET", "/apps/one/app", nil))
		done <- w.Code
	}()
	select {
	case <-seen:
		t.Error("second request reached capacity=1 upstream while first was still running")
	case code := <-done:
		require.Equal(t, http.StatusServiceUnavailable, code)
	case <-time.After(time.Second):
		t.Fatal("second request neither rejected nor routed")
	}
}
func TestRegressionStaticSessionChannelsReleased(t *testing.T) {
	reg, s := reviewServer(t, "http://127.0.0.1:1")
	require.NoError(t, reg.AddStatic(StaticRunner{ID: "static", RunnerURL: "http://127.0.0.1:1", App: "a", Capacity: 1}))
	sid, _, _, _, err := reg.Reserve("static")
	require.NoError(t, err)
	s.newChannel("owned", "events", "application/octet-stream", "static", sid)
	reg.ReleaseBySession(sid)
	require.Empty(t, s.channels, "static runners have no O2R channel, but session channels must still be removed")
}
func TestRegressionProxyTrueReturnsGeneratedURL(t *testing.T) {
	reg, _ := reviewServer(t, "http://127.0.0.1:1")
	require.NoError(t, reg.AddStatic(StaticRunner{ID: "static", RunnerURL: "http://127.0.0.1:1", App: "a", Capacity: 1, Proxy: true}))
	_, app, _, _, err := reg.Reserve("static")
	require.NoError(t, err)
	require.Contains(t, app, "/run/", "proxy=true must retain the upstream generated-proxy behavior")
}
func TestRegressionSessionPricePinnedAcrossHeartbeat(t *testing.T) {
	reg, _ := reviewServer(t, "http://127.0.0.1:1")
	reg.SetWeiPerUSD(big.NewRat(3600, 1))
	request := heartbeatRequest{RunnerID: "r", RunnerURL: "http://127.0.0.1:1", App: "a", Capacity: 1, PriceInfo: priceInfo{Price: "1", Currency: "usd", Unit: "hour"}}
	first, _, err := reg.Heartbeat(request, "bootstrap")
	require.NoError(t, err)
	sid, _, _, _, err := reg.Reserve("r")
	require.NoError(t, err)
	request.PriceInfo.Price = "2"
	_, _, err = reg.Heartbeat(request, first.HeartbeatSecret)
	require.NoError(t, err)
	for _, price := range []priceInfo{{}, {Price: "0"}, {Price: "0.0"}} {
		invalid := request
		invalid.PriceInfo = price
		_, status, err := reg.Heartbeat(invalid, first.HeartbeatSecret)
		require.ErrorContains(t, err, "positive")
		require.Equal(t, 400, status)
		invalid.RunnerID = "new"
		_, status, err = reg.Heartbeat(invalid, "bootstrap")
		require.ErrorContains(t, err, "positive")
		require.Equal(t, 400, status)
	}
	current, _, err := reg.PriceForRunner("r")
	require.NoError(t, err)
	require.Equal(t, "2", current.Price.String(), "rejected heartbeat must preserve the current paid price")
	quote, _, err := reg.PriceForSession("r", sid)
	require.NoError(t, err)
	require.Equal(t, "1", quote.Price.String(), "existing session must retain its 1-wei price")
}
func TestRegressionHourlyUSDMatchesAdvertisedUnit(t *testing.T) {
	reg, _ := reviewServer(t, "http://127.0.0.1:1")
	reg.SetWeiPerUSD(big.NewRat(3600, 1))
	price := priceInfo{Price: "3600", Currency: "usd", Unit: "hour"}
	require.NoError(t, reg.normalizePrice(&price))
	require.Equal(t, "seconds", price.Unit)
	require.Equal(t, "1", price.PriceUSD.String(), "$3600/hour must advertise $1/second")
}
func TestRegressionTrickleNextDeliversNextPublishedPart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, s := reviewServer(t, "http://127.0.0.1:1")
		s.newChannel("test", "events", "application/octet-stream", "", "")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan struct{})
		w := httptest.NewRecorder()
		go func() {
			s.ServeHTTP(w, httptest.NewRequest("GET", "/ai/trickle/test/-1", nil).WithContext(ctx))
			close(done)
		}()
		synctest.Wait()
		pub := httptest.NewRecorder()
		s.ServeHTTP(pub, httptest.NewRequest("POST", "/ai/trickle/test/0", strings.NewReader("first")))
		require.Equal(t, 200, pub.Code)
		synctest.Wait()
		select {
		case <-done:
			require.Equal(t, "first", w.Body.String())
		default:
			t.Error("GET -1 skipped the part just published and is still waiting")
			cancel()
			synctest.Wait()
		}
	})
}

type reviewStreamingWriter struct {
	header http.Header
	wrote  chan struct{}
}

func (w *reviewStreamingWriter) Header() http.Header { return w.header }
func (w *reviewStreamingWriter) WriteHeader(int)     {}
func (w *reviewStreamingWriter) Write(b []byte) (int, error) {
	select {
	case w.wrote <- struct{}{}:
	default:
	}
	return len(b), nil
}
func (w *reviewStreamingWriter) Flush() {}
func TestRegressionTrickleStreamsBeforePublisherEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, s := reviewServer(t, "http://127.0.0.1:1")
		s.newChannel("test", "events", "application/octet-stream", "", "")
		input, writer := io.Pipe()
		defer writer.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		out := &reviewStreamingWriter{header: http.Header{}, wrote: make(chan struct{}, 1)}
		go s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/ai/trickle/test/0", input))
		go s.ServeHTTP(out, httptest.NewRequest("GET", "/ai/trickle/test/0", nil).WithContext(ctx))
		_, err := writer.Write([]byte("streaming chunk"))
		require.NoError(t, err)
		synctest.Wait()
		select {
		case <-out.wrote:
		default:
			t.Error("subscriber cannot receive any bytes until the publisher closes its segment")
		}
		require.NoError(t, writer.Close())
		synctest.Wait()
		cancel()
	})
}
func TestRegressionReleasedSessionCancelsInFlightProxy(t *testing.T) {
	started := make(chan struct{})
	ended := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(ended) }))
	defer upstream.Close()
	reg, s := reviewServer(t, upstream.URL)
	require.NoError(t, reg.AddStatic(StaticRunner{ID: "r", RunnerURL: upstream.URL, App: "a", Capacity: 1}))
	sid, app, _, _, err := reg.Reserve("r")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", app, nil).WithContext(ctx))
		close(done)
	}()
	<-started
	reg.ReleaseBySession(sid)
	select {
	case <-ended:
	case <-time.After(200 * time.Millisecond):
		t.Error("session was removed but its upstream request remains live")
	}
	cancel()
	<-done
}
