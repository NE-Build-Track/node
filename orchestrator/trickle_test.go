package orchestrator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestDirectTrickleDeleteAllowsNamedChannelRecreation(t *testing.T) {
	reg, s := reviewServer(t, "http://127.0.0.1:1")
	require.NoError(t, reg.AddStatic(StaticRunner{ID: "r", RunnerURL: "http://127.0.0.1:1", App: "test"}))
	sid, _, _, _, err := reg.Reserve("r")
	require.NoError(t, err)
	_, token, _, err := reg.sessionTarget("r", sid)
	require.NoError(t, err)
	create := func() string {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/runner/r/session/"+sid+"/channels", strings.NewReader(`{"channels":[{"name":"events"}]}`))
		req.Header.Set("Livepeer-Session-Token", token)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		var body struct {
			Channels []struct {
				ChannelName string `json:"channel_name"`
			} `json:"channels"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		require.Len(t, body.Channels, 1)
		return body.Channels[0].ChannelName
	}
	first := create()
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/ai/trickle/"+first, nil))
	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, s.channels, first)
	second := create()
	require.NotEqual(t, first, second)
	require.Len(t, s.channels, 1)
}

func TestTrickleActiveStreamAndCancellation(t *testing.T) {
	for _, action := range []string{"delete", "reset", "release"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				reg, s := reviewServer(t, "http://127.0.0.1:1")
				require.NoError(t, reg.AddStatic(StaticRunner{ID: "r", RunnerURL: "http://127.0.0.1:1", App: "test"}))
				sid, _, _, _, err := reg.Reserve("r")
				require.NoError(t, err)
				s.newChannel("test", "events", "application/octet-stream", "r", sid).identity = reg.identity("r", sid)
				input, writer := io.Pipe()
				defer writer.Close()
				pubDone, subDone := make(chan struct{}), make(chan struct{})
				go func() {
					s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/ai/trickle/test/0", input))
					close(pubDone)
				}()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				out := &reviewStreamingWriter{header: http.Header{}, wrote: make(chan struct{}, 1)}
				go func() {
					s.ServeHTTP(out, httptest.NewRequest("GET", "/ai/trickle/test/0", nil).WithContext(ctx))
					close(subDone)
				}()
				_, err = writer.Write([]byte("first"))
				require.NoError(t, err)
				synctest.Wait()
				<-out.wrote
				time.Sleep(time.Minute)
				_, err = writer.Write([]byte("still streaming"))
				require.NoError(t, err)
				synctest.Wait()
				select {
				case <-out.wrote:
				default:
					t.Fatal("active stream timed out")
				}
				switch action {
				case "delete":
					s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("DELETE", "/ai/trickle/test/0", nil))
				case "reset":
					req := httptest.NewRequest("POST", "/ai/trickle/test/1", strings.NewReader("next"))
					req.Header.Set("Lp-Trickle-Reset", "true")
					s.ServeHTTP(httptest.NewRecorder(), req)
				case "release":
					reg.ReleaseBySession(sid)
				}
				synctest.Wait()
				select {
				case <-pubDone:
				default:
					t.Error("publisher was not interrupted")
				}
				select {
				case <-subDone:
				default:
					t.Error("subscriber was not interrupted")
				}
				require.GreaterOrEqual(t, test.MetricSum(t, reg.metrics.Registry, "stream_transfer_bytes_total", map[string]string{"app": "test", "runner_id": "r", "protocol": "trickle", "direction": "out"}), float64(len("firststill streaming")))
				require.Zero(t, test.MetricSum(t, reg.metrics.Registry, "http_requests_in_flight", nil))
			})
		})
	}
}

func TestTrickleOwnershipSurvivesHeartbeatChanges(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "proxy reply") }))
	defer upstream.Close()
	reg, s := reviewServer(t, upstream.URL)
	defer s.Close()
	request := httptest.NewRequest("POST", "/runners/heartbeat", strings.NewReader(`{"runner_id":"r","runner_url":"http://127.0.0.1:1","app":"original","capacity":1}`))
	request.Header.Set("Authorization", "bootstrap")
	response := httptest.NewRecorder()
	s.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var registered heartbeatResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &registered))
	require.NotNil(t, registered.O2R)
	require.Len(t, test.MetricSamples(t, reg.metrics.Registry, "runner_sessions", map[string]string{"runner_id": "r", "state": "active"}), 1)
	require.Zero(t, test.MetricSum(t, reg.metrics.Registry, "runner_sessions", map[string]string{"runner_id": "r"}))
	s.mu.Lock()
	s.publishSessionEvent(registered.O2R.ChannelName, []byte("ownership"))
	s.mu.Unlock()
	_, _, err := reg.Heartbeat(heartbeatRequest{RunnerID: "r", RunnerURL: "http://127.0.0.1:1", App: "updated", Capacity: 1}, registered.HeartbeatSecret)
	require.NoError(t, err)
	response = httptest.NewRecorder()
	s.ServeHTTP(response, httptest.NewRequest("GET", registered.O2R.URL+"/0", nil))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Equal(t, "ownership", response.Body.String())
	require.EqualValues(t, len("ownership"), test.MetricSum(t, reg.metrics.Registry, "stream_transfer_bytes_total", map[string]string{"app": "original", "direction": "out"}))
	require.Zero(t, test.MetricSum(t, reg.metrics.Registry, "stream_transfer_bytes_total", map[string]string{"app": "updated"}))
	reg.proxyTemplate = "https://{proxy}.apps.example"
	require.NoError(t, reg.AddStatic(StaticRunner{ID: "proxy", App: "generated", Mode: "single-shot", RunnerURL: upstream.URL, Capacity: 1, Proxy: true}))
	response = httptest.NewRecorder()
	s.ServeHTTP(response, httptest.NewRequest("GET", "https://proxy.apps.example/ai/trickle/"+registered.O2R.ChannelName+"/0", nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Equal(t, "proxy reply", response.Body.String())
	require.EqualValues(t, 1, test.MetricSum(t, reg.metrics.Registry, "app_calls_total", map[string]string{"app": "generated", "protocol": "http"}))
	for _, app := range []string{"generated", "original"} {
		require.Zero(t, test.MetricSum(t, reg.metrics.Registry, "app_calls_in_flight", map[string]string{"app": app}))
	}
	_, err = reg.Unregister("r", registered.HeartbeatSecret)
	for _, name := range []string{"runner_registered", "runner_sessions", "runner_capacity_available"} {
		require.Empty(t, test.MetricSamples(t, reg.metrics.Registry, name, map[string]string{"runner_id": "r"}))
	}
	require.NoError(t, err)
	before := test.MetricSum(t, reg.metrics.Registry, "stream_transfer_bytes_total", nil)
	response = httptest.NewRecorder()
	s.ServeHTTP(response, httptest.NewRequest("GET", registered.O2R.URL+"/0", nil))
	require.Equal(t, http.StatusNotFound, response.Code)
	require.Equal(t, before, test.MetricSum(t, reg.metrics.Registry, "stream_transfer_bytes_total", nil), "removed ownership must not create empty-label transfer measurements")

}

func TestNamedChannelsAreIdempotentAndReleased(t *testing.T) {
	reg, s := reviewServer(t, "http://127.0.0.1:1")
	require.NoError(t, reg.AddStatic(StaticRunner{ID: "r", RunnerURL: "http://127.0.0.1:1", App: "test"}))
	sid, _, _, _, err := reg.Reserve("r")
	require.NoError(t, err)
	_, token, _, err := reg.sessionTarget("r", sid)
	require.NoError(t, err)
	var previous string
	for range 2 {
		req := httptest.NewRequest("POST", "/runner/r/session/"+sid+"/channels", strings.NewReader(`{"channels":[{"name":"events"}]}`))
		req.Header.Set("Livepeer-Session-Token", token)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		require.Equal(t, 200, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), "application/octet-stream")
		if previous != "" {
			require.Equal(t, previous, w.Body.String())
		}
		previous = w.Body.String()
	}
	require.Len(t, s.channels, 1)
	reg.ReleaseBySession(sid)
	require.Empty(t, s.channels)
}

func TestTrickleLargeSegmentDelivered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, s := reviewServer(t, "http://127.0.0.1:1")
		s.newChannel("large", "events", "application/octet-stream", "", "")
		input, writer := io.Pipe()
		defer writer.Close()
		pub, sub := httptest.NewRecorder(), httptest.NewRecorder()
		pubDone, subDone := make(chan struct{}), make(chan struct{})
		go func() { s.ServeHTTP(pub, httptest.NewRequest("POST", "/ai/trickle/large/0", input)); close(pubDone) }()
		go func() { s.ServeHTTP(sub, httptest.NewRequest("GET", "/ai/trickle/large/0", nil)); close(subDone) }()
		chunk := strings.Repeat("x", 32<<10)
		for range (3 << 20) / len(chunk) {
			_, err := io.WriteString(writer, chunk)
			require.NoError(t, err)
			synctest.Wait() // A reader keeping up must receive the full segment.
		}
		require.NoError(t, writer.Close())
		<-pubDone
		<-subDone
		require.Equal(t, http.StatusOK, pub.Code)
		require.Equal(t, 3<<20, sub.Body.Len())
		late := httptest.NewRecorder()
		s.ServeHTTP(late, httptest.NewRequest("GET", "/ai/trickle/large/0", nil))
		require.Equal(t, http.StatusOK, late.Code)
		require.Equal(t, 3<<20, late.Body.Len())
	})
}
