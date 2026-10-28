package orchestrator

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func TestDynamicRunnerSessionProxyAndCapacity(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/hello", r.URL.Path)
		require.Equal(t, "value", r.URL.Query().Get("q"))
		require.NotEmpty(t, r.Header.Get("Livepeer-Session-Id"))
		require.NotEmpty(t, r.Header.Get("Livepeer-Session-Token"))
		_, _ = io.WriteString(w, "proxied")
	}))
	defer upstream.Close()
	policy, err := destination.New("runner", []string{"127.0.0.0/8"})
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", 10*time.Millisecond, 50*time.Millisecond)
	srv := httptest.NewServer(NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()

	body := `{"runner_url":"` + upstream.URL + `/api","app":"test","mode":"persistent","capacity":1,"price_info":{"price":0,"currency":"usd","unit":"hour"}}`
	request, err := http.NewRequest(http.MethodPost, srv.URL+"/runners/heartbeat", bytes.NewBufferString(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "bootstrap")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var heartbeat heartbeatResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&heartbeat))
	require.NoError(t, response.Body.Close())
	require.Regexp(t, `^[a-z2-7]{52}$`, heartbeat.HeartbeatSecret)
	require.Regexp(t, `^runner_[a-z2-7]{8}$`, heartbeat.RunnerID)
	require.NotNil(t, heartbeat.O2R)
	require.Regexp(t, "^"+heartbeat.RunnerID+`-[a-z2-7]{16}-o2r$`, heartbeat.O2R.ChannelName)
	// A heartbeat can move a runner to another port covered by the same grant.
	replacement := httptest.NewServer(upstream.Config.Handler)
	defer replacement.Close()
	upstream.Close()
	body = strings.Replace(body, upstream.URL, replacement.URL, 1)
	request, err = http.NewRequest(http.MethodPost, srv.URL+"/runners/heartbeat", bytes.NewBufferString(strings.Replace(body, `{"runner_url"`, `{"runner_id":"`+heartbeat.RunnerID+`","runner_url"`, 1)))
	require.NoError(t, err)
	request.Header.Set("Authorization", heartbeat.HeartbeatSecret)
	response, err = http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
	response, err = http.Post(srv.URL+"/apps/"+heartbeat.RunnerID+"/session", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var reserved struct {
		SessionID string `json:"session_id"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&reserved))
	require.Regexp(t, `^session_[a-z2-7]{16}$`, reserved.SessionID)
	require.NoError(t, response.Body.Close())
	response, err = http.Get(srv.URL + "/ai/trickle/" + heartbeat.O2R.ChannelName + "/0")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var event map[string]string
	require.NoError(t, json.NewDecoder(response.Body).Decode(&event))
	require.Equal(t, "reserved", event["event"])
	require.Equal(t, reserved.SessionID, event["session"])
	require.True(t, registry.validSessionToken(heartbeat.RunnerID, reserved.SessionID, event["session_token"]))
	require.NoError(t, response.Body.Close())
	response, err = http.Post(srv.URL+"/apps/"+heartbeat.RunnerID+"/session", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	require.NoError(t, response.Body.Close())
	response, err = http.Get(srv.URL + "/apps/" + heartbeat.RunnerID + "/session/" + reserved.SessionID + "/app/hello?q=value")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "proxied", string(data))
	require.NoError(t, response.Body.Close())
	response, err = http.Post(srv.URL+"/apps/"+heartbeat.RunnerID+"/session/"+reserved.SessionID+"/stop", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
}

func TestSessionCallbackCredentials(t *testing.T) {
	registry, server := reviewServer(t, "http://127.0.0.1:1")
	defer server.Close()
	require.NoError(t, registry.AddStatic(StaticRunner{ID: "r", RunnerURL: "http://127.0.0.1:1", App: "app", Capacity: 2}))
	sid, _, _, _, err := registry.Reserve("r")
	require.NoError(t, err)
	other, _, _, _, err := registry.Reserve("r")
	require.NoError(t, err)
	target, token, _, err := registry.sessionTarget("r", sid)
	require.NoError(t, err)
	_, otherToken, _, err := registry.sessionTarget("r", other)
	require.NoError(t, err)
	for _, operation := range []struct{ method, path string }{
		{http.MethodPost, "proxy"},
		{http.MethodPost, "channels"},
		{http.MethodDelete, "channels"},
		{http.MethodPost, "stop"},
	} {
		for name, credential := range map[string]string{"missing": "", "wrong": "wrong", "cross-session": otherToken} {
			t.Run(operation.method+" "+operation.path+"/"+name, func(t *testing.T) {
				req := httptest.NewRequest(operation.method, "/runner/r/session/"+sid+"/"+operation.path, strings.NewReader(`{}`))
				if credential != "" {
					req.Header.Set("Livepeer-Session-Token", credential)
				}
				w := httptest.NewRecorder()
				server.ServeHTTP(w, req)
				require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
			})
		}
	}
	for range 25 {
		_, err := registry.addProxy("r", sid, token, target)
		require.NoError(t, err)
	}
	req := httptest.NewRequest(http.MethodPost, "/runner/r/session/"+sid+"/proxy", strings.NewReader(`{}`))
	req.Header.Set("Livepeer-Session-Token", token)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Contains(t, w.Body.String(), "too many session proxies")
}

func TestRunnerRegistryHasBoundedCapacity(t *testing.T) {
	registry := NewRegistry("bootstrap", "https://orchestrator.example", time.Second, time.Minute)
	for i := range maxRunners {
		_, status, err := registry.Heartbeat(heartbeatRequest{
			RunnerID: fmt.Sprintf("runner%d", i), RunnerURL: "https://runner.example",
			App: "test", Mode: "persistent", Capacity: 1,
		}, "bootstrap")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
	}
	_, status, err := registry.Heartbeat(heartbeatRequest{
		RunnerID: "overflow", RunnerURL: "https://runner.example",
		App: "test", Mode: "persistent", Capacity: 1,
	}, "bootstrap")
	require.ErrorContains(t, err, "capacity")
	require.Equal(t, http.StatusServiceUnavailable, status)
}

func TestSessionProxyStreamsSSEAndWebSocket(t *testing.T) {
	upgrader := websocket.Upgrader{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: hello\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case "/ws":
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			kind, message, err := conn.ReadMessage()
			if err == nil {
				_ = conn.WriteMessage(kind, message)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer upstream.Close()
	grant := strings.TrimPrefix(upstream.URL, "http://")
	policy, err := destination.New("runner", []string{grant})
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Second, time.Minute)
	registered, _, err := registry.Heartbeat(heartbeatRequest{RunnerURL: upstream.URL, App: "stream", Mode: "persistent", Capacity: 1}, "bootstrap")
	require.NoError(t, err)
	server := httptest.NewServer(NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	sid, _, _, _, err := registry.Reserve(registered.RunnerID)
	require.NoError(t, err)
	base := server.URL + "/apps/" + registered.RunnerID + "/session/" + sid + "/app"
	response, err := http.Get(base + "/events")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "text/event-stream", response.Header.Get("Content-Type"))
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "data: hello\n", line)
	require.NoError(t, response.Body.Close())
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/ws"
	conn, handshake, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	if handshake != nil {
		require.NoError(t, handshake.Body.Close())
	}
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte("echo")))
	kind, message, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.TextMessage, kind)
	require.Equal(t, "echo", string(message))
	require.NoError(t, conn.Close())
	require.Eventually(t, func() bool { return test.MetricSum(t, registry.metrics.Registry, "app_calls_total", nil) == 2 }, time.Second, 10*time.Millisecond)
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"protocol": "websocket"}))
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "app_calls_total", map[string]string{"protocol": "sse", "outcome": "client_cancelled"}))
	require.Zero(t, test.MetricSum(t, registry.metrics.Registry, "app_calls_in_flight", nil))
}

func TestRunnerDestinationDeniedWithoutGrant(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer upstream.Close()
	policy, err := destination.New("runner", nil)
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Second, time.Minute)
	server := httptest.NewServer(NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	body := `{"runner_url":"` + upstream.URL + `","app":"test","mode":"single-shot","capacity":1,"price_info":{"price":0}}`
	request, err := http.NewRequest(http.MethodPost, server.URL+"/runners/heartbeat", bytes.NewBufferString(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "bootstrap")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	var heartbeat heartbeatResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&heartbeat))
	require.NoError(t, response.Body.Close())
	response, err = http.Get(server.URL + "/apps/" + heartbeat.RunnerID + "/app")
	require.NoError(t, err)
	require.Equal(t, http.StatusBadGateway, response.StatusCode)
	require.NoError(t, response.Body.Close())
}

func TestGeneratedProxyHasSeparateDestinationGrant(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer upstream.Close()
	grant := strings.TrimPrefix(upstream.URL, "http://")
	runnerPolicy, err := destination.New("runner", []string{"127.0.0.0/8"})
	require.NoError(t, err)
	proxyPolicy, err := destination.New("session-proxy", nil)
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Second, time.Minute)
	registered, status, err := registry.Heartbeat(heartbeatRequest{RunnerURL: upstream.URL, App: "test", Mode: "persistent", Capacity: 1}, "bootstrap")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	sid, _, _, status, err := registry.Reserve(registered.RunnerID)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	_, token, status, err := registry.sessionTarget(registered.RunnerID, sid)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	srv := NewServer(registry, runnerPolicy, proxyPolicy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	path := "/runner/" + registered.RunnerID + "/session/" + sid + "/proxy"
	create := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(`{"target_url":"`+upstream.URL+`"}`))
	create.Header.Set("Livepeer-Session-Token", "bad")
	record := httptest.NewRecorder()
	srv.ServeHTTP(record, create)
	require.Equal(t, http.StatusUnauthorized, record.Code)
	create = httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(`{"target_url":"`+upstream.URL+`"}`))
	create.Header.Set("Livepeer-Session-Token", token)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, create)
	require.Equal(t, http.StatusOK, record.Code)
	var created struct {
		ProxyID string `json:"proxy_id"`
		URL     string `json:"url"`
	}
	require.NoError(t, json.Unmarshal(record.Body.Bytes(), &created))
	require.NotEmpty(t, created.ProxyID)
	require.Equal(t, "http://orchestrator.example/run/"+created.ProxyID, created.URL)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodGet, "/run/"+created.ProxyID, nil))
	require.Equal(t, http.StatusBadGateway, record.Code)
	granted, err := destination.New("session-proxy", []string{grant})
	require.NoError(t, err)
	srv = NewServer(registry, runnerPolicy, granted, slog.New(slog.NewTextHandler(io.Discard, nil)))
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodGet, "/run/"+created.ProxyID, nil))
	require.Equal(t, http.StatusOK, record.Code)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodGet, "/proxy/"+created.ProxyID, nil))
	require.Equal(t, http.StatusOK, record.Code)
	create = httptest.NewRequest("POST", "/runner/"+registered.RunnerID+"/session/"+sid+"/channels", strings.NewReader(`{"channels":[{"name":"events"}]}`))
	create.Header.Set("Livepeer-Session-Token", token)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, create)
	require.Equal(t, 200, record.Code)
	require.EqualValues(t, 1, test.MetricSum(t, registry.metrics.Registry, "trickle_channels", map[string]string{"kind": "session"}), "the replacement server owns the only snapshot")
	_, err = registry.metrics.Registry.Gather()
	require.NoError(t, err)
}

func TestRunnerExpires(t *testing.T) {
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Millisecond, 2*time.Millisecond)
	registered, _, err := registry.Heartbeat(heartbeatRequest{RunnerURL: "https://runner.example", App: "test", Mode: "persistent", Capacity: 1}, "bootstrap")
	require.NoError(t, err)
	require.NotEmpty(t, registered.RunnerID)
	require.Len(t, test.MetricSamples(t, registry.metrics.Registry, "runner_sessions", map[string]string{"runner_id": registered.RunnerID, "state": "active"}), 1)
	require.Zero(t, test.MetricSum(t, registry.metrics.Registry, "runner_sessions", nil))
	time.Sleep(3 * time.Millisecond)
	registry.Expire()
	count, _ := registry.Counts()
	require.Zero(t, count)
	for _, name := range []string{"runner_registered", "runner_sessions", "runner_capacity_available"} {
		require.Empty(t, test.MetricSamples(t, registry.metrics.Registry, name, nil))
	}
}

func TestTrickleChannelsScopedToSession(t *testing.T) {
	policy, err := destination.New("runner", nil)
	require.NoError(t, err)
	registry := NewRegistry("bootstrap", "http://orchestrator.example", time.Second, time.Minute)
	registered, _, err := registry.Heartbeat(heartbeatRequest{RunnerURL: "https://runner.example", App: "test", Mode: "persistent", Capacity: 2}, "bootstrap")
	require.NoError(t, err)
	srv := NewServer(registry, policy, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	sid1, _, _, _, err := registry.Reserve(registered.RunnerID)
	require.NoError(t, err)
	sid2, _, _, _, err := registry.Reserve(registered.RunnerID)
	require.NoError(t, err)
	_, token1, _, err := registry.sessionTarget(registered.RunnerID, sid1)
	require.NoError(t, err)
	_, token2, _, err := registry.sessionTarget(registered.RunnerID, sid2)
	require.NoError(t, err)
	path1 := "/runner/" + registered.RunnerID + "/session/" + sid1 + "/channels"
	create := httptest.NewRequest(http.MethodPost, path1, bytes.NewBufferString(`{"channels":[{"name":"events","mime_type":"application/json"}]}`))
	create.Header.Set("Livepeer-Session-Token", token1)
	record := httptest.NewRecorder()
	srv.ServeHTTP(record, create)
	require.Equal(t, http.StatusOK, record.Code)
	var created struct {
		Channels []struct {
			ChannelName string `json:"channel_name"`
		} `json:"channels"`
	}
	require.NoError(t, json.Unmarshal(record.Body.Bytes(), &created))
	require.Len(t, created.Channels, 1)
	channelID := created.Channels[0].ChannelName
	path2 := "/runner/" + registered.RunnerID + "/session/" + sid2 + "/channels"
	deletion := httptest.NewRequest(http.MethodDelete, path2, bytes.NewBufferString(`{"channels":["`+channelID+`"]}`))
	deletion.Header.Set("Livepeer-Session-Token", token2)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, deletion)
	require.Equal(t, http.StatusOK, record.Code)
	require.JSONEq(t, `{"deleted":[]}`, record.Body.String())
	segment := "/ai/trickle/" + channelID + "/0"
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodPost, segment, bytes.NewBufferString(`{"x":1}`)))
	require.Equal(t, http.StatusOK, record.Code)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodGet, segment, nil))
	require.Equal(t, http.StatusOK, record.Code)
	require.JSONEq(t, `{"x":1}`, record.Body.String())
	deletion = httptest.NewRequest(http.MethodDelete, path1, bytes.NewBufferString(`{"channels":["`+channelID+`"]}`))
	deletion.Header.Set("Livepeer-Session-Token", token1)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, deletion)
	require.Equal(t, http.StatusOK, record.Code)
	record = httptest.NewRecorder()
	srv.ServeHTTP(record, httptest.NewRequest(http.MethodGet, segment, nil))
	require.Equal(t, http.StatusNotFound, record.Code)
}

func TestRunnerGPUIsPreservedForPythonDiscovery(t *testing.T) {
	var request heartbeatRequest
	require.NoError(t, json.Unmarshal([]byte(`{"runner_id":"gpu-runner","runner_url":"https://runner.example","app":"retained","mode":"single-shot","status":"ready","capacity":1,"gpu":{"id":"0","name":"H100","vram_mb":80000}}`), &request))
	registry := NewRegistry("bootstrap", "https://orchestrator.example", time.Second, 30*time.Second)
	_, status, err := registry.Heartbeat(request, "bootstrap")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	entries := registry.Discovery()
	require.Len(t, entries, 1)
	require.Len(t, entries[0].Runners, 1)
	require.Equal(t, "H100", entries[0].Runners[0].GPU.Name)
}
