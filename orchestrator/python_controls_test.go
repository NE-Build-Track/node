package orchestrator

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/livepeer/node/destination"
	"github.com/stretchr/testify/require"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *lockedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buf.String() }

// Ported from e2e-scope's Python runner session control test. The runner uses
// request headers to create and delete channels, create proxy URLs and stop.
func TestPinnedPythonRequestSessionControls(t *testing.T) {
	if testing.Short() {
		t.Skip("Python SDK integration test")
	}
	python, export, root := pinnedPythonSDK(t)
	registry := NewRegistry("bootstrap", "http://127.0.0.1:0", time.Second, 30*time.Second)
	policy, err := destination.New("runner", []string{"127.0.0.1"})
	require.NoError(t, err)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewServer(registry, policy, policy, logger)
	orch := httptest.NewUnstartedServer(server)
	defer orch.Close()
	defer server.Close()
	registry.service = "http://" + orch.Listener.Addr().String()
	orch.Start()
	fixture := filepath.Join(root, "cmd", "livepeer", "testdata", "python_runner_session_controls.py")
	cmd := exec.Command(python, fixture, orch.URL, "bootstrap", "unused", "request-controls", "persistent", "python-controls", "1", "metadata")
	cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(export, "src"))
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	var stderr lockedBuffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	defer func() { _ = stdin.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	startup := bufio.NewScanner(stdout)
	require.True(t, startup.Scan(), "Python runner startup failed: %s", stderr.String())
	var registered struct {
		RunnerID string `json:"runner_id"`
	}
	require.NoError(t, json.Unmarshal(startup.Bytes(), &registered), startup.Text())
	require.NotEmpty(t, registered.RunnerID)
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Post(orch.URL+"/apps/"+registered.RunnerID+"/session", "application/json", nil)
	require.NoError(t, err)
	var reservation struct {
		SessionID string `json:"session_id"`
		AppURL    string `json:"app_url"`
	}
	require.Equal(t, 200, response.StatusCode)
	require.NoError(t, json.NewDecoder(response.Body).Decode(&reservation))
	require.NoError(t, response.Body.Close())
	post := func(url string, body io.Reader) (int, map[string]any) {
		t.Helper()
		response, err := client.Post(url, "application/json", body)
		require.NoError(t, err)
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		var result map[string]any
		require.NoError(t, json.Unmarshal(data, &result), "status=%d body=%q stderr=%s", response.StatusCode, string(data), stderr.String())
		return response.StatusCode, result
	}
	status, channels := post(reservation.AppURL+"/channels", nil)
	require.Equal(t, 200, status)
	require.NotEmpty(t, channels["channel_name"])
	require.Equal(t, []any{"events"}, channels["deleted"])
	status, proxy := post(reservation.AppURL+"/proxy", nil)
	require.Equal(t, 200, status)
	proxyURL, ok := proxy["proxy_url"].(string)
	require.True(t, ok)
	status, target := post(proxyURL+"/foo/bar?x=1", strings.NewReader("proxied-body"))
	require.Equal(t, 200, status)
	require.Equal(t, "/proxy-target/foo/bar", target["path"])
	require.Equal(t, "proxied-body", target["body"])
	status, defaultProxy := post(reservation.AppURL+"/proxy-default", nil)
	require.Equal(t, 200, status)
	defaultURL, ok := defaultProxy["proxy_url"].(string)
	require.True(t, ok)
	status, target = post(defaultURL+"/app-proxy/foo?y=2", strings.NewReader("default-body"))
	require.Equal(t, 200, status)
	require.Equal(t, "/app-proxy/foo", target["path"])
	response, err = client.Post(reservation.AppURL+"/stop", "application/json", nil)
	require.NoError(t, err)
	// Stopping the session cancels this very request before the runner can
	// return its final 204 response.
	require.Equal(t, http.StatusBadGateway, response.StatusCode)
	require.NoError(t, response.Body.Close())
	response, err = client.Post(proxyURL+"/after-stop", "text/plain", nil)
	require.NoError(t, err)
	require.Equal(t, 404, response.StatusCode)
	require.NoError(t, response.Body.Close())
}
