package discovery

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/j0sh/boa/pkg/boa"
	"github.com/stretchr/testify/require"
)

func freeAddress(t *testing.T) netip.AddrPort {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := l.Addr().(*net.TCPAddr).AddrPort()
	require.NoError(t, l.Close())
	return address
}

func TestServeWithoutRunnerOrPaymentDependencies(t *testing.T) {
	for _, source := range []string{"", "http://127.0.0.1:1"} {
		t.Run(source, func(t *testing.T) {
			listen, metrics := freeAddress(t), freeAddress(t)
			baseURL := "http://" + listen.String()
			p := Params{Listen: listen, MetricsListen: metrics,
				AdvertiseNodes: true, AdvertiseRunners: true, RefreshInterval: time.Minute, MaxFailedRefreshes: 6}
			if source != "" {
				p.Nodes = []boa.Text[*url.URL]{{Value: mustURL(t, source)}}
				p.Grants = []string{"127.0.0.1:1"}
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, p, io.Discard) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(12 * time.Second):
					t.Error("discovery did not shut down")
				}
			})
			client := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone(), Timeout: time.Second}
			defer client.CloseIdleConnections()
			require.Eventually(t, func() bool {
				response, err := client.Get("http://" + metrics.String() + "/readyz")
				if err != nil {
					return false
				}
				defer response.Body.Close()
				return response.StatusCode == 200
			}, 5*time.Second, 10*time.Millisecond)
			for path, want := range map[string]string{
				"http://" + metrics.String() + "/healthz": "ok\n",
				"http://" + metrics.String() + "/metrics": "livepeer_discovery_up 1\n",
				baseURL + "/discovery":                    "[]\n",
			} {
				response, err := client.Get(path)
				require.NoError(t, err)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				require.Equal(t, 200, response.StatusCode)
				if strings.HasSuffix(path, "/metrics") {
					require.Contains(t, string(body), want)
				} else {
					require.Equal(t, want, string(body))
				}
			}
		})
	}
}

func TestDiscoveryConfigDefaultsAndPrecedence(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	out, err := execute(t, "--config=", "--print-config")
	require.NoError(t, err)
	for _, value := range []string{`Listen = "127.0.0.1:8939"`, `MetricsListen = "127.0.0.1:8940"`, `AdvertiseNodes = true`, `AdvertiseRunners = true`, `MaxFailedRefreshes = 6`, `RefreshInterval = "5m0s"`} {
		require.Contains(t, out, value)
	}
	configDir := filepath.Join(os.Getenv("HOME"), ".lpData", "offchain", "discovery")
	require.NoError(t, os.MkdirAll(configDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.toml"), []byte("RefreshInterval = '1m'\nAdvertiseNodes = false\nAdvertiseRunners = false\nMaxFailedRefreshes = 0\nNodes = ['https://child.example']\nGrants = ['127.0.0.1']\n"), 0600))
	out, err = execute(t, "--print-config")
	require.NoError(t, err)
	for _, value := range []string{`RefreshInterval = "1m0s"`, "AdvertiseNodes = false", "AdvertiseRunners = false", "MaxFailedRefreshes = 0", `Nodes = ["https://child.example"]`, `Grants = ["127.0.0.1"]`} {
		require.Contains(t, out, value)
	}
	t.Setenv("LIVEPEER_DISCOVERY_REFRESH_INTERVAL", "2m")
	out, err = execute(t, "--print-config")
	require.NoError(t, err)
	require.Contains(t, out, `RefreshInterval = "2m0s"`)
	out, err = execute(t, "--refresh-interval=3m", "--advertise-nodes=true", "--print-config")
	require.NoError(t, err)
	require.Contains(t, out, `RefreshInterval = "3m0s"`)
	require.Contains(t, out, "AdvertiseNodes = true")
}

func TestDiscoveryInvalidConfig(t *testing.T) {
	for _, args := range [][]string{
		{"--refresh-interval=0"}, {"--max-failed-refreshes=-1"}, {"--grants=bad"},
		{"--metrics-listen=0.0.0.0:8940"},
		{"--nodes=ftp://child.example"}, {"--nodes=https://user:secret@child.example"},
		{"--nodes=https://child.example?token=secret"}, {"--nodes=https://child.example#fragment"}, {"--runner-config=unused"},
	} {
		_, err := execute(t, append(args, "--config=", "--print-config")...)
		require.Error(t, err, "%v", args)
	}
}

func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cmd := Root(&output, &output)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}
