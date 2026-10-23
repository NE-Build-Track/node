package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/require"
)

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func TestRealBinaryOffchainFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("real binary integration test")
	}
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	install := t.TempDir()
	for _, name := range []string{"livepeer", "livepeer-orchestrator", "livepeer-signer", "livepeer-chain", "livepeer-discovery"} {
		build := exec.Command("go", "build", "-o", filepath.Join(install, name), "./cmd/"+name)
		build.Dir = root
		output, err := build.CombinedOutput()
		require.NoError(t, err, string(output))
	}
	dispatcher := filepath.Join(install, "livepeer")
	version, err := exec.Command(dispatcher, "version").CombinedOutput()
	require.NoError(t, err, string(version))
	require.Contains(t, string(version), "livepeer dev")
	help, err := exec.Command(dispatcher, "orchestrator", "--help").CombinedOutput()
	require.NoError(t, err, string(help))
	require.Contains(t, string(help), "bootstrap-secret-file")
	completion, err := exec.Command(dispatcher, "completion", "orchestrator", "bash").CombinedOutput()
	require.NoError(t, err, string(completion))
	require.Contains(t, string(completion), "bash completion")
	for _, component := range []string{"orchestrator", "signer", "chain", "discovery"} {
		direct := filepath.Join(install, "livepeer-"+component)
		for _, arguments := range [][]string{{"--help"}, {"completion", "bash"}, {"completion", "zsh"}, {"completion", "fish"}, {"completion", "powershell"}} {
			actual, err := exec.Command(direct, arguments...).CombinedOutput()
			require.NoError(t, err, string(actual))
			dispatched := append([]string{component}, arguments...)
			if arguments[0] == "completion" {
				dispatched = append([]string{"completion", component}, arguments[1:]...)
			}
			forwarded, err := exec.Command(dispatcher, dispatched...).CombinedOutput()
			require.NoError(t, err, string(forwarded))
			require.Equal(t, string(actual), string(forwarded), "component=%s args=%v", component, arguments)
			golden := filepath.Join(root, "cmd", "livepeer", "testdata", "cli", component+"-"+strings.TrimPrefix(strings.Join(arguments, "-"), "--")+".golden")
			home, err := os.UserHomeDir()
			require.NoError(t, err)
			value := bytes.ReplaceAll(actual, []byte(home), []byte("$HOME"))
			if arguments[0] == "completion" {
				// The generated shell boilerplate is large; pin its complete digest.
				value = fmt.Appendf(nil, "%x\n", sha256.Sum256(value))
			}
			if os.Getenv("UPDATE_CLI_GOLDENS") == "1" {
				require.NoError(t, os.MkdirAll(filepath.Dir(golden), 0755))
				require.NoError(t, os.WriteFile(golden, value, 0644))
			}
			expected, err := os.ReadFile(golden)
			require.NoError(t, err)
			if !bytes.Equal(expected, value) {
				artifact := filepath.Join(t.ArtifactDir(), filepath.Base(golden)+".actual")
				require.NoError(t, os.WriteFile(artifact, actual, 0644))
				t.Errorf("CLI golden differs: %s; full output: %s", golden, artifact)
			}

		}
	}

	testBinaryMigrations(t, install)

	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("Livepeer-Session-Token"))
		if r.URL.Path == "/credentials" {
			write := map[string]string{"token": r.Header.Get("Livepeer-Session-Token")}
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(write))
			return
		}
		require.Equal(t, "/hello", r.URL.Path)
		_, _ = io.WriteString(w, "from-runner")
	}))
	defer runner.Close()
	mainPort, metricsPort := freePort(t), freePort(t)
	serviceURL := fmt.Sprintf("http://127.0.0.1:%d", mainPort)
	metricsURL := fmt.Sprintf("http://127.0.0.1:%d", metricsPort)
	secretPath := filepath.Join(t.TempDir(), "bootstrap")
	require.NoError(t, os.WriteFile(secretPath, []byte("exact-bootstrap"), 0600))
	cmd := exec.Command(dispatcher, "orchestrator", "--listen", fmt.Sprintf("127.0.0.1:%d", mainPort), "--metrics-listen", fmt.Sprintf("127.0.0.1:%d", metricsPort), "--bootstrap-secret-file", secretPath, "--runner-grants", "127.0.0.0/8", "--session-proxy-grants", strings.TrimPrefix(runner.URL, "http://"))
	var log bytes.Buffer
	cmd.Stdout, cmd.Stderr = &log, &log
	require.NoError(t, cmd.Start())
	processDone := make(chan struct{})
	var processErr error
	go func() { processErr = cmd.Wait(); close(processDone) }()
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-processDone:
			case <-time.After(3 * time.Second):
				_ = cmd.Process.Kill()
				<-processDone
			}
		}
		_ = os.WriteFile(filepath.Join(t.ArtifactDir(), "orchestrator.log"), log.Bytes(), 0644)
	})
	require.Eventually(t, func() bool {
		response, err := http.Get(metricsURL + "/readyz")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond)
	request, err := http.NewRequest(http.MethodPost, serviceURL+"/runners/heartbeat", bytes.NewBufferString(`{"runner_url":"`+runner.URL+`","app":"e2e","mode":"persistent","capacity":1,"price_info":{"price":0}}`))
	require.NoError(t, err)
	request.Header.Set("Authorization", "exact-bootstrap")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var registered struct {
		RunnerID        string `json:"runner_id"`
		HeartbeatSecret string `json:"heartbeat_secret"`
		Orchestrator    string `json:"orchestrator"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&registered))
	require.NoError(t, response.Body.Close())
	require.NotEmpty(t, registered.HeartbeatSecret)
	require.Equal(t, serviceURL, registered.Orchestrator)
	response, err = http.Post(serviceURL+"/apps/"+registered.RunnerID+"/session", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var reserved struct {
		SessionID string `json:"session_id"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&reserved))
	require.NoError(t, response.Body.Close())
	response, err = http.Get(serviceURL + "/apps/" + registered.RunnerID + "/session/" + reserved.SessionID + "/app/hello")
	require.NoError(t, err)
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "from-runner", string(data))
	require.NoError(t, response.Body.Close())
	testDiscoveryBinary(t, install, serviceURL)
	checkGoSDK(t, root, serviceURL, runner.URL)
	checkPythonSDK(t, root, serviceURL, runner.URL)
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	select {
	case <-processDone:
		require.NoError(t, processErr, log.String())
	case <-time.After(5 * time.Second):
		t.Fatal("orchestrator did not shut down")
	}
	cmd.Process = nil
}

func testDiscoveryBinary(t *testing.T, install, orchestratorURL string) {
	t.Helper()
	listen := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	metrics := fmt.Sprintf("http://127.0.0.1:%d", freePort(t))
	baseURL := "http://" + listen
	logFile, err := os.Create(filepath.Join(t.TempDir(), "discovery.log"))
	require.NoError(t, err)
	defer logFile.Close()
	cmd := exec.Command(filepath.Join(install, "livepeer"), "discovery", "--config=", "--listen", listen,
		"--metrics-listen", strings.TrimPrefix(metrics, "http://"),
		"--nodes", orchestratorURL, "--grants", strings.TrimPrefix(orchestratorURL, "http://"))
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("discovery did not shut down")
		}
	}()
	client := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool {
		response, err := client.Get(metrics + "/readyz")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == 200
	}, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		response, err := client.Get(baseURL + "/discovery")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var entries []struct {
			Address string `json:"address"`
			Runners []struct {
				URL string `json:"url"`
			} `json:"runners"`
		}
		if json.NewDecoder(response.Body).Decode(&entries) != nil || len(entries) != 2 {
			return false
		}
		return entries[0].Address == "" && entries[1].Address == orchestratorURL && len(entries[1].Runners) > 0 &&
			strings.HasPrefix(entries[1].Runners[0].URL, orchestratorURL+"/apps/")
	}, 5*time.Second, 20*time.Millisecond)
}

func checkPythonSDK(t *testing.T, root, serviceURL, runnerURL string) {
	t.Helper()
	python, export := test.PythonSDK(t, root)
	if python == "" {
		return
	}
	fixture := filepath.Join(root, "cmd", "livepeer", "testdata", "python_sdk_compat.py")
	run := exec.Command(python, fixture, serviceURL, runnerURL, "exact-bootstrap")
	run.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(export, "src"), "PYTHONDONTWRITEBYTECODE=1")
	output, err := run.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Python SDK registration")
}

func checkGoSDK(t *testing.T, root, serviceURL, runnerURL string) {
	t.Helper()
	sdkDir := test.GoSDK(t, root)
	if sdkDir == "" {
		return
	}
	module := exec.Command("go", "list", "-m", "-f", "{{.GoVersion}}")
	module.Dir = root
	goVersion, err := module.Output()
	require.NoError(t, err)
	dir := t.TempDir()
	goMod := fmt.Sprintf("module sdkcompat\n\ngo %s\n\nrequire github.com/livepeer/golang-runner v0.0.0\n\nreplace github.com/livepeer/golang-runner => %s\n", strings.TrimSpace(string(goVersion)), sdkDir)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0600))
	source, err := os.ReadFile(filepath.Join(root, "cmd", "livepeer", "testdata", "go_sdk_compat.go"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "main.go"), source, 0600))
	run := exec.Command("go", "run", "main.go", serviceURL, runnerURL, "exact-bootstrap")
	run.Dir = dir
	output, err := run.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Go SDK registration")
}
