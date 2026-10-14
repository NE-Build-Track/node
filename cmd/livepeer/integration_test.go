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
	for _, name := range []string{"livepeer", "livepeer-orchestrator", "livepeer-signer", "livepeer-chain"} {
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
	for _, component := range []string{"orchestrator", "signer", "chain"} {
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
	cmd := exec.Command(dispatcher, "orchestrator", "--listen", fmt.Sprintf("127.0.0.1:%d", mainPort), "--metrics-listen", fmt.Sprintf("127.0.0.1:%d", metricsPort), "--bootstrap-secret-file", secretPath, "--runner-grants", "127.0.0.1", "--session-proxy-grants", strings.TrimPrefix(runner.URL, "http://"))
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

func checkPythonSDK(t *testing.T, root, serviceURL, runnerURL string) {
	t.Helper()
	sdkDir := os.Getenv("PYTHON_RUNNER_SDK_DIR")
	if sdkDir == "" {
		sdkDir = filepath.Join(root, "..", "python-runner")
	}
	if _, err := os.Stat(filepath.Join(sdkDir, "pyproject.toml")); err != nil {
		if os.Getenv("PYTHON_RUNNER_SDK_DIR") != "" {
			t.Fatalf("configured Python SDK checkout unavailable: %v", err)
		}
		t.Log("Python runner SDK checkout unavailable; skipped SDK fixture")
		return
	}
	python := os.Getenv("PYTHON_RUNNER_PYTHON")
	if python == "" {
		python = filepath.Join(sdkDir, ".venv", "bin", "python")
	}
	if _, err := os.Stat(python); err != nil {
		if os.Getenv("PYTHON_RUNNER_PYTHON") != "" {
			t.Fatalf("configured Python SDK interpreter unavailable: %v", err)
		}
		t.Log("Python runner SDK interpreter unavailable; skipped SDK fixture")
		return
	}
	revision, err := exec.Command("git", "-C", sdkDir, "rev-parse", "HEAD").Output()
	require.NoError(t, err)
	require.Equal(t, "44df06157fcdb864e37d971e8caba86b2a7dc92e", strings.TrimSpace(string(revision)))
	t.Attr("python_sdk_revision", strings.TrimSpace(string(revision)))
	// By default export the committed tree: a developer checkout can have local
	// experiments without changing the compatibility fixture under test.
	export := t.TempDir()
	archive := exec.Command("git", "-C", sdkDir, "archive", "HEAD")
	tar := exec.Command("tar", "-xf", "-", "-C", export)
	pipe, err := archive.StdoutPipe()
	require.NoError(t, err)
	tar.Stdin = pipe
	require.NoError(t, tar.Start())
	require.NoError(t, archive.Run())
	require.NoError(t, tar.Wait())
	if os.Getenv("PYTHON_RUNNER_USE_WORKING_TREE") == "1" {
		export = sdkDir
		t.Attr("python_sdk_tree", "working-copy")
	} else {
		t.Attr("python_sdk_tree", "committed")
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
	sdkDir := os.Getenv("GO_RUNNER_SDK_DIR")
	if sdkDir == "" {
		sdkDir = filepath.Join(root, "..", "golang-runner")
	}
	if _, err := os.Stat(filepath.Join(sdkDir, "go.mod")); err != nil {
		t.Log("Go runner SDK checkout unavailable; skipped SDK fixture")
		return
	}
	revision, err := exec.Command("git", "-C", sdkDir, "rev-parse", "HEAD").Output()
	require.NoError(t, err)
	require.Equal(t, "c3be5a14a91f8a3437133419becaced324d804fe", strings.TrimSpace(string(revision)))
	t.Attr("go_sdk_revision", strings.TrimSpace(string(revision)))
	status, err := exec.Command("git", "-C", sdkDir, "status", "--porcelain").Output()
	require.NoError(t, err)
	require.Empty(t, status, "SDK checkout must be clean for compatibility result")
	dir := t.TempDir()
	goMod := "module sdkcompat\n\ngo 1.27.2\n\nrequire github.com/livepeer/golang-runner v0.0.0\n\nreplace github.com/livepeer/golang-runner => " + sdkDir + "\n"
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
