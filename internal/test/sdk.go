package test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const pythonSDKRevision = "76a1a1c58656ef2ad0c28542ea5f99086b76db69"
const goSDKRevision = "c3be5a14a91f8a3437133419becaced324d804fe"

// GoSDK exports the pinned SDK without changing the developer's checkout.
// It returns an empty path when an unconfigured checkout is absent.
func GoSDK(t *testing.T, root string) string {
	t.Helper()
	sdkDir := os.Getenv("GO_RUNNER_SDK_DIR")
	if sdkDir == "" {
		sdkDir = filepath.Join(root, "..", "golang-runner")
	}
	if _, err := os.Stat(filepath.Join(sdkDir, "go.mod")); err != nil {
		if os.Getenv("GO_RUNNER_SDK_DIR") != "" {
			t.Fatalf("configured Go SDK fixture unavailable: %v", err)
		}
		t.Log("Go runner SDK checkout unavailable; skipped SDK fixture")
		return ""
	}
	t.Attr("go_sdk_revision", goSDKRevision)
	t.Attr("go_sdk_tree", "committed")
	return exportRevision(t, sdkDir, "https://github.com/livepeer/golang-runner.git", goSDKRevision)
}

// PythonSDK exports the pinned SDK without changing the developer's checkout.
// It returns empty paths when an unconfigured checkout or interpreter is absent.
func PythonSDK(t *testing.T, root string) (python, export string) {
	t.Helper()
	sdkDir := os.Getenv("PYTHON_RUNNER_SDK_DIR")
	if sdkDir == "" {
		sdkDir = filepath.Join(root, "..", "python-runner")
	}
	python = os.Getenv("PYTHON_RUNNER_PYTHON")
	if python == "" {
		python = filepath.Join(sdkDir, ".venv", "bin", "python")
	}
	for _, path := range []string{filepath.Join(sdkDir, "pyproject.toml"), python} {
		if _, err := os.Stat(path); err != nil {
			if os.Getenv("PYTHON_RUNNER_SDK_DIR") != "" || os.Getenv("PYTHON_RUNNER_PYTHON") != "" {
				t.Fatalf("configured Python SDK fixture unavailable: %v", err)
			}
			t.Log("Python SDK checkout or interpreter unavailable; skipped SDK fixture")
			return "", ""
		}
	}
	t.Attr("python_sdk_revision", pythonSDKRevision)
	t.Attr("python_sdk_tree", "committed")
	t.Setenv("PYTHONDONTWRITEBYTECODE", "1")
	return python, exportRevision(t, sdkDir, "https://github.com/livepeer/livepeer-python-gateway.git", pythonSDKRevision)
}

func exportRevision(t *testing.T, sdkDir, remote, revision string) string {
	t.Helper()
	if err := exec.Command("git", "-C", sdkDir, "cat-file", "-e", revision+"^{commit}").Run(); err != nil {
		// A development checkout may not have fetched the pinned main revision.
		// Fetch into a temporary repository rather than updating that checkout.
		sdkDir = t.TempDir()
		output, err := exec.Command("git", "init", "--bare", sdkDir).CombinedOutput()
		require.NoError(t, err, string(output))
		output, err = exec.Command("git", "-C", sdkDir, "fetch", "--depth=1", remote, revision).CombinedOutput()
		require.NoError(t, err, "fetch pinned SDK %s: %s", revision, output)
	}
	export := t.TempDir()
	archive := exec.Command("git", "-C", sdkDir, "archive", revision)
	tar := exec.Command("tar", "-xf", "-", "-C", export)
	pipe, err := archive.StdoutPipe()
	require.NoError(t, err)
	tar.Stdin = pipe
	require.NoError(t, tar.Start())
	require.NoError(t, archive.Run())
	require.NoError(t, tar.Wait())
	return export
}
