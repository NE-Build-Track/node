package test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExportRevisionPreservesDevelopmentCheckout(t *testing.T) {
	git := func(t *testing.T, dir string, args ...string) string {
		t.Helper()
		output, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
		require.NoError(t, err, string(output))
		return strings.TrimSpace(string(output))
	}
	commit := func(t *testing.T, dir, content string) string {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "sdk.py"), []byte(content), 0600))
		git(t, dir, "add", "sdk.py")
		git(t, dir, "-c", "user.name=Test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false", "commit", "-m", content)
		return git(t, dir, "rev-parse", "HEAD")
	}
	remote := t.TempDir()
	git(t, remote, "init")
	revision := commit(t, remote, "pinned SDK\n")

	for _, available := range []bool{true, false} {
		name := "fetch missing revision"
		if available {
			name = "use local revision"
		}
		t.Run(name, func(t *testing.T) {
			sdkDir := t.TempDir()
			if available {
				git(t, sdkDir, "clone", remote, ".")
			} else {
				git(t, sdkDir, "init")
			}
			head := commit(t, sdkDir, "development HEAD\n")
			require.NoError(t, os.WriteFile(filepath.Join(sdkDir, "sdk.py"), []byte("uncommitted edits\n"), 0600))
			status := git(t, sdkDir, "status", "--porcelain")

			export := exportRevision(t, sdkDir, remote, revision)
			data, err := os.ReadFile(filepath.Join(export, "sdk.py"))
			require.NoError(t, err)
			require.Equal(t, "pinned SDK\n", string(data))
			require.Equal(t, head, git(t, sdkDir, "rev-parse", "HEAD"))
			require.Equal(t, status, git(t, sdkDir, "status", "--porcelain"))
			data, err = os.ReadFile(filepath.Join(sdkDir, "sdk.py"))
			require.NoError(t, err)
			require.Equal(t, "uncommitted edits\n", string(data))
			if !available {
				err := exec.Command("git", "-C", sdkDir, "cat-file", "-e", revision+"^{commit}").Run()
				require.Error(t, err, "fetch must leave the development repository unchanged")
			}
		})
	}
}
