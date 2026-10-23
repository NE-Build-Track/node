package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/livepeer/node/cmd/version"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) == 0 {
		usage()
		return 2
	}
	switch args[0] {
	case "version", "--version", "-v":
		fmt.Println(version.String())
		return 0
	case "--help", "-h", "help":
		if len(args) > 1 {
			return dispatch(args[1], []string{"--help"})
		}
		usage()
		return 0
	case "completion":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: livepeer completion <orchestrator|signer|chain|discovery> <bash|zsh|fish|powershell>")
			return 2
		}
		return dispatch(args[1], append([]string{"completion"}, args[2:]...))
	case "orchestrator", "signer", "chain", "discovery":
		return dispatch(args[0], args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown livepeer command %q\n", args[0])
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprintln(os.Stdout, "Usage: livepeer <orchestrator|signer|chain|discovery|completion|version> [args...]")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Run 'livepeer help <component>' for component options.")
}

// bundledExecutable never searches PATH. This prevents an unrelated program
// named livepeer-* from being executed in place of a bundled component.
func bundledExecutable(component string) (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(self)
	name := "livepeer-" + component
	for _, candidate := range []string{filepath.Join(dir, name), filepath.Join(dir, "libexec", name), filepath.Join(dir, "..", "libexec", name)} {
		info, err := os.Stat(candidate)
		if err == nil && info.Mode().IsRegular() && info.Mode()&0111 != 0 {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("bundled executable %s not found beside livepeer or in its libexec directory", name)
}

func dispatch(component string, args []string) int {
	switch component {
	case "orchestrator", "signer", "chain", "discovery":
	default:
		fmt.Fprintf(os.Stderr, "unknown livepeer component %q\n", component)
		return 2
	}
	path, err := bundledExecutable(component)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 127
	}
	if err := syscall.Exec(path, append([]string{path}, args...), os.Environ()); err != nil {
		// syscall.Exec is the normal path: PID, standard streams, signals and
		// exit status become those of the component. Fallback is for systems
		// without working exec semantics in their launcher.
		cmd := exec.Command(path, args...)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		if runErr := cmd.Run(); runErr != nil {
			if exit, ok := runErr.(*exec.ExitError); ok {
				return exit.ExitCode()
			}
			fmt.Fprintln(os.Stderr, runErr)
			return 126
		}
	}
	return 0
}
