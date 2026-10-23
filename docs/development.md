# Development

The minimum Go version is declared in [go.mod](../go.mod). CI and release metadata
use that version; keep the digest-pinned Go builder in `docker/Dockerfile` aligned
with it. Build all executables with:

```sh
make build
bin/livepeer version
bin/livepeer orchestrator --help
bin/livepeer completion orchestrator bash
```

## Releases

Set the [VERSION](../VERSION) file to the next `MAJOR.MINOR.PATCH` number without
a `v` prefix, leading zeroes, prerelease suffix, or build metadata. Commit it,
then run the release helper with the Git remote name:

```sh
./release.sh github
```

The helper creates and pushes a `vMAJOR.MINOR.PATCH` tag. Release tags cannot be
updated or deleted, and published releases are immutable.
The [Release workflow](../.github/workflows/release.yml) on GitHub publishes
bundles after CI and packaging tests pass. To test packaging before tagging, run
**Release** manually from GitHub Actions; manual runs save artifacts without
publishing a release.

## Checks

```sh
go test -race ./...
go vet ./...
go build ./...
```

[CI](../.github/workflows/ci.yml) also runs Staticcheck and Govulncheck, provisions
the SDK fixtures below, and saves test artifacts. Import boundaries are checked
by `TestImportLattice` in [cmd/livepeer/imports_test.go](../cmd/livepeer/imports_test.go).

## SDK integration fixtures

Integration tests use these pinned revisions:

| SDK | Revision | Environment variable |
| --- | --- | --- |
| Go: `livepeer/golang-runner` | `c3be5a14a91f8a3437133419becaced324d804fe` | `GO_RUNNER_SDK_DIR` |
| Python: `livepeer/livepeer-python-gateway` | `76a1a1c58656ef2ad0c28542ea5f99086b76db69` | `PYTHON_RUNNER_SDK_DIR` |

SDK tests always export the pinned revision, regardless of the checkout's branch
or local edits, and fetch it into a temporary repository if it is missing locally.
To provision fixtures in a temporary directory (requires Git, Python 3.12, and `uv`):

```sh
livepeer_fixture_dir=$(mktemp -d)
export GO_RUNNER_SDK_DIR="$livepeer_fixture_dir/golang-runner"
export PYTHON_RUNNER_SDK_DIR="$livepeer_fixture_dir/livepeer-python-gateway"
git clone https://github.com/livepeer/golang-runner.git "$GO_RUNNER_SDK_DIR"
git -C "$GO_RUNNER_SDK_DIR" checkout --detach c3be5a14a91f8a3437133419becaced324d804fe
git clone https://github.com/livepeer/livepeer-python-gateway.git "$PYTHON_RUNNER_SDK_DIR"
git -C "$PYTHON_RUNNER_SDK_DIR" checkout --detach 76a1a1c58656ef2ad0c28542ea5f99086b76db69
uv sync --project "$PYTHON_RUNNER_SDK_DIR" --locked --no-dev --extra examples --python 3.12
export PYTHON_RUNNER_PYTHON="$PYTHON_RUNNER_SDK_DIR/.venv/bin/python"
go test -race ./...
```

Without explicit paths, tests look for sibling `golang-runner` and
`python-runner` checkouts and the Python checkout's `.venv/bin/python`. SDK
coverage can be skipped when the checkouts or dependencies are absent, so read
the test logs before reporting a compatibility result. Both SDK fixtures use
their pinned committed trees; development checkouts are left unchanged.

The fixtures cover persistent and single-shot sessions, proxy modes, discovery,
runner callbacks, Trickle, and Python media/control round trips. Payment tests
cover live/fixed calls, sustained refresh, payer exposure, concurrent liability,
and crash/reorg/finality handling with deterministic Ethereum interfaces. These
do not replace staging against real contracts.

## CLI and wire snapshots

Refresh CLI help and completion snapshots after an intentional interface change:

```sh
UPDATE_CLI_GOLDENS=1 go test ./cmd/livepeer -run TestRealBinaryOffchainFlow
```

Review the resulting diff. See the [HTTP API reference](http-api.md).
