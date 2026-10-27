# Development

Install the Go version in [go.mod](../go.mod) or newer. From the repository root:

```sh
make build
export PATH="$PWD/bin:$PATH"
livepeer version
livepeer orchestrator --help
livepeer completion orchestrator bash
```

CI and release metadata use the declared Go version; keep the digest-pinned
builder in `docker/Dockerfile` aligned with it.

## Releases

Set the [VERSION](../VERSION) file to the next `MAJOR.MINOR.PATCH` number without
a `v` prefix, leading zeroes, prerelease suffix, or build metadata. Merge it to
`main`, then run **Release** from GitHub Actions on `main`:

```sh
gh workflow run release.yml --repo livepeer/node --ref main
```

The [Release workflow](../.github/workflows/release.yml) requires repository admin
permission and uses the dispatch commit throughout CI, packaging, tagging,
GitHub publication, and Docker publication.

If a job fails, choose **Re-run failed jobs** in that same run within seven days,
while the tested bundles and images are retained. Matching tags, incomplete
drafts, and published stable releases with identical assets are safe to retry;
conflicting tags or published assets fail without replacement. If artifacts
have expired, rerun all jobs to rebuild before retrying publication.

For tested snapshot bundles without publication, run
[Build](../.github/workflows/build.yml) manually. Eligible PRs also build bundles;
main pushes automatically test and publish snapshot Docker images, retained as
workflow artifacts for one day.

## Checks

```sh
bash .github/scripts/test-release.sh
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

Tests export the pinned committed trees, leaving development checkouts unchanged,
and fetch missing revisions into a temporary repository. To provision fixtures
(requires Git, Python 3.12, and `uv`):

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
coverage can be skipped when checkouts or dependencies are absent; check the
logs before reporting compatibility.

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
