#!/bin/sh
set -eu

archive=$1
version=$2
commit=$3
bundle=$(basename "$archive" .tar.gz)

# This script runs in a fresh, offline Alpine container with only the archive
# and this script mounted. No checkout, SDK, Go compiler, or module cache is present.
if command -v go >/dev/null 2>&1; then
  echo 'Smoke environment unexpectedly contains Go' >&2
  exit 1
fi
cd "$(dirname "$archive")"
sha256sum -c "$bundle.tar.gz.sha256"
mkdir -p /tmp/unpack /tmp/install/bin /tmp/home /tmp/work
tar -xzf "$archive" -C /tmp/unpack
cd "/tmp/unpack/$bundle"
sha256sum -c SHA256SUMS
cp bin/* /tmp/install/bin/

# Remove the extracted tree so dispatch can only use the installed siblings.
cd /tmp/work
rm -rf /tmp/unpack
export HOME=/tmp/home
export PATH=/tmp/install/bin:/usr/bin:/bin
unset GOROOT GOPATH GOTOOLCHAIN GOENV GOWORK GOFLAGS

test "$(livepeer version)" = "livepeer $version ($commit)"
for component in orchestrator signer chain discovery; do
  "livepeer-$component" --version | grep -F "livepeer $version ($commit)"
  livepeer "$component" --version | grep -F "livepeer $version ($commit)"
  "livepeer-$component" --help > direct-help
  livepeer "$component" --help > bundled-help
  cmp direct-help bundled-help
  "livepeer-$component" completion bash > direct-completion
  livepeer completion "$component" bash > bundled-completion
  cmp direct-completion bundled-completion
done

# Exercise embedded SQLite migrations in the installed cgo-free binaries.
livepeer orchestrator migrate --data-dir /tmp/work/orchestrator up
livepeer signer migrate --data-dir /tmp/work/signer up

# Check readiness and clean shutdown for each installed service.
smoke_service() {
  component=$1 metrics_port=$2
  shift 2
  livepeer "$component" "$@" > "$component.log" 2>&1 &
  pid=$!
  trap 'kill "$pid" 2>/dev/null || true' EXIT
  ready=false
  attempt=0
  while [ "$attempt" -lt 30 ]; do
    attempt=$((attempt + 1))
    if wget -q -O ready "http://127.0.0.1:$metrics_port/readyz" && grep -qx ready ready; then
      ready=true
      break
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  if [ "$ready" != true ]; then
    cat "$component.log" >&2
    exit 1
  fi
  if [ "$component" = discovery ]; then
    wget -q -O discovery.json http://127.0.0.1:8939/discovery
    grep -Fxq '[]' discovery.json
  fi
  kill -TERM "$pid"
  wait "$pid"
  trap - EXIT
}

printf 'release-smoke-secret' > bootstrap-secret
chmod 600 bootstrap-secret
smoke_service orchestrator 8936 --data-dir /tmp/work/orchestrator \
  --bootstrap-secret-file /tmp/work/bootstrap-secret \
  --listen 127.0.0.1:8935 --metrics-listen 127.0.0.1:8936 \
  --service-url http://127.0.0.1:8935
smoke_service discovery 8940 --listen 127.0.0.1:8939 --metrics-listen 127.0.0.1:8940
echo "Installed $bundle: metadata, dispatch, completions, migrations, and readiness passed without Go."
