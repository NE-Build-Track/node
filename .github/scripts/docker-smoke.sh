#!/bin/sh
set -eu

image=$1
version=$2
commit=$3

# Test installed commands, certificate roots, and default writable storage as
# the image's own user, with no compiler, checkout, or external network.
docker run --rm --network none --read-only --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --tmpfs /tmp:rw,mode=1777 \
  --mount type=volume,dst=/home/livepeer/.lpData \
  --entrypoint /bin/sh "$image" -eu -c '
    test "$(id -u)" = 65532
    test "$HOME" = /home/livepeer
    test -w "$HOME/.lpData"
    test -s /etc/ssl/certs/ca-certificates.crt
    if command -v go >/dev/null 2>&1; then
      echo "Smoke environment unexpectedly contains Go" >&2
      exit 1
    fi
    cd /tmp
    expected="livepeer $1 ($2)"
    test "$(livepeer version)" = "$expected"
    for component in orchestrator signer chain discovery; do
      test "$("livepeer-$component" --version)" = "livepeer-$component version $expected"
      test "$(livepeer "$component" --version)" = "livepeer-$component version $expected"
      "livepeer-$component" --help > direct-help
      livepeer "$component" --help > bundled-help
      cmp direct-help bundled-help
      "livepeer-$component" completion bash > direct-completion
      livepeer completion "$component" bash > bundled-completion
      cmp direct-completion bundled-completion
    done
    livepeer orchestrator migrate up
    livepeer signer migrate up
  ' smoke "$version" "$commit"

# Run the normal entrypoint as PID 1. Probe its loopback listener from a helper
# container so the runtime image needs no HTTP client.
for component in orchestrator discovery; do
  case "$component" in
    orchestrator) metrics_port=8936 ;;
    discovery) metrics_port=8940 ;;
  esac
  container=$(docker create --network none --read-only --cap-drop=ALL \
    --security-opt=no-new-privileges \
    --tmpfs /tmp:rw,mode=1777 \
    --mount type=volume,dst=/home/livepeer/.lpData \
    -e LIVEPEER_ORCHESTRATOR_BOOTSTRAP_SECRET=docker-smoke-secret \
    "$image" "$component")
  trap 'docker rm -fv "$container" >/dev/null 2>&1 || true' EXIT
  docker start "$container" >/dev/null
  if ! docker run --rm --network "container:$container" --read-only --cap-drop=ALL \
    --security-opt=no-new-privileges --user 65534:65534 \
    alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 \
    sh -eu -c '
      metrics_port=$1
      attempt=0
      while [ "$attempt" -lt 30 ]; do
        if response=$(wget -q -T 1 -O - "http://127.0.0.1:$metrics_port/readyz") && [ "$response" = ready ]; then
          exit 0
        fi
        attempt=$((attempt + 1))
        sleep 1
      done
      exit 1
    ' probe "$metrics_port"; then
    docker logs "$container" >&2
    exit 1
  fi
  docker stop --timeout 5 "$container" >/dev/null
  if [ "$(docker inspect --format '{{.State.ExitCode}}' "$container")" != 0 ]; then
    docker logs "$container" >&2
    exit 1
  fi
  docker rm -fv "$container" >/dev/null
  trap - EXIT
done
echo "Image $image: metadata, dispatch, completions, migrations, readiness, and shutdown passed."
