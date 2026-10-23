FROM --platform=$BUILDPLATFORM golang:1.27.3@sha256:f20d83a83e18dbd2a4714b8ffd4b30b287b6243962dd75608e86abcc63ba4efc AS build
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOENV=off GOWORK=off \
    GOFLAGS=-mod=readonly GOAMD64=v1 GOARM64=v8.0
WORKDIR /src
COPY go.mod go.sum ./
RUN sha256sum go.mod go.sum > /tmp/modules.sha256 && \
    test "$(go env GOVERSION)" = "go$(awk '$1 == "go" {print $2}' go.mod)" && \
    go mod download && go mod verify && \
    sha256sum -c /tmp/modules.sha256
COPY . .
ARG TARGETOS
ARG TARGETARCH
ARG VERSION
ARG COMMIT=unknown
RUN --mount=type=cache,target=/root/.cache/go-build \
    version="${VERSION:-v$(cat VERSION)-dev}" && \
    GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath -buildvcs=false \
    -ldflags "-s -w -buildid= -X github.com/livepeer/node/cmd/version.Version=${version} -X github.com/livepeer/node/cmd/version.Commit=${COMMIT}" \
    -o /out/ ./cmd/... && \
    sha256sum -c /tmp/modules.sha256 && \
    for binary in /out/*; do \
      go version -m "$binary" | grep -q 'CGO_ENABLED=0' || exit 1; \
    done

FROM scratch AS binaries
COPY --from=build /out/ /

FROM ubuntu:26.04@sha256:f144425ff09be612d6d9ad965196e9cdc23dae1f42110a8a11a3e9a8198759f7
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=binaries / /usr/local/bin/
COPY LICENSE /usr/share/doc/livepeer/LICENSE
RUN groupadd --gid 65532 livepeer && \
    useradd --uid 65532 --gid 65532 --create-home --shell /usr/sbin/nologin livepeer && \
    mkdir /home/livepeer/.lpData && chown 65532:65532 /home/livepeer/.lpData
ENV HOME=/home/livepeer
WORKDIR /home/livepeer
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/livepeer"]
CMD ["--help"]
