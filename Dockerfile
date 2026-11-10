# versitygw + the s3-2q eviction sidecar in one image. s3-2q is PID 1 and
# supervises versitygw (forwarding signals and rotating its access log).
ARG GO_VERSION=1.26

# Build natively and cross-compile (CGO is off), so multi-arch builds do not
# run the Go toolchain under emulation.
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
ARG TARGETOS TARGETARCH
RUN apk add --no-cache git
ARG VERSITYGW_VERSION=v1.11.0
ENV CGO_ENABLED=0 GOTOOLCHAIN=auto GOOS=${TARGETOS} GOARCH=${TARGETARCH}
RUN git clone --depth 1 --branch ${VERSITYGW_VERSION} https://github.com/versity/versitygw /src/versitygw
WORKDIR /src/versitygw
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags "-s -w -X main.Version=${VERSITYGW_VERSION}" -o /out/versitygw ./cmd/versitygw
WORKDIR /src/s3-2q
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/go/pkg/mod GOOS= GOARCH= go mod download
COPY . .
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags "-s -w" -o /out/s3-2q ./cmd/s3-2q

FROM alpine:3
RUN apk add --no-cache ca-certificates && mkdir -p /data /state
COPY --from=build /out/versitygw /out/s3-2q /usr/local/bin/
VOLUME ["/data", "/state"]
EXPOSE 7070 7071
# Set ROOT_ACCESS_KEY / ROOT_SECRET_KEY; both processes read them.
ENTRYPOINT ["s3-2q"]
CMD ["-data-dir", "/data", "-access-log", "/state/access.log", "-snapshot", "/state/index.gz", "-listen", ":7071", \
     "--", "versitygw", "--port", ":7070", "--access-log", "/state/access.log", "posix", "/data"]
