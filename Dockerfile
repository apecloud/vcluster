# Build program
FROM golang:1.26 AS builder

WORKDIR /vcluster-dev
ARG TARGETOS
ARG TARGETARCH
ARG BUILD_VERSION=dev
ARG TELEMETRY_PRIVATE_KEY=""
ARG GOPROXY="https://goproxy.cn,direct"
ENV GOPROXY=${GOPROXY}

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
COPY vendor/ vendor/

# Copy the go source
COPY cmd/vcluster cmd/vcluster
COPY cmd/vclusterctl cmd/vclusterctl
COPY pkg/ pkg/
COPY config/ config/

ENV GO111MODULE=on
ENV DEBUG=true

# create and set GOCACHE now, this should slightly speed up the first build inside of the container
# also create /.config folder for GOENV, as dlv needs to write there when starting debugging
RUN mkdir -p /.cache /.config
ENV GOCACHE=/.cache
ENV GOENV=/.config

# Keep the builder runtime's existing root home.
ENV HOME=/

# Build cmd
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
	--mount=type=cache,id=gobuild,target=/.cache/go-build \
	CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} GO111MODULE=on go build -mod vendor -ldflags "-X github.com/loft-sh/vcluster/pkg/telemetry.SyncerVersion=$BUILD_VERSION -X github.com/loft-sh/vcluster/pkg/telemetry.telemetryPrivateKey=$TELEMETRY_PRIVATE_KEY" -o /vcluster cmd/vcluster/main.go

# RUN useradd -u 12345 nonroot
# USER nonroot

ENTRYPOINT ["go", "run", "-mod", "vendor", "cmd/vcluster/main.go", "start"]

# we use alpine for easier debugging
FROM alpine:3.23

# install runtime dependencies
RUN apk upgrade --no-cache zlib && apk add --no-cache ca-certificates zstd tzdata

# Set root path as working directory
WORKDIR /

COPY --from=builder /vcluster .

# RUN useradd -u 12345 nonroot
# USER nonroot

ENTRYPOINT ["/vcluster", "start"]
