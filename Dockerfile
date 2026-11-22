# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
COPY migrations ./migrations
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates
USER 65532:65532

FROM runtime AS api
COPY --from=build /out/server /usr/local/bin/server
EXPOSE 8080
ENTRYPOINT ["server"]

FROM runtime AS worker
COPY --from=build /out/worker /usr/local/bin/worker
EXPOSE 9091
ENTRYPOINT ["worker"]

FROM runtime AS scheduler
COPY --from=build /out/scheduler /usr/local/bin/scheduler
ENTRYPOINT ["scheduler"]

FROM runtime AS cli
COPY --from=build /out/cli /usr/local/bin/cli
ENTRYPOINT ["cli"]

FROM api AS server
