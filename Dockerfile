FROM golang:1.25.5-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY . .

ARG VERSION=unknown
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X 'main.version=${VERSION}' -X 'main.commit=${COMMIT}' -X 'main.buildTime=${BUILD_TIME}'" \
    -o pgtofu \
    ./cmd/pgtofu

RUN go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.19.0

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 1000 -S pgtofu && \
    adduser -u 1000 -S pgtofu -G pgtofu

WORKDIR /app

COPY --from=builder /build/pgtofu /usr/local/bin/pgtofu
COPY --from=builder /go/bin/migrate /usr/local/bin/migrate

USER pgtofu

ENTRYPOINT ["pgtofu"]
CMD ["--help"]
