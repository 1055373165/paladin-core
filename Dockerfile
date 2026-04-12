FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app/paladin-core ./cmd/paladin-core

FROM alpine:3.19
RUN apk add --no-cache wget
COPY --from=builder /app/paladin-core /app/paladin-core
ENTRYPOINT ["/app/paladin-core"]
