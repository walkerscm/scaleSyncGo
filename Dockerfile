FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /scalesync ./cmd/scalesync

FROM alpine:3.20
RUN apk --no-cache add ca-certificates
COPY --from=builder /scalesync /usr/local/bin/scalesync

ENTRYPOINT ["scalesync"]
