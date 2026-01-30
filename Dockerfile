# syntax=docker/dockerfile:1

# Build stage
FROM golang:1.25.5-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /consumer

# Final stage
FROM alpine:latest
COPY --from=builder /consumer /consumer
CMD ["/consumer"]
