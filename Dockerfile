FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY . .

RUN go mod tidy

RUN go build -o custom-scheduler .

FROM alpine:latest

COPY --from=builder /app/custom-scheduler /custom-scheduler

CMD ["/custom-scheduler"]
