FROM golang:1.26.9-alpine3.24 AS builder
RUN apk update && apk add gcc librdkafka-dev openssl-libs-static zlib-static zstd-libs libsasl lz4-dev lz4-static zstd-static libc-dev musl-dev 
WORKDIR /app
COPY main.go go.mod go.sum ./
RUN go build -tags musl -o main .

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/main .
CMD [ "./main" ]