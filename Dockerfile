FROM alpine:3.7
MAINTAINER Andy Truong <andy@go1.com.au>

RUN apk add --no-cache ca-certificates

COPY . /app
VOLUME ["/app"]
