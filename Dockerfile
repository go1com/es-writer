FROM golang:1.14-alpine

WORKDIR /go/src/github.com/go1com/es-writer/
COPY    . /go/src/github.com/go1com/es-writer/

RUN apk add --no-cache git
RUN GO111MODULE=on go mod vendor
RUN CGO_ENABLED=0 GOOS=linux go build -o /app /go/src/github.com/go1com/es-writer/cmd/main.go

FROM alpine:3.11
RUN apk add --no-cache ca-certificates
COPY --from=0 /app /app
ENTRYPOINT ["/app"]
