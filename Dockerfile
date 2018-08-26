FROM golang:1.10

WORKDIR /go/src/github.com/go1com/es-writer/
COPY    . /go/src/github.com/go1com/es-writer/

RUN go get github.com/golang/dep/cmd/dep
RUN pwd; ${GOPATH}/bin/dep ensure
RUN CGO_ENABLED=0 GOOS=linux go build -o /app /go/src/github.com/go1com/es-writer/cmd/main.go



FROM alpine:3.8
LABEL maintainer="Andy Truong <andy@go1.com.au>"

RUN apk add --no-cache ca-certificates
COPY --from=0 /app /app
ENTRYPOINT ["/app"]
