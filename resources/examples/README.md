Run in Docker
====

### Docker compose


```
# Build binary
# ---------------------
export GOPATH=$HOME/go1/monolith/
cd ~/go1/monolith/src/go1/es-writer/
env GOOS=linux GOARCH=386 go build -o app cmd/main.go

# Build Docker image
# ---------------------
cd ~/go1/monolith/src/go1/es-writer/
docker build -t registry.code.go1.com.au/fn/es-writer:local .

# Start docker-compose
# ---------------------
cd ~/go1/monolith/src/go1/es-writer/resources/examples/
docker-compose up
```

### For debugging

```
docker run -it --rm --link="rabbitmq" --link="elasticsearch" \
    -e RABBITMQ_URL="amqp://go1:go1@rabbitmq:5672/" \
    -e ELASTIC_SEARCH_URL="http://elasticsearch:9200/?sniff=false" \
    -v $HOME"/go1/monolith/src/go1/es-writer:/app" \
    alpine:3.7

> [BASH] /app/app -debug
```
