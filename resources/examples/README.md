Run in Docker
====


```
# Build Docker image
# ---------------------
export GOPATH=$HOME/go1/monolith/
cd ~/go1/monolith/src/go1/es-writer/
docker build -t registry.code.go1.com.au/fn/es-writer:local .

# Start docker-compose
# ---------------------
cd ~/go1/monolith/src/go1/es-writer/resources/examples
docker-compose up
```
