Elastic Search writer [![Build Status](https://travis-ci.org/go1com/es-writer.svg?branch=master)](https://travis-ci.org/go1com/es-writer)
====

## Problems

It's very easy to have conflict when we have multiple services writing data into same Elastic Search server.

To avoid this problem, the service should publish message to a certain instead of writing to ES direclty. So that we can have ES-Writer, a single actor that connect with Elastic Search.

By this convention, the services doesn't need to know credentials of Elastic Search server.

It's also easy to create cluster Elastic Search servers without magic.

## Usage

### Queueing

To write data to ES server, your service publishes messages to `GO1` RabbitMQ with:

- **Exchange:** `$exchange`
- **Key:** `$routingKey`
- **Payload:** `OBJECT`
    - Examples:
        - fixtures/indices/indices-create.json
        - fixtures/indices/indices-drop.json
        - fixtures/portal/portal-index.json
        - fixtures/portal/portal-update.json
        - fixtures/portal/portal-update-by-query.json
        - fixtures/portal/portal-delete.json
        - fixtures/portal/portal-delete-by-query.json

Ref https://www.elastic.co/guide/en/elasticsearch/reference/5.2/docs-bulk.html

### Consuming

    /path/to/es-writer
        -refresh         true # Optional. allowed values: true, wait_for. default is true

### Notes

Limit these kinds of request because they are not bulkable:

- /_update_by_query
- /_delete_by_query
- PUT /index_name
- DELETE /index_name

### Test local

```
docker run -d -p 9200:9200                --rm --name es       elasticsearch:5-alpine
docker run --rm -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER='go1' -e RABBITMQ_DEFAULT_PASS='go1' rabbitmq:3-management
go test -race -v ./...
```
