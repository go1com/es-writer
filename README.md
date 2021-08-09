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
        -serviceName     SERVICE_NAME<STRING=es-writer>
        -url             RABBITMQ_URL<STRING=amqp://go1:go1@127.0.0.1:5672/>
        -kind            RABBITMQ_KIND<STRING=topic>
        -exchange        RABBITMQ_EXCHANGE<STRING=events>
        -routing-key     RABBITMQ_ROUTING_KEY<STRING=es.writer.go1>
        -consumer-name   RABBITMQ_CONSUMER_NAME<STRING=es-writer>
        -queue-name      RABBITMQ_QUEUE_NAME<STRING=es-writer>
        -url-contains     URL_CONTAINS<STRING>     # example: `award:`
        -url-not-contains URL_NOT_CONTAINS<STRING> # example: `award:`
        -prefetch-count  RABBITMQ_PREFETCH_COUNT<INT=50>
        -es-url          ELASTIC_SEARCH_URL<STRING=http://127.0.0.1:9200/?sniff=false>
        -refresh         true # Optional. allowed values: true, wait_for. default is true
        -admin-port      ADMIN_PORT<STRING=:8001>
        -debug           DEBUG<BOOL=true>. Default is false
        -single-active-consumer SINGLE_ACTIVE_CONSUMER<BOOL=true>. default is false. @see https://www.rabbitmq.com/consumers.html#single-active-consumer
        -bulk-timeout BULK_TIMEOUT=5m

### Notes

Limit these kinds of request because they are not bulkable:

- /_update_by_query
- /_delete_by_query
- PUT /index_name
- DELETE /index_name

### Test local

```
docker run -d -p 9200:9200                --rm --name es       elasticsearch:5-alpine
docker run -d -p 5672:5672 -p 15672:15672 --rm --name rabbitmq rabbitmq:3-management
go test -race -v ./...
```
