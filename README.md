ES-Writer
====

Ref https://www.elastic.co/guide/en/elasticsearch/reference/5.2/docs-bulk.html

## URL format

    /path
        ?routing=STRING
        &parent=STRING
        &refresh=true|false
        &wait_for_completion=true|false

## Index

```
{
  "uri":  "/go1_dev/portal/111/_create",
  "body": "{"id":111}"
}
```

## Update

```
{
  "uri":         "/go1_dev/eck_metadata/333/_update",
  "body":        "{"doc":{"field_x":"xxxxx"}}"
}
```

## Update by query

```
{
  "uri":  "/go1_dev/enrolment/_update_by_query",
  "body": {
    "query":{
        "term": {"account.id":333}},
        "script":{
            "inline": "ctx._source.account.managers = params.managers",
            "params":{
                "managers":[1,2,3]
            }
        }
    }
}
```

## Delete

```
{
  "http_method": "DELETE",
  "uri":         "/go1_dev/portal/111"
}
```

## Delete by query

```
{
  "uri":  "/go1_dev/_delete_by_query",
  "body": {
    "query": {
        "query": {
            "term": {
                "status":0
            }
        }
    }
  }
}
```
