package action

import (
	"gopkg.in/olivere/elastic.v5"
	"strings"
	"fmt"
	"encoding/json"
	"github.com/jmespath/go-jmespath"
)

type Element struct {
	elastic.BulkableRequest

	DeliveryTag uint64

	Method            string      `json:"http_method"`
	Uri               string      `json:"uri"`
	Body              interface{} `json:"body"`
	Routing           string
	Parent            string
	Refresh           bool
	WaitForCompletion bool
	RetryOnConflict   int    // only available for create, update
	Conflict          string // only available for update_by_query
	// wait_for_active_shards

	Request     elastic.BulkableRequest
	Index       string
	DocType     string
	DocId       string
	Version     int64
	VersionType string
}

func (e Element) String() string {
	lines, err := e.Source()
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}

	return strings.Join(lines, "\n")
}

func (e Element) Source() ([]string, error) {
	if strings.HasSuffix(e.Uri, "/_create") {
		body, err := json.Marshal(e.Body)
		if err != nil {
			return nil, err
		}

		return []string{NewCommand(e).String("index"), string(body)}, nil
	} else if strings.HasSuffix(e.Uri, "/_update") {
		var body []byte

		// { "doc": … } || { "script": … }
		for _, key := range []string{"doc", "script"} {
			result, err := jmespath.Search(key, e.Body)
			if result != nil {
				body, err = json.Marshal(e.Body)
				if err != nil {
					return nil, err
				}

				break
			}
		}

		return []string{NewCommand(e).String("update"), string(body)}, nil
	} else if e.Method == "DELETE" {
		return []string{NewCommand(e).String("delete")}, nil
	}

	return nil, fmt.Errorf("unknown request type")
}

func (e *Element) RequestType() string {
	uri := strings.TrimLeft(e.Uri, "/")
	uriChunks := strings.Split(uri, "/")

	// URI pattern: REQUEST /go1_dev
	if len(uriChunks) == 1 {
		if e.Method == "PUT" {
			return "indices_create"
		}

		if e.Method == "DELETE" {
			return "indices_delete"
		}
	}

	if strings.HasSuffix(e.Uri, "/_update_by_query") {
		return "update_by_query"
	}

	if strings.HasSuffix(e.Uri, "/_delete_by_query") {
		return "delete_by_query"
	}

	return "bulkable"
}

func (e *Element) IndicesCreateService(client *elastic.Client) (*elastic.IndicesCreateService, error) {
	req := elastic.NewIndicesCreateService(client)
	req.Index(e.Index)
	req.BodyJson(e.Body)

	return req, nil
}

func (e *Element) IndicesDeleteService(client *elastic.Client) (*elastic.IndicesDeleteService, error) {
	req := elastic.NewIndicesDeleteService(client)
	req.Index([]string{e.Index})

	return req, nil
}

func (e *Element) UpdateByQueryService(client *elastic.Client) (*elastic.UpdateByQueryService, error) {
	req := client.UpdateByQuery(e.Index)

	if e.Routing != "" {
		req.Routing(e.Routing)
	}

	if e.DocType != "" {
		req.Type(e.DocType)
	}

	if e.Refresh {
		req.Refresh("yes")
	}

	if e.Conflict != "" {
		req.Conflicts(e.Conflict)
	}

	q := NewSimpleQuery(e.Body)
	req.Query(q)

	return req, nil
}

func (e *Element) DeleteByQueryService(client *elastic.Client) (*elastic.DeleteByQueryService, error) {
	req := client.DeleteByQuery()

	if e.Routing != "" {
		req.Routing(e.Routing)
	}

	if e.DocType != "" {
		req.Type(e.DocType)
	}

	if e.Refresh {
		req.Refresh("yes")
	}

	if e.Conflict != "" {
		req.Conflicts(e.Conflict)
	}

	q := NewSimpleQuery(e.Body)
	req.Query(q)

	return req, nil
}
