package action

import (
	"gopkg.in/olivere/elastic.v5"
	"strings"
	"fmt"
	"encoding/json"
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

	Request     elastic.BulkableRequest
	Index       string
	DocType     string
	DocId       string
	Version     int64
	VersionType string

	// wait_for_active_shards
}

func (e *Element) String() string {
	lines, err := e.Source()
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}

	return strings.Join(lines, "\n")
}

func (e *Element) Source() ([]string, error) {
	var err error
	var command string
	var body string

	// Build command line
	// ---------------------

	// index
	// update

	// Build body line
	// ---------------------
	if e.Body != nil {
		byteSlice, err := json.Marshal(e.Body)
		if err != nil {
			return nil, err
		}

		body = string(byteSlice)
	}

	return []string{command, body}, err
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
			if 1 == strings.Count(e.Method, "/") {
				return "indices_delete"
			}
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

func (e *Element) BuildIndicesCreateRequest(client *elastic.Client) (*elastic.IndicesCreateService, error) {
	req := elastic.NewIndicesCreateService(client)
	req.Index(e.Index)
	body, err := json.Marshal(e.Body)
	if err != nil {
		return nil, err
	}

	req.Body(string(body))

	return req, nil
}

func (e *Element) BuildIndicesDeleteRequest(client *elastic.Client) (*elastic.IndicesDeleteService, error) {
	req := elastic.NewIndicesDeleteService(client)
	req.Index([]string{e.Index})

	return req, nil
}

func (e *Element) BuildUpdateByQueryRequest(client *elastic.Client) (*elastic.UpdateByQueryService, error) {
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

func (e *Element) BuildDeleteByQueryRequest(client *elastic.Client) (*elastic.DeleteByQueryService, error) {
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
