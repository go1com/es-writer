package action

import (
	"gopkg.in/olivere/elastic.v5"
	"strings"
	"fmt"
)

type Element struct {
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

func (e *Element) RequestType() string {
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

func (e *Element) BuildRequest() (elastic.BulkableRequest, error) {
	if "DELETE" == e.Method {
		req := elastic.NewBulkDeleteRequest()
		req.Routing(e.Routing)
		req.Parent(e.Parent)
		req.Id(e.DocId)
		req.Version(e.Version)

		return req, nil
	}

	if strings.HasSuffix(e.Uri, "/_update") {
		req := elastic.NewBulkUpdateRequest()
		req.Routing(e.Routing)
		req.Parent(e.Parent)
		req.Id(e.DocId)
		req.Version(e.Version)
		req.RetryOnConflict(e.RetryOnConflict)
		req.Doc(e.Body)
		// req.Script()

		return req, nil
	}

	if strings.HasSuffix(e.Uri, "/_create") {
		req := elastic.NewBulkIndexRequest()
		req.Version(e.Version)
		req.VersionType(e.VersionType)
		req.RetryOnConflict(e.RetryOnConflict)

		return req, nil
	}

	return nil, fmt.Errorf("unable to convert to ElasticSearch query")
}
