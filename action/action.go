package action

import (
	"gopkg.in/olivere/elastic.v5"
	"strings"
	"fmt"
)

type Action struct {
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

func (a *Action) Bulkable() string {
	if strings.HasSuffix(a.Uri, "/_update_by_query") {
		return "_update_by_query"
	}

	if strings.HasSuffix(a.Uri, "/_delete_by_query") {
		return "_delete_by_query"
	}

	return "bulkable"
}

func (a *Action) BuildUpdateByQueryRequest(client *elastic.Client) (*elastic.UpdateByQueryService, error) {
	req := client.UpdateByQuery(a.Index)

	if a.Routing != "" {
		req.Routing(a.Routing)
	}

	if a.DocType != "" {
		req.Type(a.DocType)
	}

	if a.Refresh {
		req.Refresh("yes")
	}

	if a.Conflict != "" {
		req.Conflicts(a.Conflict)
	}

	q := es.NewSimpleQuery(a.Body)
	req.Query(q)

	return req, nil
}

func (a *Action) BuildDeleteByQueryRequest(client *elastic.Client) (*elastic.DeleteByQueryService, error) {
	req := client.DeleteByQuery()

	if a.Routing != "" {
		req.Routing(a.Routing)
	}

	if a.DocType != "" {
		req.Type(a.DocType)
	}

	if a.Refresh {
		req.Refresh("yes")
	}

	if a.Conflict != "" {
		req.Conflicts(a.Conflict)
	}

	q := NewSimpleQuery(a.Body)
	req.Query(q)

	return req, nil
}

func (a *Action) BuildRequest() (elastic.BulkableRequest, error) {
	if "DELETE" == a.Method {
		req := elastic.NewBulkDeleteRequest()
		req.Routing(a.Routing)
		req.Parent(a.Parent)
		req.Id(a.DocId)
		req.Version(a.Version)

		return req, nil
	}

	if strings.HasSuffix(a.Uri, "/_update") {
		req := elastic.NewBulkUpdateRequest()
		req.Routing(a.Routing)
		req.Parent(a.Parent)
		req.Id(a.DocId)
		req.Version(a.Version)
		req.RetryOnConflict(a.RetryOnConflict)
		req.Doc(a.Body)
		// req.Script()

		return req, nil
	}

	if strings.HasSuffix(a.Uri, "/_create") {
		req := elastic.NewBulkIndexRequest()
		req.Version(a.Version)
		req.VersionType(a.VersionType)
		req.RetryOnConflict(a.RetryOnConflict)

		return req, nil
	}

	return nil, fmt.Errorf("unable to convert to ElasticSearch query")
}
