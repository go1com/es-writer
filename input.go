package a_wip

import (
	"gopkg.in/olivere/elastic.v5"
	"strings"
	"encoding/json"
	"fmt"
	"github.com/jmespath/go-jmespath"
	"net/url"
	"strconv"
	es "go1/a-wip/elastic"
)

func NewAction(deliveryTag uint64, raw []byte) (*Action, error) {
	m := Action{}
	err := json.Unmarshal(raw, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to parse m: %s", err.Error())
	}

	if _, err := jmespath.Search("id", m.Body); err != nil {
		return nil, fmt.Errorf("failed to parse m.body: %s", err.Error())
	}

	uri, err := url.Parse(m.Uri)
	if err != nil {
		fmt.Printf("Failed to parse m.uri: %s.\n", err.Error())
	}

	m.Routing = uri.Query().Get("routing")
	m.Parent = uri.Query().Get("parent")
	m.Refresh = uri.Query().Get("refresh") != "false"
	m.WaitForCompletion = uri.Query().Get("wait_for_completion") != "false"

	retryOnConflict := uri.Query().Get("retry_on_conflict")
	if retryOnConflict != "" {
		m.RetryOnConflict, err = strconv.Atoi(retryOnConflict)
		if err != nil {
			return nil, err
		}
	}

	conflict := uri.Query().Get("conflict");
	if conflict != "" {
		m.Conflict = conflict
	}

	m.VersionType = uri.Query().Get("version_type")
	version := uri.Query().Get("version")
	if version != "" {
		m.Version, err = strconv.ParseInt(version, 10, 32)
		if err != nil {
			return nil, err
		}
	}

	// Parse index, doc-type, doc-id, … from URI
	uriChunks := strings.Split(m.Uri, "/")

	if "DELETE" == m.Method {
		m.Index = uriChunks[1] // URI pattern: /go1_dev/portal/111
		m.DocType = uriChunks[2]
		m.DocId = uriChunks[3]
	} else {
		strings.Split(m.Uri, "/")
		switch {
		case strings.HasSuffix(m.Uri, "/_delete_by_query"):
			m.Index = uriChunks[1] // URI pattern: /go1_dev/_delete_by_query

		case strings.HasSuffix(m.Uri, "/_update_by_query"):
			m.Index = uriChunks[1]
			if uriChunks[2] == "_update_by_query" {
				// URI pattern: /go1_dev/_update_by_query
			} else {
				// URI pattern: /go1_dev/enrolment/_update_by_query
				m.DocType = uriChunks[2]
			}

		case strings.HasSuffix(m.Uri, "/_update"):
			m.Index = uriChunks[1] // URI pattern: /go1_dev/eck_metadata/333/_update
			m.DocType = uriChunks[2]
			m.DocId = uriChunks[3]

		case strings.HasSuffix(m.Uri, "/_create"):
			m.Index = uriChunks[1] // URI pattern: /go1_dev/portal/111/_create
			m.DocType = uriChunks[2]
			m.DocId = uriChunks[3]
		}
	}

	return &m, nil
}

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

	q := es.NewSimpleQuery(a.Body)
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
