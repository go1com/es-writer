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

func NewInputMessage(raw []byte) (*InputMessage, error) {
	m := InputMessage{}
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

type InputMessage struct {
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

func (m *InputMessage) Bulkable() bool {
	if strings.HasSuffix(m.Uri, "/_update_by_query") {
		return false
	}

	if strings.HasSuffix(m.Uri, "/_delete_by_query") {
		return false
	}

	return true
}

func (m *InputMessage) BuildUpdateByQueryRequest(client *elastic.Client) (*elastic.UpdateByQueryService, error) {
	req := client.UpdateByQuery(m.Index)

	if m.Routing != "" {
		req.Routing(m.Routing)
	}

	if m.DocType != "" {
		req.Type(m.DocType)
	}

	if m.Refresh {
		req.Refresh("yes")
	}

	if m.Conflict != "" {
		req.Conflicts(m.Conflict)
	}

	q := es.NewSimpleQuery(m.Body)
	req.Query(q)

	return req, nil
}

func (m *InputMessage) BuildRequest() (elastic.BulkableRequest, error) {
	if "DELETE" == m.Method {
		req := elastic.NewBulkDeleteRequest()
		req.Routing(m.Routing)
		req.Parent(m.Parent)
		req.Id(m.DocId)
		req.Version(m.Version)

		return req, nil
	}

	if strings.HasSuffix(m.Uri, "/_update") {
		req := elastic.NewBulkUpdateRequest()
		req.Routing(m.Routing)
		req.Parent(m.Parent)
		req.Id(m.DocId)
		req.Version(m.Version)
		req.RetryOnConflict(m.RetryOnConflict)
		req.Doc(m.Body)
		// req.Script()

		return req, nil
	}

	if strings.HasSuffix(m.Uri, "/_create") {
		req := elastic.NewBulkIndexRequest()
		req.Version(m.Version)
		req.VersionType(m.VersionType)
		req.RetryOnConflict(m.RetryOnConflict)

		return req, nil
	}

	return nil, fmt.Errorf("unable to convert to ElasticSearch query")
}
