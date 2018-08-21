package action

import (
	"encoding/json"
	"fmt"
	"github.com/jmespath/go-jmespath"
	"net/url"
	"strconv"
	"strings"
)

func NewElement(deliveryTag uint64, raw []byte) (*Element, error) {
	m := Element{}
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
