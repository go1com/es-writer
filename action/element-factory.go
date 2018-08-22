package action

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

func NewElement(deliveryTag uint64, raw []byte) (Element, error) {
	e := Element{}
	err := json.Unmarshal(raw, &e)
	if err != nil {
		return e, fmt.Errorf("failed to parse e: %s", err.Error())
	}

	uri, err := url.Parse(e.Uri)
	if err != nil {
		fmt.Printf("Failed to parse e.uri: %s.\n", err.Error())
	}

	e.Routing = uri.Query().Get("routing")
	e.Parent = uri.Query().Get("parent")
	e.Refresh = uri.Query().Get("refresh") != "false"
	e.WaitForCompletion = uri.Query().Get("wait_for_completion") != "false"

	retryOnConflict := uri.Query().Get("retry_on_conflict")
	if retryOnConflict != "" {
		e.RetryOnConflict, err = strconv.Atoi(retryOnConflict)
		if err != nil {
			return e, err
		}
	}

	conflict := uri.Query().Get("conflict");
	if conflict != "" {
		e.Conflict = conflict
	}

	e.VersionType = uri.Query().Get("version_type")
	version := uri.Query().Get("version")
	if version != "" {
		e.Version, err = strconv.ParseInt(version, 10, 32)
		if err != nil {
			return e, err
		}
	}

	// Parse index, doc-type, doc-id, … from URI
	uriChunks := strings.Split(e.Uri, "/")

	// URI pattern: REQUEST /go1_dev
	if len(uriChunks) == 2 {
		if e.Method == "PUT" {
			e.Index = uriChunks[1] // indices_create
		} else if e.Method == "DELETE" {
			e.Index = uriChunks[1] // indices_delete
		}
	} else {
		if "DELETE" == e.Method {
			e.Index = uriChunks[1] // URI pattern: /go1_dev/portal/111
			e.DocType = uriChunks[2]
			e.DocId = uriChunks[3]
		} else {
			strings.Split(e.Uri, "/")
			switch {
			case strings.HasSuffix(e.Uri, "/_delete_by_query"):
				e.Index = uriChunks[1] // URI pattern: /go1_dev/_delete_by_query

			case strings.HasSuffix(e.Uri, "/_update_by_query"):
				e.Index = uriChunks[1]
				if uriChunks[2] == "_update_by_query" {
					// URI pattern: /go1_dev/_update_by_query
				} else {
					// URI pattern: /go1_dev/enrolment/_update_by_query
					e.DocType = uriChunks[2]
				}

			case strings.HasSuffix(e.Uri, "/_update"):
				e.Index = uriChunks[1] // URI pattern: /go1_dev/eck_metadata/333/_update
				e.DocType = uriChunks[2]
				e.DocId = uriChunks[3]

			case strings.HasSuffix(e.Uri, "/_create"):
				e.Index = uriChunks[1] // URI pattern: /go1_dev/portal/111/_create
				e.DocType = uriChunks[2]
				e.DocId = uriChunks[3]
			}
		}
	}

	return e, nil
}
