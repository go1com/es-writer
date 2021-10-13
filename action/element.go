package action

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jmespath/go-jmespath"
	"gopkg.in/olivere/elastic.v5"
)

type Element struct {
	elastic.BulkableRequest

	Method            string      `json:"http_method"`
	Uri               string      `json:"uri"`
	Body              interface{} `json:"body"`
	Routing           string
	Parent            string
	Refresh           string
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

// Implement elastic.BulkableRequest interface
func (e Element) String() string {
	lines, err := e.Source()
	if err != nil {
		return fmt.Sprintf("error: %v", err)
	}

	return strings.Join(lines, "\n")
}

// Implement elastic.BulkableRequest interface
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
	} else if strings.HasSuffix(e.Uri, "/_aliases") {
		body, err := json.Marshal(e.Body)
		if err != nil {
			return nil, err
		}

		return []string{string(body)}, nil
	} else if e.Method == "DELETE" {
		return []string{NewCommand(e).String("delete")}, nil
	}

	return nil, fmt.Errorf("unknown request type")
}

