package main

import (
	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/olivere/elastic.v5/config"
	"github.com/Sirupsen/logrus"
	"time"
	"context"
	"fmt"
	"encoding/json"
	"github.com/jmespath/go-jmespath"
	"net/url"
	"strings"
)

type WritingMessage struct {
	Method            string      `json:"http_method"`
	Uri               string      `json:"uri"`
	Body              interface{} `json:"body"`
	Routing           string
	Parent            string
	Refresh           bool
	WaitForCompletion bool

	Request elastic.BulkableRequest
	Index   string
	DocType string
	DocId   string

	// wait_for_active_shards
	// update: _retry_on_conflict
}

func NewWritingMessage(raw []byte) (*WritingMessage, error) {
	msg := WritingMessage{}
	err := json.Unmarshal(raw, &msg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse msg: %s", err.Error())
	}

	if _, err := jmespath.Search("id", msg.Body); err != nil {
		return nil, fmt.Errorf("failed to parse msg.body: %s", err.Error())
	}

	uri, err := url.Parse(msg.Uri)
	if err != nil {
		fmt.Printf("Failed to parse msg.uri: %s.\n", err.Error())
	}

	msg.Routing = uri.Query().Get("routing")
	msg.Parent = uri.Query().Get("parent")
	msg.Refresh = uri.Query().Get("refresh") != "false"
	msg.WaitForCompletion = uri.Query().Get("wait_for_completion") != "false"

	// set request type
	if "DELETE" == msg.Method {
		msg.Request = elastic.NewBulkDeleteRequest()
	} else {
		strings.Split(msg.Uri, "/")
		switch {
		case strings.HasSuffix(msg.Uri, "/_delete_by_query"):
			msg.Request = elastic.NewBulkDeleteRequest() // TODO
		case strings.HasSuffix(msg.Uri, "/_update_by_query"):
			msg.Request = elastic.NewBulkUpdateRequest() // TODO
		case strings.HasSuffix(msg.Uri, "/_update"):
			msg.Request = elastic.NewBulkUpdateRequest()
		case strings.HasSuffix(msg.Uri, "/_create"):
			msg.Request = elastic.NewBulkIndexRequest()
		}
	}

	return &msg, nil
}

func main() {
	var input []byte

	if true {
		input = []byte(`
			{
				"http_method": "POST", 
				"uri":  "/go1_dev/portal/111/_create?routing=go1_dev&parent=333", 
				"body": {
					"id": 111
				}
			}
`)
		msg, err := NewWritingMessage(input)
		if err != nil {
			return
		}

		if msg.Refresh {

		}

		return
	}

	requests := []string{
		"",
		"",
		"",
	}

	ctx := context.Background()
	processor, _ := processor(ctx)
	for _, request := range requests {
		if false {
			fmt.Println(request)
		}

		req := elastic.
			NewBulkIndexRequest().
			Type("").
			Id("").
			Doc(struct{}{})

		elastic.
			NewBulkIndexRequest()

		processor.Add(req)
	}
}

func processor(ctx context.Context) (*elastic.BulkProcessor, error) {
	cfg, err := config.Parse("")
	if err != nil {
		logrus.Fatalf("failed to parse URL: %s", err.Error())

		return nil, err
	}

	client, err := elastic.NewClientFromConfig(cfg)
	if err != nil {
		logrus.Fatalf("faield to create client: %s", err.Error())

		return nil, err
	}

	processor, err := elastic.
		NewBulkProcessorService(client).
		Name("wip-es").
		Stats(true).
		FlushInterval(2 * time.Second).
		BulkActions(20).
		Do(ctx)

	if err != nil {
		logrus.Fatalf("failed to create bulk processor: %s", err.Error())
	}

	return processor, err
}
