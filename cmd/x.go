package main

import (
	"encoding/json"
	"github.com/jmespath/go-jmespath"
	"github.com/Sirupsen/logrus"
	"fmt"
	"context"
	"go1/es-writer/action"
	"gopkg.in/olivere/elastic.v5"
	"go1/es-writer"
)

func main() {
	ctx := context.Background()
	flags := es_writer.NewFlags()
	_, bulk, err := action.Clients(ctx, flags)
	if err != nil {
		logrus.WithError(err).Errorln("failed to create ES clients")
	}

	req := elastic.NewBulkDeleteRequest()
	fmt.Println(req)
	return

	req.Index("go1_dev")
	req.Type("portal")
	req.Id("12344")

	bulk.Add(req)

	err = bulk.Flush()
	if err != nil {
		logrus.WithError(err).Errorf("Failed to perform bulk request to Elastic Search")
	}

	if false {
		if false {
			found()
		} else {
			notFound()
		}
	}
}

func found() {
	var x interface{}

	input := `
		{ 
			"script" : { 
				"inline": "ctx._source.counter += params.param1", 
				"lang" : "painless", 
				"params" : {"param1" : 1}
			}, 
			"upsert" : {
				"counter" : 1
			}
		}
`

	json.Unmarshal([]byte(input), &x)

	x2, err := jmespath.Search("script.[inline, lang, params]", x)
	if err != nil {
		logrus.WithError(err).Fatalf("failed to search")
	}

	fmt.Printf("%T: %s | %d\n", x2, x2)
}

func notFound() {
	var x interface{}

	input := `
		{ 
			"doc" : { 
				"inline": "ctx._source.counter += params.param1", 
				"lang" : "painless", 
				"params" : {"param1" : 1}
			}, 
			"upsert" : {
				"counter" : 1
			}
		}
`

	json.Unmarshal([]byte(input), &x)

	_, err := jmespath.Search("script.[inline, lang, params]", x)
	if err != nil {
		logrus.WithError(err).Fatalf("failed to search")
	}
}
