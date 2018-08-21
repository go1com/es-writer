package main

import (
	"encoding/json"
	"github.com/jmespath/go-jmespath"
	"github.com/Sirupsen/logrus"
	"fmt"
)

func main() {
	if false {
		found()
	} else {
		notFound()
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
