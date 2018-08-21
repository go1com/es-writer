package main

import (
	"encoding/json"
	"github.com/jmespath/go-jmespath"
	"github.com/Sirupsen/logrus"
	"fmt"
)

func main() {
	indicesCreate()
}

func indicesCreate() {
	input := `
		{
			"mappings": {
				"portal": {
					"__parent": { "type": "user" },
					"_routing": { "required": true },
					"properties": {
						"id": { "type": "keyword" }
					}
				}
			}
		}
`

	var raw interface{}
	err := json.Unmarshal([]byte(input), &raw)
	if err != nil {
		logrus.WithError(err).Errorln()
	} else {
		mappings, err := jmespath.Search("mappings", raw)
		if err != nil {
			logrus.WithError(err).Errorln()
		} else {
			fmt.Println(mappings)

			bMapping, err := json.Marshal(mappings)
			fmt.Printf("MAP: %s ERR: %s\n", bMapping, err)

			bRaw, _ := json.Marshal(raw)
			fmt.Printf("RAW: %s", bRaw)
		}
	}
}

func isScript() {
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

func isNotScript() {
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
