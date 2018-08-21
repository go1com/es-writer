package main

import (
	"go1/es-writer"
	"github.com/Sirupsen/logrus"
	"fmt"
)

func main() {
	f := es_writer.NewFlags()
	*f.EsUrl = "http://127.0.0.1:9200/?sniff=false"

	es, err := f.ElasticSearchClient()
	if err != nil {
		logrus.WithError(err).Println("ERROR")
	}

	fmt.Println(es.Start)
}
