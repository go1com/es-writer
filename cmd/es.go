package main

import (
	"context"
	"gopkg.in/olivere/elastic.v5"
	"github.com/Sirupsen/logrus"
	"go1/es-writer/action"
)

func main() {
	ctx := context.Background()
	_, bulk, _ := action.Clients(ctx)

	req := elastic.NewBulkDeleteRequest()
	req.Index("go1_dev")
	req.Type("portal")
	req.Id("12344")

	bulk.Add(req)
	err := bulk.Flush()

	if err != nil {
		logrus.WithError(err).Errorf("Failed to perform bulk request to Elastic Search")
	}
}
