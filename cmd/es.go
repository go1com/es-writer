package main

import (
	"go1/a-wip"
	"context"
	"gopkg.in/olivere/elastic.v5"
	"github.com/Sirupsen/logrus"
)

func main() {
	ctx := context.Background()
	_, bulk, _ := a_wip.ElasticSearchTools(ctx)

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
