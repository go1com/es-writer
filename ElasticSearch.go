package a_wip

import (
	"context"
	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/olivere/elastic.v5/config"
	"github.com/Sirupsen/logrus"
	"time"
)

func ElasticSearchTools(ctx context.Context) (*elastic.Client, *elastic.BulkProcessor, error) {
	cfg, err := config.Parse("")
	if err != nil {
		logrus.Fatalf("failed to parse URL: %s", err.Error())

		return nil, nil, err
	}

	client, err := elastic.NewClientFromConfig(cfg)
	if err != nil {
		logrus.Fatalf("faield to create client: %s", err.Error())

		return nil, nil, err
	}

	client.UpdateByQuery("").
		Do(ctx)

	processor, err := elastic.
		NewBulkProcessorService(client).
		Name("wip-es").
		Stats(true).
		FlushInterval(2 * time.Second).
		BulkActions(20).
		Do(ctx)
	
	if err != nil {
		logrus.Fatalf("failed to create bulk tools: %s", err.Error())
	}

	return client, processor, err
}
