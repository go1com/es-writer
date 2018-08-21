package action

import (
	"context"
	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/olivere/elastic.v5/config"
	"github.com/Sirupsen/logrus"
	"time"
	"go1/es-writer"
)

func Clients(ctx context.Context, flags es_writer.Flags) (*elastic.Client, *elastic.BulkProcessor, error) {
	cfg, err := config.Parse(*flags.EsUrl)
	if err != nil {
		logrus.Fatalf("failed to parse URL: %s", err.Error())

		return nil, nil, err
	}

	client, err := elastic.NewClientFromConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	
	processor, err := elastic.
		NewBulkProcessorService(client).
		Name("es-writter").
		Stats(true).
		FlushInterval(2 * time.Second).
		BulkActions(20).
		Do(ctx)

	if err != nil {
		logrus.Fatalf("failed to create bulk tools: %s", err.Error())

		return nil, nil, err
	}

	return client, processor, nil
}
