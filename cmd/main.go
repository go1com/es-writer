package main

import (
	"go1/es-writer"
	"github.com/sirupsen/logrus"
	"context"
)

func main() {
	ctx := context.Background()
	flags := es_writer.NewFlags()

	// RabbitMQ connection & channel
	// ---------------------
	con, err := flags.RabbitMqConnection()
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create dog connection.")
		return
	} else {
		defer con.Close()
	}

	ch, err := flags.RabbitMqChannel(con)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create dog channel.")
		return
	} else {
		defer ch.Close()
	}

	// ElasticSearch connection & bulk-processor
	// ---------------------
	es, err := flags.ElasticSearchClient()
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create ElasticSearch client.")
	}

	bulk, err := flags.ElasticSearchBulkProcessor(ctx, es)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create ElasticSearch bulk processor.")
	}

	// Dog: Listen on rabbitMQ and dispatch actions to ElasticSearch
	// ---------------------
	dog := es_writer.NewDog(ch, *flags.PrefetchCount, es, bulk, false)
	logrus.
		WithError(dog.Watch(ctx, flags)).
		Fatalln("Failed watching.")
}
