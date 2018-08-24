package main

import (
	"go1/es-writer"
	"github.com/sirupsen/logrus"
	"context"
)

func main() {
	ctx := context.Background()
	flags := es_writer.NewFlags()

	// Credentials can be leak with debug enabled.
	if *flags.Debug {
		logrus.Infoln("======= ElasticSearch-Writer =======")
		logrus.Infof("RabbitMQ URL: %s", *flags.Url)
		logrus.Infof("RabbitMQ kind: %s", *flags.Kind)
		logrus.Infof("RabbitMQ exchange: %s", *flags.Exchange)
		logrus.Infof("RabbitMQ routing key: %s", *flags.RoutingKey)
		logrus.Infof("RabbitMQ prefetch count: %d", *flags.PrefetchCount)
		logrus.Infof("RabbitMQ prefetch size: %d", *flags.PrefetchSize)
		logrus.Infof("RabbitMQ queue name: %s", *flags.QueueName)
		logrus.Infof("RabbitMQ consumer name: %s", *flags.ConsumerName)
		logrus.Infof("ElasticSearch URL: %s", *flags.EsUrl)
		logrus.Infof("Tick interval: %s", *flags.TickInterval)
		logrus.Infoln("")
	}

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
		WithError(dog.Start(ctx, flags)).
		Fatalln("Failed watching.")
}
