package main

import (
	"go1/es-writer"
	"github.com/Sirupsen/logrus"
	"context"
	w "go1/es-writer/watcher"
	"go1/es-writer/action"
)

func main() {
	ctx := context.Background()
	flags := es_writer.NewFlags()

	// RabbitMQ connection & channel
	// ---------------------
	con, err := w.Connection(*flags.Url)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create watcher connection.")
		return
	} else {
		defer con.Close()
	}

	ch, err := w.Channel(con, *flags.Kind, *flags.Exchange, *flags.PrefetchCount, *flags.PrefetchSize)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create watcher channel.")
		return
	} else {
		defer ch.Close()
	}

	// ElasticSearch connection & bulk-processor
	// ---------------------
	es, bulk, err := action.Clients(ctx, flags)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create ElasticSearch client.")
	} else {
		defer bulk.Close()
	}

	// Watcher: Listen on rabbitMQ and dispatch actions to ElasticSearch
	// ---------------------
	watcher := w.NewWatcher(ch, *flags.PrefetchCount, es, bulk)
	err = watcher.Watch(ctx, flags)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed watching.")
	}
}
