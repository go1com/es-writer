package main

import (
	"go1/es-writer"
	"github.com/Sirupsen/logrus"
	"context"
	"go1/es-writer/watcher"
	"go1/es-writer/action"
)

func main() {
	ctx := context.Background()
	flags := es_writer.NewFlags()
	con, err := watcher.Connection(*flags.Url)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create watcher connection.")
		return
	} else {
		defer con.Close()
	}

	ch, err := watcher.Channel(con, *flags.Kind, *flags.Exchange, *flags.PrefetchCount, *flags.PrefetchSize)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create watcher channel.")
		return
	} else {
		defer ch.Close()
	}

	es, bulk, err := action.Clients(ctx, flags)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create ElasticSearch client.")
	} else {
		defer bulk.Close()
	}

	w := watcher.NewWatcher(ch, *flags.PrefetchCount, es, bulk)
	err = w.Watch(ctx, flags)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed watching.")
	}
}
