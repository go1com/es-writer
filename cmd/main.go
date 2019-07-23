package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/go1com/es-writer"
)

func main() {
	ctx := context.Background()
	f := es_writer.NewFlags()

	// Credentials can be leaked with debug enabled.
	if *f.Debug {
		logrus.Infoln("======= ElasticSearch-App =======")
		logrus.Infof("RabbitMQ URL: %s", *f.Url)
		logrus.Infof("RabbitMQ kind: %s", *f.Kind)
		logrus.Infof("RabbitMQ exchange: %s", *f.Exchange)
		logrus.Infof("RabbitMQ routing key: %s", *f.RoutingKey)
		logrus.Infof("RabbitMQ prefetch count: %d", *f.PrefetchCount)
		logrus.Infof("RabbitMQ prefetch size: %d", *f.PrefetchSize)
		logrus.Infof("RabbitMQ queue name: %s", *f.QueueName)
		logrus.Infof("RabbitMQ consumer name: %s", *f.ConsumerName)
		logrus.Infof("ElasticSearch URL: %s", *f.EsUrl)
		logrus.Infof("Tick interval: %s", *f.TickInterval)
		logrus.Infof("URL must contains: %s", *f.UrlContain)
		logrus.Infof("URL must not contains: %s", *f.UrlNotContain)
		logrus.Infoln("====================================")
		logrus.SetLevel(logrus.DebugLevel)
	}

	app, err, stop := f.App()
	if err != nil {
		logrus.
			WithError(err).
			Panicln("failed to get the app")
	}

	defer func() { stop <- true }()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)

	go es_writer.StartPrometheusServer(*f.AdminPort)
	app.Start(ctx, f, terminate)

	os.Exit(1)
}
