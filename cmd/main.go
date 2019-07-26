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
	cnfPath := os.Getenv("CONSUMER_CONFIG_YAML_FILE")
	cnf, err := es_writer.NewConfigurationFromYamlFile(cnfPath)
	if err != nil {
		logrus.
			WithError(err).
			Panicln("failed to get app configuration")
	}

	verboseConfig(cnf) // Credentials can be leaked with debug enabled.

	app, err, stop := cnf.App()
	if err != nil {
		logrus.
			WithError(err).
			Panicln("failed to get the app")
	}

	defer func() { stop <- true }()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)

	go es_writer.StartPrometheusServer(cnf.AdminPort)
	ctx := context.Background()
	app.Start(ctx, cnf, terminate)

	os.Exit(1)
}

func verboseConfig(cnf *es_writer.Configuration) {
	if cnf.Debug {
		logrus.Infoln("======= ElasticSearch-App =======")
		logrus.Infof("RabbitMQ URL: %s", cnf.RabbitMq.Url)
		logrus.Infof("RabbitMQ kind: %s", cnf.RabbitMq.Kind)
		logrus.Infof("RabbitMQ exchange: %s", cnf.RabbitMq.Exchange)
		logrus.Infof("RabbitMQ routing key: %s", cnf.RabbitMq.RoutingKey)
		logrus.Infof("RabbitMQ prefetch count: %d", cnf.RabbitMq.PrefetchCount)
		logrus.Infof("RabbitMQ prefetch size: %d", cnf.RabbitMq.PrefetchCount)
		logrus.Infof("RabbitMQ queue name: %s", cnf.RabbitMq.QueueName)
		logrus.Infof("RabbitMQ consumer name: %s", cnf.RabbitMq.ConsumerName)
		logrus.Infof("ElasticSearch URL: %s", cnf.ElasticSearch.Url)
		logrus.Infof("Tick interval: %s", cnf.TickInterval)
		logrus.Infof("URL must contains: %s", cnf.Filter.UrlContains)
		logrus.Infof("URL must not contains: %s", cnf.Filter.UrlNotContains)
		logrus.Infoln("====================================")
		logrus.SetLevel(logrus.DebugLevel)
	}
}
