package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/go1com/es-writer"
)

func main() {
	ctx := context.Background()
	ctn := es_writer.NewContainer()

	// Credentials can be leaked with debug enabled.
	if *ctn.Debug {
		logrus.Infoln("======= ElasticSearch-App =======")
		logrus.Infof("RabbitMQ URL: %s", *ctn.Url)
		logrus.Infof("RabbitMQ kind: %s", *ctn.Kind)
		logrus.Infof("RabbitMQ exchange: %s", *ctn.Exchange)
		logrus.Infof("RabbitMQ routing key: %s", *ctn.RoutingKey)
		logrus.Infof("RabbitMQ prefetch count: %d", *ctn.PrefetchCount)
		logrus.Infof("RabbitMQ prefetch size: %d", *ctn.PrefetchSize)
		logrus.Infof("RabbitMQ queue name: %s", *ctn.QueueName)
		logrus.Infof("RabbitMQ consumer name: %s", *ctn.ConsumerName)
		logrus.Infof("ElasticSearch URL: %s", *ctn.EsUrl)
		logrus.Infof("Tick interval: %s", *ctn.TickInterval)
		logrus.Infof("URL must contains: %s", *ctn.UrlContain)
		logrus.Infof("URL must not contains: %s", *ctn.UrlNotContain)
		logrus.Infoln("====================================")
		logrus.SetLevel(logrus.DebugLevel)
	}

	app, err, stop := ctn.App()
	if err != nil {
		logrus.
			WithError(err).
			Panicln("failed to get the app")
	}

	defer func() { stop <- true }()

	if ctn.DataDog.Host != "" {
		addr := net.JoinHostPort(ctn.DataDog.Host, ctn.DataDog.Port)

		tracer.Start(
			tracer.WithAgentAddr(addr),
			tracer.WithServiceName(ctn.DataDog.ServiceName),
			tracer.WithGlobalTag("env", ctn.DataDog.Env),
		)

		defer tracer.Stop()
	}

	go app.Run(ctx, ctn)

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)
	<-terminate
	os.Exit(1)
}
