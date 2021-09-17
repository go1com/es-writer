package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/go1com/es-writer"
)

func main() {
	ctx := context.Background()
	ctn := es_writer.NewContainer()

	// Credentials can be leaked with debug enabled.
	ctn.Logger.Debug("starting es-writer",
		zap.String("service", ctn.DataDog.ServiceName),
		zap.String("rabbitmq.url", *ctn.Url),
		zap.String("rabbitmq.kind", *ctn.Kind),
		zap.String("rabbitmq.exchange", *ctn.Exchange),
		zap.String("rabbitmq.routingKey", *ctn.RoutingKey),
		zap.Int("rabbitmq.prefetchCount", *ctn.PrefetchCount),
		zap.Int("rabbitmq.prefetchSize", *ctn.PrefetchSize),
		zap.String("rabbitmq.queueName", *ctn.QueueName),
		zap.String("rabbitmq.consumerName", *ctn.ConsumerName),
		zap.String("elasticSearch.url", *ctn.EsUrl),
		zap.Duration("tickInterval", *ctn.TickInterval),
		zap.String("url.contains", *ctn.UrlContain),
		zap.String("url.notContains", *ctn.UrlNotContain),
	)

	if ctn.DataDog.Host != "" {
		addr := net.JoinHostPort(ctn.DataDog.Host, ctn.DataDog.Port)

		tracer.Start(
			tracer.WithAgentAddr(addr),
			tracer.WithServiceName(ctn.DataDog.ServiceName),
			tracer.WithGlobalTag("env", ctn.DataDog.Env),
		)

		defer tracer.Stop()
	}

	app, err, onErrorCh := ctn.App()
	if err != nil {
		ctn.Logger.Panic("failed to get the app", zap.Error(err))
	}

	go func() {
		if err := app.Run(ctx, ctn); err != nil {
			ctn.Logger.Panic("application error", zap.Error(err))
			onErrorCh <- true
		}
	}()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)
	<-terminate
	os.Exit(1)
}
