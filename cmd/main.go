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
	logger, err := zap.NewProduction()
	if nil != err {
		panic("failed to create logger: " + err.Error())
	}

	ctx := context.Background()
	cnf := es_writer.NewConfiguration(logger)

	// Credentials can be leaked with debug enabled.
	if *cnf.Debug {
		logger = logger.WithOptions(zap.IncreaseLevel(zap.DebugLevel))

		logger.Info("starting es-writer",
			zap.String("service", cnf.DataDog.ServiceName),
			zap.String("rabbitmq.url", *cnf.Url),
			zap.String("rabbitmq.kind", *cnf.Kind),
			zap.String("rabbitmq.exchange", *cnf.Exchange),
			zap.String("rabbitmq.routingKey", *cnf.RoutingKey),
			zap.Int("rabbitmq.prefetchCount", *cnf.PrefetchCount),
			zap.Int("rabbitmq.prefetchSize", *cnf.PrefetchSize),
			zap.String("rabbitmq.queueName", *cnf.QueueName),
			zap.String("rabbitmq.consumerName", *cnf.ConsumerName),
			zap.String("elasticSearch.url", *cnf.EsUrl),
			zap.Duration("tickInterval", *cnf.TickInterval),
			zap.String("url.contains", *cnf.UrlContain),
			zap.String("url.notContains", *cnf.UrlNotContain),
		)
	}

	if cnf.DataDog.Host != "" {
		addr := net.JoinHostPort(cnf.DataDog.Host, cnf.DataDog.Port)

		tracer.Start(
			tracer.WithAgentAddr(addr),
			tracer.WithServiceName(cnf.DataDog.ServiceName),
			tracer.WithGlobalTag("env", cnf.DataDog.Env),
		)

		defer tracer.Stop()
	}

	app, err, onErrorCh := cnf.App()
	if err != nil {
		logger.Panic("failed to get the app", zap.Error(err))
	}

	go func() {
		if err := app.Run(ctx, cnf); err != nil {
			logger.Panic("application error", zap.Error(err))
			onErrorCh <- true
		}
	}()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)
	<-terminate
	os.Exit(1)
}
