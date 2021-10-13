package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/go1com/es-writer"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	if nil != err {
		panic("failed to create logger: " + err.Error())
	}

	cnf := es_writer.NewConfig(logger)
	if app, err, _ := cnf.App(); err != nil {
		logger.Panic("failed to get the app", zap.Error(err))
	} else {
		ctx := context.Background()
		go debug(ctx, app, &cnf)
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)
	<-terminate
	os.Exit(0)
}

func debug(ctx context.Context, app *es_writer.App, cnf *es_writer.Config) {
	*cnf.QueueName = *cnf.QueueName + "-debug"
	*cnf.SingleActiveConsumer = false
	messages := app.Rabbit.Messages(*cnf)

	for {
		select {
		case <-ctx.Done():
			break

		case m := <-messages:
			fmt.Println(string(m.Body))
			_ = m.Ack(false)
		}
	}
}
