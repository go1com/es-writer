package es_writer

import (
	"testing"
	"context"
	"github.com/streadway/amqp"
	"io/ioutil"
	"runtime"
	"path"
	"github.com/Sirupsen/logrus"
	"time"
)

func TestFlags(t *testing.T) {
	ctx := context.Background()
	f := NewFlags()
	con, err := f.RabbitMqConnection()
	if err != nil {
		t.Fatalf("failed to make rabbitMQ connection: %s", err.Error())
	} else {
		defer con.Close()
	}

	ch, err := f.RabbitMqChannel(con)
	if err != nil {
		t.Fatalf("failed to make rabbitMQ channel: %s", err.Error())
	} else {
		defer ch.Close()
	}

	es, err := f.ElasticSearchClient()
	if err != nil {
		t.Fatalf("failed to make ElasticSearch client: %s", err.Error())
	}

	bulk, err := f.ElasticSearchBulkProcessor(ctx, es)
	if err != nil {
		t.Fatalf("failed to make ElasticSearch bulk processor: %s", err.Error())
	} else {
		defer bulk.Close()
	}
}

func queue(ch *amqp.Channel, f Flags, file string) {
	err := ch.Publish(*f.Exchange, *f.RoutingKey, false, false, amqp.Publishing{
		Body: realpath(file),
	})

	if err != nil {
		logrus.WithError(err).Panicln("failed to publish message")
	}
}

func realpath(filePath string) []byte {
	_, currentFileName, _, _ := runtime.Caller(1)
	filePath = path.Dir(currentFileName) + "/fixtures/" + filePath
	body, _ := ioutil.ReadFile(filePath)

	return body
}
