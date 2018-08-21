package es_writer

import (
	"flag"
	"time"
	"github.com/streadway/amqp"
	"github.com/Sirupsen/logrus"
	"fmt"
	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/olivere/elastic.v5/config"
	"context"
)

type Flags struct {
	Url           *string
	Kind          *string
	Exchange      *string
	RoutingKey    *string
	PrefetchCount *int
	PrefetchSize  *int
	TickInterval  *time.Duration
	QueueName     *string
	ConsumerName  *string
	EsUrl         *string
}

func NewFlags() Flags {
	f := Flags{}
	f.Url = flag.String("url", "amqp://go1:go1@127.0.0.1:5672/", "")
	f.Kind = flag.String("kind", "topic", "")
	f.Exchange = flag.String("exchange", "events", "")
	f.RoutingKey = flag.String("routing-key", "wip", "")
	f.PrefetchCount = flag.Int("prefetch-count", 50, "")
	f.PrefetchSize = flag.Int("prefetch-size", 0, "")
	f.TickInterval = flag.Duration("tick-iterval", 5*time.Second, "")
	f.QueueName = flag.String("queue-name", "a-wip", "")
	f.ConsumerName = flag.String("consumer-name", "wip-rabbit-mq", "")
	f.EsUrl = flag.String("es-url", "http://127.0.0.1:9200", "")
	flag.Parse()

	return f
}

func (f *Flags) RabbitMqConnection() (*amqp.Connection, error) {
	con, err := amqp.Dial(*f.Url)
	if nil != err {
		return nil, err
	}

	go func() {
		conCloseChan := con.NotifyClose(make(chan *amqp.Error))

		select
		{
		case err := <-conCloseChan:
			logrus.WithError(err).Panicln("RabbitMQ connection error.")
		}
	}()

	return con, nil
}

func (f *Flags) RabbitMqChannel(con *amqp.Connection) (*amqp.Channel, error) {
	ch, err := con.Channel()
	if nil != err {
		return nil, err
	}

	if "topic" != *f.Kind && "direct" != *f.Kind {
		ch.Close()

		return nil, fmt.Errorf("unsupported channel kind: %s", *f.Kind)
	}

	err = ch.ExchangeDeclare(*f.Exchange, *f.Kind, false, false, false, false, nil)
	if nil != err {
		ch.Close()

		return nil, err
	}

	err = ch.Qos(*f.PrefetchCount, *f.PrefetchSize, false)
	if nil != err {
		ch.Close()

		return nil, err
	}

	return ch, nil
}

func (f *Flags) ElasticSearchClient() (*elastic.Client, error) {
	cfg, err := config.Parse(*f.EsUrl)
	if err != nil {
		logrus.Fatalf("failed to parse URL: %s", err.Error())

		return nil, err
	}

	client, err := elastic.NewClientFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (f *Flags) ElasticSearchBulkProcessor(ctx context.Context, client *elastic.Client) (*elastic.BulkProcessor, error) {
	processor, err := elastic.
		NewBulkProcessorService(client).
		Name("es-writter").
		Stats(true).
		FlushInterval(2 * time.Second).
		BulkActions(20).
		Do(ctx)

	if err != nil {
		return nil, err
	}

	return processor, nil
}
