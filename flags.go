package es_writer

import (
	"flag"
	"time"
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
