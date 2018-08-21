package es_writer

import "flag"

type Flags struct {
	Url           *string
	Kind          *string
	Exchange      *string
	RoutingKey    *string
	PrefetchCount *int
	PrefetchSize  *int
	QueueName     *string
	ConsumerName  *string
}

func NewFlags() Flags {
	o := Flags{}
	o.Url = flag.String("url", "amqp://go1:go1@127.0.0.1:5672/", "")
	o.Kind = flag.String("kind", "topic", "")
	o.Exchange = flag.String("exchange", "events", "")
	o.RoutingKey = flag.String("routing-key", "wip", "")
	o.PrefetchCount = flag.Int("prefetch-count", 50, "")
	o.PrefetchSize = flag.Int("prefetch-size", 0, "")
	o.QueueName = flag.String("queue-name", "a-wip", "")
	o.ConsumerName = flag.String("consumer-name", "wip-rabbit-mq", "")
	flag.Parse()

	return o
}
