package es_writer

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type RabbitMqInput struct {
	ch   *amqp.Channel
	tags []uint64
}

func (r *RabbitMqInput) messages(flags Container) <-chan amqp.Delivery {
	queue, err := r.ch.QueueDeclare(*flags.QueueName, false, false, false, false, nil, )
	if nil != err {
		logrus.Panic(err)
	}

	err = r.ch.QueueBind(queue.Name, *flags.RoutingKey, *flags.Exchange, true, nil)
	if nil != err {
		logrus.Panic(err)
	}

	messages, err := r.ch.Consume(queue.Name, *flags.ConsumerName, false, false, false, true, nil)
	if nil != err {
		logrus.Panic(err)
	}

	return messages
}

func (r *RabbitMqInput) start(ctx context.Context, flags Container, handler PushCallback) error {
	messages := r.messages(flags)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case m := <-messages:
			bufferMutext.Lock()
			r.onMessage(ctx, m, handler)
			bufferMutext.Unlock()
		}
	}
}

func (r *RabbitMqInput) onMessage(ctx context.Context, m amqp.Delivery, handler PushCallback) {
	if m.DeliveryTag == 0 {
		r.ch.Nack(m.DeliveryTag, false, false)
		return
	}

	// distributed tracing
	carrier := HeaderToTextMapCarrier(m.Headers)
	if spanCtx, err := tracer.Extract(carrier); err == nil {
		opts := []ddtrace.StartSpanOption{
			tracer.ServiceName("explore-indexer"),
			tracer.SpanType("rabbitmq-consumer"),
			tracer.ResourceName("consumer"),
			tracer.Tag("message.routingKey", m.RoutingKey),
			tracer.ChildOf(spanCtx),
		}

		span := tracer.StartSpan("consumer.forwarding", opts...)
		defer span.Finish()
	}

	err, ack, buffer := handler(ctx, m.Body)
	if err != nil {
		logrus.WithError(err).Errorln("Failed to handle new message: " + string(m.Body))
	}

	if ack {
		r.ch.Ack(m.DeliveryTag, false)
	}

	if buffer {
		r.tags = append(r.tags, m.DeliveryTag)
	}
}

func (r *RabbitMqInput) onFlush() {
	for _, deliveryTag := range r.tags {
		r.ch.Ack(deliveryTag, true)
	}

	r.tags = r.tags[:0]
}
