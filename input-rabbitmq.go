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

func (this *RabbitMqInput) messages(flags Container) <-chan amqp.Delivery {
	queue, err := this.ch.QueueDeclare(*flags.QueueName, false, false, false, false, nil, )
	if nil != err {
		logrus.Panic(err)
	}

	err = this.ch.QueueBind(queue.Name, *flags.RoutingKey, *flags.Exchange, true, nil)
	if nil != err {
		logrus.Panic(err)
	}

	messages, err := this.ch.Consume(queue.Name, *flags.ConsumerName, false, false, false, true, nil)
	if nil != err {
		logrus.Panic(err)
	}

	return messages
}

func (this *RabbitMqInput) start(ctx context.Context, flags Container, handler PushCallback) error {
	messages := this.messages(flags)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case message := <-messages:
			bufferMutext.Lock()
			this.onMessage(ctx, message, handler)
			bufferMutext.Unlock()
		}
	}
}

func (this *RabbitMqInput) onMessage(ctx context.Context, m amqp.Delivery, handler PushCallback) {
	if m.DeliveryTag == 0 {
		this.ch.Nack(m.DeliveryTag, false, false)
		return
	}

	// distributed tracing
	carrier := HeaderToTextMapCarrier(m.Headers)
	if spanCtx, err := tracer.Extract(carrier); err == nil {
		opts := []ddtrace.StartSpanOption{
			tracer.ServiceName("es-writer"),
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
		this.ch.Ack(m.DeliveryTag, false)
	}

	if buffer {
		this.tags = append(this.tags, m.DeliveryTag)
	}
}

func (this *RabbitMqInput) onFlush() {
	for _, deliveryTag := range this.tags {
		this.ch.Ack(deliveryTag, true)
	}

	this.tags = this.tags[:0]
}
