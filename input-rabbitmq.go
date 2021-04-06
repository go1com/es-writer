package es_writer

import (
	"context"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type RabbitMqInput struct {
	app  *App
	ch   *amqp.Channel
	tags []uint64
}

func (this *RabbitMqInput) messages(flags Container) <-chan amqp.Delivery {
	args := amqp.Table{}
	if flags.SingleActiveConsumer != nil {
		if *flags.SingleActiveConsumer {
			// @see https://www.rabbitmq.com/consumers.html#single-active-consumer
			args["x-single-active-consumer"] = true
		}
	}

	queue, err := this.ch.QueueDeclare(*flags.QueueName, false, false, false, false, args)
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
			err := this.onMessage(ctx, message, handler)
			bufferMutext.Unlock()

			if nil != err {
				return err
			}
		}
	}
}

func (this *RabbitMqInput) onMessage(ctx context.Context, m amqp.Delivery, handler PushCallback) error {
	if m.DeliveryTag == 0 {
		this.ch.Nack(m.DeliveryTag, false, false)
		return nil
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

	err, ack, buffer, flush := handler(ctx, m.Body)
	if err != nil {
		logrus.WithError(err).Errorln("Failed to handle new message: " + string(m.Body))
		return err
	}

	if ack {
		if err := this.ch.Ack(m.DeliveryTag, false); nil != err {
			return errors.Wrap(err, "failed ack")
		}
	}

	if buffer {
		this.tags = append(this.tags, m.DeliveryTag)
	}

	if flush {
		return this.app.flush(ctx)
	}

	return nil
}

func (this *RabbitMqInput) onFlush() {
	for _, deliveryTag := range this.tags {
		this.ch.Ack(deliveryTag, true)
	}

	this.tags = this.tags[:0]
}
