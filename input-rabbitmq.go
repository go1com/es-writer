package es_writer

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type RabbitMqInput struct {
	app    *App
	logger *zap.Logger
	ch     *amqp.Channel
	tags   []uint64
}

func (this *RabbitMqInput) messages(flags Configuration) <-chan amqp.Delivery {
	args := amqp.Table{}
	if flags.SingleActiveConsumer != nil {
		if *flags.SingleActiveConsumer {
			// @see https://www.rabbitmq.com/consumers.html#single-active-consumer
			args["x-single-active-consumer"] = true
		}
	}

	queue, err := this.ch.QueueDeclare(*flags.QueueName, false, false, false, false, args)
	if nil != err {
		this.logger.Panic("failed to acquire queue", zap.Error(err))
	}

	err = this.ch.QueueBind(queue.Name, *flags.RoutingKey, *flags.Exchange, true, nil)
	if nil != err {
		this.logger.Panic("failed to bind queue", zap.Error(err))
	}

	messages, err := this.ch.Consume(queue.Name, *flags.ConsumerName, false, false, false, true, nil)
	if nil != err {
		this.logger.Panic("failed to getting messages from queue", zap.Error(err))
	}

	return messages
}

func (this *RabbitMqInput) start(ctx context.Context, container Configuration, handler PushCallback) error {
	messages := this.messages(container)
	for {
		var err error
		flush := false

		select {
		case <-ctx.Done():
			return ctx.Err()

		case message := <-messages:
			if err, flush = this.onMessage(ctx, message, handler); nil != err {
				return err
			}

		case <-time.After(*container.TickInterval):
			flush = true
		}

		if flush {
			if err := this.app.flush(ctx); err != nil {
				return err
			}
		}
	}
}

func (this *RabbitMqInput) onMessage(ctx context.Context, m amqp.Delivery, handler PushCallback) (error, bool) {
	if m.DeliveryTag == 0 {
		err := this.ch.Nack(m.DeliveryTag, false, false)

		if nil != err {
			this.logger.Error("failed nack", zap.Error(err))
		}

		return nil, false
	}

	err, ack, buffer, flush := handler(ctx, m)
	if err != nil {
		this.logger.Error(
			"failed to handle new message",
			zap.String("m.routingKey", string(m.RoutingKey)),
			zap.String("m.body", string(m.Body)),
		)

		return err, false
	}

	if ack {
		if err := this.ch.Ack(m.DeliveryTag, false); nil != err {
			return errors.Wrap(err, "failed ack"), false
		}
	}

	if buffer {
		this.tags = append(this.tags, m.DeliveryTag)
	}

	return nil, flush
}

func (this *RabbitMqInput) onFlush() {
	for _, deliveryTag := range this.tags {
		this.ch.Ack(deliveryTag, true)
	}

	this.tags = this.tags[:0]
}
