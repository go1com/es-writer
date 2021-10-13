package es_writer

import (
	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type RabbitMqInput struct {
	app    *App
	logger *zap.Logger
	ch     *amqp.Channel
	tags   []uint64
}

func (this *RabbitMqInput) Messages(container Config) <-chan amqp.Delivery {
	args := amqp.Table{}
	if container.SingleActiveConsumer != nil {
		if *container.SingleActiveConsumer {
			// @see https://www.rabbitmq.com/consumers.html#single-active-consumer
			args["x-single-active-consumer"] = true
		}
	}

	queue, err := this.ch.QueueDeclare(*container.QueueName, false, false, false, false, args)
	if nil != err {
		this.logger.Panic("failed to acquire queue", zap.Error(err))
	}

	err = this.ch.QueueBind(queue.Name, *container.RoutingKey, *container.Exchange, true, nil)
	if nil != err {
		this.logger.Panic("failed to bind queue", zap.Error(err))
	}

	messages, err := this.ch.Consume(queue.Name, *container.ConsumerName, false, false, false, true, nil)
	if nil != err {
		this.logger.Panic("failed to getting messages from queue", zap.Error(err))
	}

	return messages
}
