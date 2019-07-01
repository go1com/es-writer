package es_writer

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type RabbitMqInput struct {
	ch   *amqp.Channel
	tags []uint64
}

func (w *Dog) messages(flags Flags) <-chan amqp.Delivery {
	queue, err := w.rabbit.ch.QueueDeclare(*flags.QueueName, false, false, false, false, nil, )
	if nil != err {
		logrus.Panic(err)
	}

	err = w.rabbit.ch.QueueBind(queue.Name, *flags.RoutingKey, *flags.Exchange, true, nil)
	if nil != err {
		logrus.Panic(err)
	}

	messages, err := w.rabbit.ch.Consume(queue.Name, *flags.ConsumerName, false, false, false, true, nil)
	if nil != err {
		logrus.Panic(err)
	}

	return messages
}

func (w *Dog) onRabbitMqMessage(ctx context.Context, m amqp.Delivery) {
	if m.DeliveryTag == 0 {
		w.rabbit.ch.Nack(m.DeliveryTag, false, false)
		return
	}

	err, ack, buffer := w.woof(ctx, m.Body)
	if err != nil {
		logrus.WithError(err).Errorln("Failed to handle new message: " + string(m.Body))
	}

	if ack {
		w.rabbit.ch.Ack(m.DeliveryTag, true)
	}

	if buffer {
		w.rabbit.tags = append(w.rabbit.tags, m.DeliveryTag)
	}
}
