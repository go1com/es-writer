package es_writer

import (
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func (w *Dog) messages(flags Flags) <-chan amqp.Delivery {
	queue, err := w.ch.QueueDeclare(*flags.QueueName, false, false, false, false, nil, )
	if nil != err {
		logrus.Panic(err)
	}

	err = w.ch.QueueBind(queue.Name, *flags.RoutingKey, *flags.Exchange, true, nil)
	if nil != err {
		logrus.Panic(err)
	}

	messages, err := w.ch.Consume(queue.Name, *flags.ConsumerName, false, false, false, true, nil)
	if nil != err {
		logrus.Panic(err)
	}

	return messages
}
