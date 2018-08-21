package watcher

import (
	"github.com/streadway/amqp"
	"github.com/Sirupsen/logrus"
	"fmt"
)

func Connection(queueUrl string) (*amqp.Connection, error) {
	con, err := amqp.Dial(queueUrl)

	if nil != err {
		return nil, err
	}

	go func() {
		conCloseChan := con.NotifyClose(make(chan *amqp.Error))

		select
		{
		case err := <-conCloseChan:
			logrus.Errorln(err.Error())

			panic(err.Error())
		}
	}()

	return con, nil
}

func Channel(con *amqp.Connection, kind string, exchangeName string, prefetchCount int, prefetchSize int) (*amqp.Channel, error) {
	ch, err := con.Channel()
	if nil != err {
		return nil, err
	}

	if "topic" != kind && "direct" != kind {
		ch.Close()

		return nil, fmt.Errorf("unsupported channel kind: %s", kind)
	}

	err = ch.ExchangeDeclare(exchangeName, kind, false, false, false, false, nil)
	if nil != err {
		ch.Close()

		return nil, err
	}

	err = ch.Qos(prefetchCount, prefetchSize, false)
	if nil != err {
		ch.Close()

		return nil, err
	}

	return ch, nil
}

func Messages(ch *amqp.Channel, queueName string, exchange string, routingKey string, consumerName string) (<-chan amqp.Delivery, error) {
	queue, err := ch.QueueDeclare(queueName, false, false, false, false, nil, )
	if nil != err {
		return nil, err
	}

	err = ch.QueueBind(queue.Name, routingKey, exchange, true, nil)
	if nil != err {
		return nil, err
	}

	messages, err := ch.Consume(queue.Name, consumerName, false, false, false, true, nil)
	if nil != err {
		return nil, err
	}

	return messages, nil
}
