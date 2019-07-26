package es_writer

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type RabbitMqInput struct {
	ch   *amqp.Channel
	tags []uint64
}

func (r *RabbitMqInput) messages(cnf *Configuration) <-chan amqp.Delivery {
	queue, err := r.ch.QueueDeclare(cnf.RabbitMq.QueueName, false, false, false, false, nil, )
	if nil != err {
		logrus.Panic(err)
	}

	err = r.ch.QueueBind(queue.Name, cnf.RabbitMq.RoutingKey, cnf.RabbitMq.Exchange, true, nil)
	if nil != err {
		logrus.Panic(err)
	}

	messages, err := r.ch.Consume(queue.Name, cnf.RabbitMq.ConsumerName, false, false, false, true, nil)
	if nil != err {
		logrus.Panic(err)
	}

	return messages
}

func (r *RabbitMqInput) start(ctx context.Context, cnf *Configuration, pushHandler PushCallback, terminate chan bool, booting *sync.WaitGroup) {
	wg := sync.WaitGroup{}
	messages := r.messages(cnf)
	booting.Done()

	for {
		select {
		case <-terminate:
			wg.Wait() // don't terminate until data flushing is completed
			return

		case m := <-messages:
			wg.Add(1)
			bufferMutext.Lock()
			r.onMessage(ctx, m, pushHandler)
			bufferMutext.Unlock()
			wg.Done()
		}
	}
}

func (r *RabbitMqInput) onMessage(ctx context.Context, m amqp.Delivery, pushHandler PushCallback) {
	if m.DeliveryTag == 0 {
		r.ch.Nack(m.DeliveryTag, false, false)
		return
	}

	err, ack, buffer := pushHandler(ctx, m.Body)
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
