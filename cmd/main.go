package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"go1/a-wip"
	"time"
	"github.com/Sirupsen/logrus"
)

func main() {
	o := a_wip.NewOpts()
	con, err := a_wip.Connection(*o.Url)
	if err != nil {
		fmt.Println("ERROR", err)
	}

	defer con.Close()

	ch := a_wip.Channel(con, *o.Kind, *o.Exchange, *o.PrefetchCount, *o.PrefetchSize)
	defer ch.Close()

	messages := a_wip.Messages(ch, *o.QueueName, *o.Exchange, *o.RoutingKey, *o.ConsumerName)
	ticker := time.NewTicker(5 * time.Second)
	listen(ch, messages, ticker, *o.PrefetchCount)
}

func listen(ch *amqp.Channel, messages <-chan amqp.Delivery, ticker *time.Ticker, count int) {
	actions := a_wip.NewActions()

	process := func() {
		logrus.Infof("\n\nhas %d to do.\n", actions.Length())

		for _, m := range actions.Items() {
			logrus.Infof("[consum] %s %s %d", m.RoutingKey, m.Body, m.DeliveryTag)
			ch.Ack(m.DeliveryTag, true)
		}

		actions.Clear()
	}

	for {
		select {
		case <-ticker.C:
			if actions.Length() > 0 {
				process()
			}

		case m := <-messages:
			actions.Add(m)

			if actions.Length() >= count {
				process()
			}
		}
	}
}
