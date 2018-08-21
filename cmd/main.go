package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"go1/a-wip"
	"time"
	"github.com/Sirupsen/logrus"
	"context"
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

func listen(ch *amqp.Channel, messages <-chan amqp.Delivery, ticker *time.Ticker, count int) (error) {
	ctx := context.Background()
	actions := a_wip.NewActions()
	client, bulk, err := a_wip.ElasticSearchTools(ctx)

	if err != nil {
		return err
	}

	process := func() {
		deliveryTags := []uint64{}
		for _, a := range actions.Items() {
			req, err := a.BuildRequest()
			if err != nil {
				logrus.WithError(err).Errorf("Failed to build Elastic Request")
			}

			deliveryTags = append(deliveryTags, a.DeliveryTag)
			bulk.Add(req)
		}

		err := bulk.Flush()
		if err != nil {
			for _, deliveryTag := range deliveryTags {
				ch.Ack(deliveryTag, true)
			}

			actions.Clear()
		}
	}

	for {
		select {
		case <-ticker.C:
			if actions.Length() > 0 {
				process()
			}

		case m := <-messages:
			if action, err := a_wip.NewAction(m.DeliveryTag, m.Body); err != nil {
				logrus.
					WithError(err).
					Errorf("Failed to convert queue message to Elastic Search request.")
			} else {
				// Not all requests are bulkable.
				capatibility := action.Bulkable()

				switch capatibility {
				case "bulkable":
					actions.Add(*action)

					if actions.Length() >= count {
						process()
					}

				case "_update_by_query":
					if actions.Length() > 0 {
						process()
					}

					req, err := action.BuildUpdateByQueryRequest(client)
					if err != nil {
						req.Do(ctx)
					}

				case "_delete_by_query":
					if actions.Length() > 0 {
						process()
					}

					req, err := action.BuildDeleteByQueryRequest(client)
					if err != nil {
						req.Do(ctx)
					}
				}
			}
		}
	}

	return nil
}
