package main

import (
	"github.com/streadway/amqp"
	"go1/es-writer"
	"time"
	"github.com/Sirupsen/logrus"
	"context"
	"go1/es-writer/action"
	"go1/es-writer/watcher"
)

func main() {
	flags := es_writer.NewFlags()
	con, err := watcher.Connection(*flags.Url)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create watcher connection.")
		return
	} else {
		defer con.Close()
	}

	ch, err := watcher.Channel(con, *flags.Kind, *flags.Exchange, *flags.PrefetchCount, *flags.PrefetchSize)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to create watcher channel.")
		return
	} else {
		defer ch.Close()
	}

	messages, err := watcher.Messages(ch, *flags.QueueName, *flags.Exchange, *flags.RoutingKey, *flags.ConsumerName)
	if err != nil {
		logrus.WithError(err).Fatalln("Failed to consume messages.")
	} else {
		ticker := time.NewTicker(*flags.TickInterval)
		listen(flags, ch, messages, ticker, *flags.PrefetchCount)
	}
}

func listen(flags es_writer.Flags, ch *amqp.Channel, messages <-chan amqp.Delivery, ticker *time.Ticker, count int) (error) {
	ctx := context.Background()
	actions := action.NewActions()
	client, bulk, err := action.Clients(ctx, flags)

	if err != nil {
		return err
	}

	process := func() {
		deliveryTags := []uint64{}
		for _, a := range actions.Items() {
			req, err := a.BuildRequest()
			if err != nil {
				logrus.WithError(err).Errorf("Failed to build Elastic Search request")
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
			if element, err := action.NewElement(m.DeliveryTag, m.Body); err != nil {
				logrus.
					WithError(err).
					Errorf("Failed to convert queue message to Elastic Search request.")
			} else {
				// Not all requests are bulkable.
				capatibility := element.Bulkable()

				switch capatibility {
				case "bulkable":
					actions.Add(*element)
					if actions.Length() >= count {
						process()
					}

				case "_update_by_query":
					if actions.Length() > 0 {
						process()
					}

					req, err := element.BuildUpdateByQueryRequest(client)
					if err != nil {
						req.Do(ctx)
					}

				case "_delete_by_query":
					if actions.Length() > 0 {
						process()
					}

					req, err := element.BuildDeleteByQueryRequest(client)
					if err != nil {
						req.Do(ctx)
					}
				}
			}
		}
	}

	return nil
}
