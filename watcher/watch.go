package watcher

import (
	"go1/es-writer/action"
	"context"
	"go1/es-writer"
	"github.com/streadway/amqp"
	"time"
	"github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
	"fmt"
)

type Watcher struct {
	ch       *amqp.Channel
	actions  *action.Container
	count    int
	esClient *elastic.Client
	esBulk   *elastic.BulkProcessor
}

func NewWatcher(ch *amqp.Channel, count int, es *elastic.Client, bulk *elastic.BulkProcessor) *Watcher {
	w := &Watcher{
		ch:       ch,
		actions:  action.NewContainer(),
		count:    count,
		esClient: es,
		esBulk:   bulk,
	}

	return w
}

func (w *Watcher) Watch(ctx context.Context, flags es_writer.Flags) (error) {
	ticker := time.NewTicker(*flags.TickInterval)
	messages, err := Messages(w.ch, *flags.QueueName, *flags.Exchange, *flags.RoutingKey, *flags.ConsumerName)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ticker.C:
			if w.actions.Length() > 0 {
				w.flush()
			}

		case m := <-messages:
			w.onNewMessage(ctx, m)
		}
	}

	return nil
}

func (w *Watcher) onNewMessage(ctx context.Context, m amqp.Delivery) error {
	element, err := action.NewElement(m.DeliveryTag, m.Body)
	if err != nil {
		return err
	}

	// Not all requests are bulkable
	capatibility := element.Bulkable()
	if "bulkable" != capatibility {
		return w.handleUnBulkableAction(ctx, capatibility, element)
	}

	w.actions.Add(*element)
	if w.actions.Length() >= w.count {
		w.flush()
	}

	return nil
}

func (w *Watcher) handleUnBulkableAction(ctx context.Context, capatibility string, element *action.Element) error {
	switch capatibility {
	case "_update_by_query":
		if w.actions.Length() > 0 {
			w.flush()
		}

		req, err := element.BuildUpdateByQueryRequest(w.esClient)
		if err != nil {
			_, err := req.Do(ctx)

			return err
		}

	case "_delete_by_query":
		if w.actions.Length() > 0 {
			w.flush()
		}

		req, err := element.BuildDeleteByQueryRequest(w.esClient)
		if err != nil {
			_, err := req.Do(ctx)
			return err
		}

	default:
		return fmt.Errorf("unsupported request type: %s", capatibility)
	}

	return nil
}

func (w *Watcher) flush() {
	deliveryTags := []uint64{}
	for _, a := range w.actions.Items() {
		req, err := a.BuildRequest()
		if err != nil {
			logrus.WithError(err).Errorf("Failed to build Elastic Search request")
		}

		deliveryTags = append(deliveryTags, a.DeliveryTag)
		w.esBulk.Add(req)
	}

	err := w.esBulk.Flush()
	if err != nil {
		for _, deliveryTag := range deliveryTags {
			w.ch.Ack(deliveryTag, true)
		}

		w.actions.Clear()
	}
}
