package es_writer

import (
	"go1/es-writer/action"
	"context"
	"github.com/streadway/amqp"
	"time"
	"github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
	"fmt"
)

type Watcher struct {
	// RabbitMQ
	ch      *amqp.Channel
	actions *action.Container
	count   int

	// ElasticSearch
	esClient *elastic.Client
	esBulk   *elastic.BulkProcessor
}

func NewWatcher(ch *amqp.Channel, count int, es *elastic.Client, bulk *elastic.BulkProcessor) *Watcher {
	return &Watcher{
		ch:       ch,
		actions:  action.NewContainer(),
		count:    count,
		esClient: es,
		esBulk:   bulk,
	}
}

func (w *Watcher) Watch(ctx context.Context, flags Flags) (error) {
	ticker := time.NewTicker(*flags.TickInterval)
	messages, err := w.messages(flags)
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

func (w *Watcher) messages(flags Flags) (<-chan amqp.Delivery, error) {
	queue, err := w.ch.QueueDeclare(*flags.QueueName, false, false, false, false, nil, )
	if nil != err {
		return nil, err
	}

	err = w.ch.QueueBind(queue.Name, *flags.RoutingKey, *flags.Exchange, true, nil)
	if nil != err {
		return nil, err
	}

	messages, err := w.ch.Consume(queue.Name, *flags.ConsumerName, false, false, false, true, nil)
	if nil != err {
		return nil, err
	}

	return messages, nil
}

func (w *Watcher) onNewMessage(ctx context.Context, m amqp.Delivery) error {
	element, err := action.NewElement(m.DeliveryTag, m.Body)
	if err != nil {
		return err
	}

	// Not all requests are bulkable
	requestType := element.RequestType()
	if "bulkable" != requestType {
		return w.handleUnBulkableAction(ctx, requestType, element)
	}

	w.actions.Add(*element)
	if w.actions.Length() >= w.count {
		w.flush()
	}

	return nil
}

func (w *Watcher) handleUnBulkableAction(ctx context.Context, capatibility string, element *action.Element) error {
	switch capatibility {
	case "update_by_query":
		if w.actions.Length() > 0 {
			w.flush()
		}

		req, err := element.BuildUpdateByQueryRequest(w.esClient)
		if err != nil {
			_, err := req.Do(ctx)

			return err
		}

	case "delete_by_query":
		if w.actions.Length() > 0 {
			w.flush()
		}

		req, err := element.BuildDeleteByQueryRequest(w.esClient)
		if err != nil {
			_, err := req.Do(ctx)
			return err
		}

	case "indices_create":
		req, err := element.BuildIndicesCreateRequest(w.esClient)
		if err != nil {
			_, err := req.Do(ctx)
			return err
		}

	case "indices_delete":
		req, err := element.BuildIndicesDeleteRequest(w.esClient)
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
	for _, a := range w.actions.Elements() {
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
