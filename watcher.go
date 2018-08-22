package es_writer

import (
	"go1/es-writer/action"
	"context"
	"github.com/streadway/amqp"
	"time"
	"gopkg.in/olivere/elastic.v5"
	"fmt"
	"github.com/Sirupsen/logrus"
	"strings"
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

func (w *Watcher) UnitWorks() int {
	return w.actions.Length()
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
			if m.DeliveryTag == 0 {
				w.ch.Nack(m.DeliveryTag, false, false)
				continue
			}

			err := w.onNewMessage(ctx, m)
			if err != nil {
				logrus.WithError(err).Errorln("Failed to handle new message: " + string(m.Body))
			}
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
		err = w.handleUnBulkableAction(ctx, requestType, element)
		if err == nil {
			w.ch.Ack(m.DeliveryTag, true)
		}

		return err
	}

	w.actions.Add(element)
	if w.actions.Length() >= w.count {
		w.flush()
	}

	return nil
}

func (w *Watcher) handleUnBulkableAction(ctx context.Context, requestType string, element action.Element) error {
	switch requestType {
	case "update_by_query":
		if w.actions.Length() > 0 {
			w.flush()
		}

		service, err := element.UpdateByQueryService(w.esClient)
		if err != nil {
			_, err := service.Do(ctx)

			return err
		}

	case "delete_by_query":
		if w.actions.Length() > 0 {
			w.flush()
		}

		service, err := element.DeleteByQueryService(w.esClient)
		if err != nil {
			_, err := service.Do(ctx)
			return err
		}

	case "indices_create":
		service, err := element.IndicesCreateService(w.esClient)
		if err != nil {
			return err
		}

		_, err = service.Do(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				logrus.WithError(err).Errorln("That's ok if the index is existing.")

				return nil
			}
		}

		return err

	case "indices_delete":
		service, err := element.IndicesDeleteService(w.esClient)
		if err != nil {
			return err
		}

		_, err = service.Do(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "[type=index_not_found_exception]") {
				logrus.WithError(err).Infoln("That's ok if the index is not existing, already deleted somewhere.")

				return nil
			}
		}

		return err

	default:
		return fmt.Errorf("unsupported request type: %s", requestType)
	}

	return nil
}

func (w *Watcher) flush() {
	deliveryTags := []uint64{}
	for _, element := range w.actions.Elements() {
		deliveryTags = append(deliveryTags, element.DeliveryTag)
		w.esBulk.Add(element)
	}

	err := w.esBulk.Flush()
	if err != nil {
		for _, deliveryTag := range deliveryTags {
			w.ch.Ack(deliveryTag, true)
		}

		w.actions.Clear()
	}
}
