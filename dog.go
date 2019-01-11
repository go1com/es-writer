package es_writer

import (
	"github.com/go1com/es-writer/action"

	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gopkg.in/olivere/elastic.v5"
	"strings"
	"time"
)

type Dog struct {
	debug bool

	// RabbitMQ
	ch      *amqp.Channel
	actions *action.Container
	count   int

	// ElasticSearch
	es   *elastic.Client
	bulk *elastic.BulkProcessor
}

func (w *Dog) UnitWorks() int {
	return w.actions.Length()
}

func (w *Dog) Start(ctx context.Context, flags Flags) (error) {
	ticker := time.NewTicker(*flags.TickInterval)
	messages, err := w.messages(flags)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ticker.C:
			if w.actions.Length() > 0 {
				metricFlushCounter.WithLabelValues("time").Inc()
				w.flush(ctx)
			}

		case m := <-messages:
			if m.DeliveryTag == 0 {
				w.ch.Nack(m.DeliveryTag, false, false)
				continue
			}

			err := w.woof(ctx, m)
			if err != nil {
				logrus.WithError(err).Errorln("Failed to handle new message: " + string(m.Body))
			}
		}
	}

	return nil
}

func (w *Dog) messages(flags Flags) (<-chan amqp.Delivery, error) {
	queue, err := w.ch.QueueDeclare(*flags.QueueName, false, false, false, false, nil, )
	if nil != err {
		return nil, err
	}

	err = w.ch.QueueBind(queue.Name, *flags.RoutingKey, *flags.Exchange, true, nil)
	if nil != err {
		return nil, err
	}

	return w.ch.Consume(queue.Name, *flags.ConsumerName, false, false, false, true, nil)
}

func (w *Dog) woof(ctx context.Context, m amqp.Delivery) error {
	element, err := action.NewElement(m.DeliveryTag, m.Body)
	if err != nil {
		return err
	}

	// Not all requests are bulkable
	requestType := element.RequestType()
	if "bulkable" != requestType {
		if w.actions.Length() > 0 {
			w.flush(ctx)
		}

		err = w.doubleWoof(ctx, requestType, element)
		if err == nil {
			w.ch.Ack(m.DeliveryTag, true)
		}

		return err
	}

	if w.debug {
		logrus.Debugln("[woof] bulkable action: ", w.actions.Length()+1)
	}

	w.actions.Add(element)
	if w.actions.Length() >= w.count {
		metricFlushCounter.WithLabelValues("length").Inc()
		w.flush(ctx)
	}

	return nil
}

func (w *Dog) doubleWoof(ctx context.Context, requestType string, element action.Element) error {
	metricActionCounter.WithLabelValues(requestType).Inc()

	switch requestType {
	case "update_by_query":
		service, err := element.UpdateByQueryService(w.es)
		if err != nil {
			metricFailureCounter.WithLabelValues(requestType).Inc()
			return err
		}

		conflictRetryIntervals := []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 7 * time.Second, 0}
		for _, conflictRetryInterval := range conflictRetryIntervals {
			start := time.Now()
			_, err = service.Do(ctx)
			metricDurationHistogram.
				WithLabelValues(requestType).
				Observe(time.Since(start).Seconds())

			if err == nil {
				break
			}

			if strings.Contains(err.Error(), "Error 409 (Conflict)") {
				metricFailureCounter.WithLabelValues(requestType).Inc()
				logrus.WithError(err).Errorf("writing has conflict; try again in %s.\n", conflictRetryInterval)
				time.Sleep(conflictRetryInterval)
			}
			metricRetryCounter.WithLabelValues(requestType).Inc()
		}

		if err != nil {
			metricFailureCounter.WithLabelValues(requestType).Inc()
		}

		return err

	case "delete_by_query":
		service, err := element.DeleteByQueryService(w.es)
		if err != nil {
			metricFailureCounter.WithLabelValues(requestType).Inc()
			return err
		}

		conflictRetryIntervals := []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 7 * time.Second, 0}
		for _, conflictRetryInterval := range conflictRetryIntervals {
			start := time.Now()
			_, err = service.Do(ctx)
			metricDurationHistogram.
				WithLabelValues(requestType).
				Observe(time.Since(start).Seconds())

			if err == nil {
				break
			}

			if strings.Contains(err.Error(), "Error 409 (Conflict)") {
				metricFailureCounter.WithLabelValues(requestType).Inc()
				logrus.WithError(err).Errorf("deleting has conflict; try again in %s.\n", conflictRetryInterval)
				time.Sleep(conflictRetryInterval)
			}

			metricRetryCounter.WithLabelValues(requestType).Inc()
		}

		if err != nil {
			metricFailureCounter.WithLabelValues(requestType).Inc()
		}

		return err

	case "indices_create":
		service, err := element.IndicesCreateService(w.es)
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
		service, err := element.IndicesDeleteService(w.es)
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

	case "indices_alias":
		res, err := action.CreateIndiceAlias(ctx, w.es, element)
		if err != nil {
			if strings.Contains(err.Error(), "index_not_found_exception") {
				logrus.WithError(err).Errorln("That's ok if the index is existing.")
				return nil
			}
		}

		logrus.
			WithError(err).
			WithField("action", "indices_alias").
			WithField("res", res).
			WithField("body", element.String()).
			Infoln("create")

		return err

	default:
		metricInvalidCounter.WithLabelValues(requestType).Inc()
		return fmt.Errorf("unsupported request type: %s", requestType)
	}

	return nil
}

func (w *Dog) flush(ctx context.Context) {
	metricActionCounter.WithLabelValues("bulk").Inc()
	// TODO: Need a review on refreshing flag here, this can make index very slowly
	bulk := w.es.Bulk().Refresh("true")

	deliveryTags := []uint64{}
	for _, element := range w.actions.Elements() {
		deliveryTags = append(deliveryTags, element.DeliveryTag)
		bulk.Add(element)
	}

	retries := []time.Duration{
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
		60 * time.Second,
		90 * time.Second,
		90 * time.Second,
		90 * time.Second,
	}

	var hasError error
	var retriableError bool

	for _, retry := range retries {
		start := time.Now()
		res, err := bulk.Do(ctx)
		metricDurationHistogram.
			WithLabelValues("bulk").
			Observe(time.Since(start).Seconds())

		if err != nil {
			hasError = err
			retriable := false

			if strings.Contains(err.Error(), "no available connection") {
				retriable = true
			} else {
				if strings.HasPrefix(err.Error(), "Post") {
					if strings.HasSuffix(err.Error(), "EOF") {
						retriable = true
					}
				}
			}

			if retriable {
				metricRetryCounter.WithLabelValues("bulk").Inc()
				retriableError = true
				logrus.WithError(err).Warningln("failed flushing")
				time.Sleep(retry)
				continue
			} else {
				retriableError = false
				break
			}
		}

		for _, rItem := range res.Items {
			for riKey, riValue := range rItem {
				if riValue.Error != nil {
					logrus.
						WithField("key", riKey).
						WithField("type", riValue.Error.Type).
						WithField("phase", riValue.Error.Phase).
						WithField("reason", riValue.Error.Reason).
						Warningln("")
				}
			}
		}

		logrus.Debugln("[woof] bulk took: %d", res.Took)

		break
	}

	if hasError == nil {
		for _, deliveryTag := range deliveryTags {
			w.ch.Ack(deliveryTag, true)
		}

		w.actions.Clear()
	} else {
		logrus.
			WithError(hasError).
			WithField("retriable", retriableError).
			Panicln("failed flushing")

		metricFailureCounter.WithLabelValues("bulk").Inc()
	}
}
