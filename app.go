package es_writer

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go1com/es-writer/action"

	"github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

type PushCallback func(context.Context, []byte) (error, bool, bool)

type App struct {
	debug bool

	// RabbitMQ
	rabbit *RabbitMqInput
	buffer *action.Container
	count  int

	// ElasticSearch
	es      *elastic.Client
	bulk    *elastic.BulkProcessor
	refresh string
}

func (w *App) Start(ctx context.Context, flags Flags, terminate chan os.Signal) {
	pushHandler := w.push()

	terminateRabbit := make(chan bool)
	go w.rabbit.start(ctx, flags, pushHandler, terminateRabbit)

	terminateInterval := make(chan bool)
	go interval(w, ctx, flags, terminateInterval)

	<-terminate
	terminateRabbit <- true
	terminateInterval <- true
}

func (w *App) push() PushCallback {
	return func(ctx context.Context, body []byte) (error, bool, bool) {
		buffer := false
		ack := false
		element, err := action.NewElement(body)
		if err != nil {
			return err, ack, buffer
		}

		// Not all requests are bulkable
		requestType := element.RequestType()
		if "bulkable" != requestType {
			if w.buffer.Length() > 0 {
				w.flush(ctx)
			}

			err = w.handleUnbulkableRequest(ctx, requestType, element)
			ack = err == nil

			return err, ack, buffer
		}

		w.buffer.Add(element)

		if w.buffer.Length() < w.count {
			buffer = true

			return nil, ack, buffer
		}

		metricFlushCounter.WithLabelValues("length").Inc()
		w.flush(ctx)

		return nil, ack, buffer
	}
}

func (w *App) handleUnbulkableRequest(ctx context.Context, requestType string, element action.Element) error {
	defer metricActionCounter.WithLabelValues(requestType).Inc()

	switch requestType {
	case "update_by_query":
		return hanldeUpdateByQuery(ctx, w.es, element, requestType)

	case "delete_by_query":
		return hanldeDeleteByQuery(ctx, w.es, element, requestType)

	case "indices_create":
		return hanldeIndicesCreate(ctx, w.es, element)

	case "indices_delete":
		return handleIndicesDelete(ctx, w.es, element)

	case "indices_alias":
		return handleIndicesAlias(ctx, w.es, element)

	default:
		metricInvalidCounter.WithLabelValues(requestType).Inc()
		return fmt.Errorf("unsupported request type: %s", requestType)
	}
}

func (w *App) flush(ctx context.Context) {
	metricActionCounter.WithLabelValues("bulk").Inc()
	bulk := w.es.Bulk().Refresh(w.refresh)

	for _, element := range w.buffer.Elements() {
		bulk.Add(element)
	}

	w.doFlush(ctx, bulk)
	w.buffer.Clear()
	w.rabbit.onFlush()
}

func (w *App) doFlush(ctx context.Context, bulk *elastic.BulkService) {
	var hasError error
	var retriableError bool

	for _, retry := range retriesInterval {
		start := time.Now()
		res, err := bulk.Do(ctx)
		metricDurationHistogram.
			WithLabelValues("bulk").
			Observe(time.Since(start).Seconds())

		if err != nil {
			hasError = err

			if w.isErrorRetriable(err) {
				retriableError = true
				time.Sleep(retry)
				continue
			} else {
				retriableError = false
				break
			}
		}

		w.verboseResponse(res)

		break
	}

	if hasError != nil {
		logrus.
			WithError(hasError).
			WithField("retriable", retriableError).
			Panicln("failed flushing")

		metricFailureCounter.WithLabelValues("bulk").Inc()
	}
}

func (w *App) isErrorRetriable(err error) bool {
	retriable := false

	if strings.Contains(err.Error(), "no available connection") {
		retriable = true
	} else if strings.HasPrefix(err.Error(), "Post") {
		if strings.HasSuffix(err.Error(), "EOF") {
			retriable = true
		}
	}

	if retriable {
		metricRetryCounter.WithLabelValues("bulk").Inc()
		logrus.WithError(err).Warningln("failed flushing")
	}

	return retriable
}

func (w *App) verboseResponse(res *elastic.BulkResponse) {
	for _, rItem := range res.Items {
		for riKey, riValue := range rItem {
			if riValue.Error != nil {
				logrus.
					WithField("key", riKey).
					WithField("type", riValue.Error.Type).
					WithField("phase", riValue.Error.Phase).
					WithField("reason", riValue.Error.Reason).
					Errorf("failed to process item %s", riKey)
			}
		}
	}

	logrus.Debugln("[push] bulk took: ", res.Took)
}
