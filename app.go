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

	// message filtering
	urlContains    string
	urlNotContains string

	// ElasticSearch
	es      *elastic.Client
	bulk    *elastic.BulkProcessor
	refresh string
}

func (app *App) Start(ctx context.Context, flags Flags, terminate chan os.Signal) {
	pushHandler := app.push()

	terminateRabbit := make(chan bool)
	go app.rabbit.start(ctx, flags, pushHandler, terminateRabbit)

	terminateInterval := make(chan bool)
	go interval(app, ctx, flags, terminateInterval)

	<-terminate
	terminateRabbit <- true
	terminateInterval <- true
}

func (app *App) push() PushCallback {
	return func(ctx context.Context, body []byte) (error, bool, bool) {
		buffer := false
		ack := false
		element, err := action.NewElement(body)
		if err != nil {
			return err, ack, buffer
		}

		// message filtering: Don't process if not contains expecting text.
		if "" != app.urlContains {
			if !strings.Contains(element.Uri, app.urlContains) {
				return nil, true, false
			}
		}

		// message filtering: Don't process if contains unexpecting text.
		if "" != app.urlNotContains {
			if strings.Contains(element.Uri, app.urlNotContains) {
				return nil, true, false
			}
		}

		// Not all requests are bulkable
		requestType := element.RequestType()
		if "bulkable" != requestType {
			if app.buffer.Length() > 0 {
				app.flush(ctx)
			}

			err = app.handleUnbulkableRequest(ctx, requestType, element)
			ack = err == nil

			return err, ack, buffer
		}

		app.buffer.Add(element)

		if app.buffer.Length() < app.count {
			buffer = true

			return nil, ack, buffer
		}

		metricFlushCounter.WithLabelValues("length").Inc()
		app.flush(ctx)

		return nil, ack, buffer
	}
}

func (app *App) handleUnbulkableRequest(ctx context.Context, requestType string, element action.Element) error {
	defer metricActionCounter.WithLabelValues(requestType).Inc()

	switch requestType {
	case "update_by_query":
		return hanldeUpdateByQuery(ctx, app.es, element, requestType)

	case "delete_by_query":
		return hanldeDeleteByQuery(ctx, app.es, element, requestType)

	case "indices_create":
		return hanldeIndicesCreate(ctx, app.es, element)

	case "indices_delete":
		return handleIndicesDelete(ctx, app.es, element)

	case "indices_alias":
		return handleIndicesAlias(ctx, app.es, element)

	default:
		metricInvalidCounter.WithLabelValues(requestType).Inc()
		return fmt.Errorf("unsupported request type: %s", requestType)
	}
}

func (app *App) flush(ctx context.Context) {
	metricActionCounter.WithLabelValues("bulk").Inc()
	bulk := app.es.Bulk().Refresh(app.refresh)

	for _, element := range app.buffer.Elements() {
		bulk.Add(element)
	}

	app.doFlush(ctx, bulk)
	app.buffer.Clear()
	app.rabbit.onFlush()
}

func (app *App) doFlush(ctx context.Context, bulk *elastic.BulkService) {
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

			if app.isErrorRetriable(err) {
				retriableError = true
				time.Sleep(retry)
				continue
			} else {
				retriableError = false
				break
			}
		}

		app.verboseResponse(res)

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

func (app *App) isErrorRetriable(err error) bool {
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

func (app *App) verboseResponse(res *elastic.BulkResponse) {
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
