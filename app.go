package es_writer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

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
	es                *elastic.Client
	bulkTimeoutString string
	bulkTimeout       time.Duration
	refresh           string
}

func (this *App) Run(ctx context.Context, container Container) {
	handler := this.push()

	eg := errgroup.Group{}
	eg.Go(func() error { return this.rabbit.start(ctx, container, handler) })
	eg.Go(func() error { return this.loop(ctx, container) })
	err := eg.Wait()
	if nil != err {
		logrus.WithError(err).Errorln("application exit")
	}
}

func (this *App) loop(ctx context.Context, container Container) error {
	ticker := time.NewTicker(*container.TickInterval)

	// ping server every 5m to avoid connection reset error
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				break

			case <-time.After(5 * time.Minute):
				_, _ = this.es.ClusterHealth().Do(ctx)
			}
		}
	}(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if this.buffer.Length() > 0 {
				bufferMutext.Lock()
				this.flush(ctx)
				bufferMutext.Unlock()
			}
		}
	}

}

func (this *App) push() PushCallback {
	return func(ctx context.Context, body []byte) (error, bool, bool) {
		buffer := false
		ack := false
		element, err := action.NewElement(body)
		if err != nil {
			return err, ack, buffer
		}

		// message filtering: Don't process if not contains expecting text.
		if "" != this.urlContains {
			if !strings.Contains(element.Uri, this.urlContains) {
				return nil, true, false
			}
		}

		// message filtering: Don't process if contains unexpecting text.
		if "" != this.urlNotContains {
			if strings.Contains(element.Uri, this.urlNotContains) {
				return nil, true, false
			}
		}

		// Not all requests are bulkable
		requestType := element.RequestType()
		if "bulkable" != requestType {
			if this.buffer.Length() > 0 {
				this.flush(ctx)
			}

			err = this.handleUnbulkableRequest(ctx, requestType, element)
			ack = err == nil

			return err, ack, buffer
		}

		this.buffer.Add(element)

		if this.buffer.Length() < this.count {
			buffer = true

			return nil, ack, buffer
		}

		this.flush(ctx)

		return nil, ack, buffer
	}
}

func (this *App) handleUnbulkableRequest(ctx context.Context, requestType string, element action.Element) error {
	switch requestType {
	case "update_by_query":
		return hanldeUpdateByQuery(ctx, this.es, element, requestType)

	case "delete_by_query":
		return hanldeDeleteByQuery(ctx, this.es, element, requestType)

	case "indices_create":
		return handleIndicesCreate(ctx, this.es, element)

	case "indices_delete":
		return handleIndicesDelete(ctx, this.es, element)

	case "indices_alias":
		return handleIndicesAlias(ctx, this.es, element)

	default:
		return fmt.Errorf("unsupported request type: %s", requestType)
	}
}

func (this *App) flush(ctx context.Context) {
	bulk := this.es.Bulk().Refresh(this.refresh)
	bulk.Timeout(this.bulkTimeoutString)
	
	for _, element := range this.buffer.Elements() {
		bulk.Add(element)
	}

	ctx, cancel := context.WithTimeout(ctx, this.bulkTimeout)
	defer cancel()
	this.doFlush(ctx, bulk)
	this.buffer.Clear()
	this.rabbit.onFlush()
}

func (this *App) doFlush(ctx context.Context, bulk *elastic.BulkService) {
	var hasError error
	var retriableError bool

	for _, retry := range retriesInterval {
		logrus.Debugln("Flushing")
		res, err := bulk.Do(ctx)
		hasError = err

		if err != nil {
			if this.isErrorRetriable(err) {
				retriableError = true

				logrus.
					WithField("time", retry).
					Infoln("Sleep")

				time.Sleep(retry)
				continue
			} else {
				retriableError = false
				break
			}
		}

		this.verboseResponse(res)

		break
	}

	if hasError != nil {
		logrus.
			WithError(hasError).
			WithField("retriable", retriableError).
			Panicln("failed flushing")
	}
}

func (this *App) isErrorRetriable(err error) bool {
	retriable := false

	if strings.Contains(err.Error(), "no available connection") {
		retriable = true
	} else if strings.Contains(err.Error(), "connection reset by peer") {
		retriable = true
	} else if strings.HasPrefix(err.Error(), "Post") {
		if strings.HasSuffix(err.Error(), "EOF") {
			retriable = true
		}
	}

	if retriable {
		logrus.WithError(err).Warningln("failed flushing")
	}

	return retriable
}

func (this *App) verboseResponse(res *elastic.BulkResponse) {
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
