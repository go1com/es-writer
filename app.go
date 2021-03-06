package es_writer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go1com/es-writer/action"

	"github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

type PushCallback func(context.Context, []byte) (err error, ack bool, buff bool, flush bool)

type App struct {
	debug bool

	// RabbitMQ
	rabbit *RabbitMqInput
	buffer *action.Container
	mutex  *sync.Mutex
	count  int

	// message filtering
	urlContains    string
	urlNotContains string

	// ElasticSearch
	es                *elastic.Client
	bulkTimeoutString string
	bulkTimeout       time.Duration
	refresh           string
	isFlushing        bool
	isFlushingRWMutex *sync.RWMutex
}

func (this *App) Run(ctx context.Context, container Container) error {
	handler := this.push()

	return this.rabbit.start(ctx, container, handler)
}


func (this *App) push() PushCallback {
	return func(ctx context.Context, body []byte) (error, bool, bool, bool) {
		ack := false
		buffer := false
		element, err := action.NewElement(body)

		if err != nil {
			return err, false, false, false
		}

		// message filtering: Don't process if not contains expecting text.
		if "" != this.urlContains {
			if !strings.Contains(element.Uri, this.urlContains) {
				return nil, true, false, false
			}
		}

		// message filtering: Don't process if contains unexpecting text.
		if "" != this.urlNotContains {
			if strings.Contains(element.Uri, this.urlNotContains) {
				return nil, true, false, false
			}
		}

		// Not all requests are bulkable
		requestType := element.RequestType()
		if "bulkable" != requestType {
			if this.buffer.Length() > 0 {
				// before perform unbulkable action, we need flushing the buffer first.
				if err := this.flush(ctx); nil != err {
					// failed flushing, can't execute unbulkable action yet.
					// may try again later
					return err, true, buffer, false
				}
			}

			// execute unbulkable action now.
			// if no error found, ack the message now.
			err = this.handleUnbulkableRequest(ctx, requestType, element)
			ack = err == nil

			return err, ack, buffer, false
		}

		this.buffer.Add(element)

		if this.buffer.Length() < this.count {
			buffer = true

			return nil, ack, buffer, false
		}

		return nil, ack, true, true
	}
}

func (this *App) handleUnbulkableRequest(ctx context.Context, requestType string, element action.Element) error {
	switch requestType {
	case "update_by_query":
		return handleUpdateByQuery(ctx, this.es, element, requestType)

	case "delete_by_query":
		return handleDeleteByQuery(ctx, this.es, element, requestType)

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

func (this *App) flush(ctx context.Context) error {
	this.mutex.Lock()
	this.isFlushingRWMutex.Lock()
	this.isFlushing = true

	defer func() {
		this.isFlushing = false
		this.mutex.Unlock()
		this.isFlushingRWMutex.Unlock()
	}()

	if this.buffer.Length() == 0 {
		return nil
	}

	var cancel context.CancelFunc

	bulk := this.es.Bulk().Refresh(this.refresh)
	for _, element := range this.buffer.Elements() {
		bulk.Add(element)
	}

	if this.bulkTimeoutString != "" {
		bulk.Timeout(this.bulkTimeoutString)
		ctx, cancel = context.WithTimeout(ctx, this.bulkTimeout)
		defer cancel()
	}

	if err := this.doFlush(ctx, bulk); nil != err {
		return err
	}

	this.buffer.Clear()
	this.rabbit.onFlush()

	return nil
}

func (this *App) doFlush(ctx context.Context, bulk *elastic.BulkService) error {
	for _, retry := range retriesInterval {
		logrus.Debugln("Flushing")
		res, err := bulk.Do(ctx)

		if err != nil {
			if this.isErrorRetriable(err) {
				logrus.
					WithField("time", retry).
					Infoln("Sleep")

				time.Sleep(retry)
				continue
			} else {
				return err
			}
		}

		this.verboseResponse(res)

		break
	}

	return nil
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
				relateItems := make([]action.Element, 0, this.buffer.Length())

				for _, item := range this.buffer.Elements() {
					if item.DocType == riValue.Type && item.DocId == riValue.Id && item.Index == riValue.Index {
						relateItems = append(relateItems, item)
					}
				}

				logrus.
					WithField("key", riKey).
					WithField("type", riValue.Error.Type).
					WithField("phase", riValue.Error.Phase).
					WithField("reason", riValue.Error.Reason).
					WithField("relateItems", relateItems).
					Errorf("failed to process item %s", riKey)
			}
		}
	}

	logrus.
		WithField("res.took", res.Took).
		WithField("res.items", len(res.Items)).
		WithField("res.errors", res.Errors).
		Debugln("bulk done")
}
