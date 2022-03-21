package es_writer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go1com/es-writer/action"
	"github.com/streadway/amqp"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"go.uber.org/zap"
	"gopkg.in/olivere/elastic.v5"
)

type PushCallback func(context.Context, amqp.Delivery) (err error, ack bool, buff bool, flush bool)

type App struct {
	serviceName string
	debug       bool
	logger      *zap.Logger

	// RabbitMQ
	rabbit  *RabbitMqInput
	buffers []*action.Container
	mutex   *sync.Mutex
	count   int

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
	spans             []tracer.Span
}

func (this *App) Run(ctx context.Context, container Container) error {
	handler := this.push()

	return this.rabbit.start(ctx, container, handler)
}

func (this *App) push() PushCallback {
	return func(ctx context.Context, m amqp.Delivery) (error, bool, bool, bool) {
		spanOpts := []ddtrace.StartSpanOption{
			tracer.ServiceName(this.serviceName),
			tracer.SpanType("rabbitmq-consumer"),
			tracer.ResourceName("consumer"),
			tracer.Tag("message.routingKey", m.RoutingKey),
		}

		defer func() {
			span := tracer.StartSpan("consumer.forwarding", spanOpts...)
			this.spans = append(this.spans, span)
		}()

		ack := false
		buffer := false
		element, err := action.NewElement(m.Body)

		if err != nil {
			tracer.Tag("error", err.Error())

			return err, false, false, false
		}

		// message filtering: Don't process if not contains expecting text.
		if "" != this.urlContains {
			if !strings.Contains(element.Uri, this.urlContains) {
				tracer.Tag("skipped", true)

				return nil, true, false, false
			}
		}

		// message filtering: Don't process if contains unexpecting text.
		if "" != this.urlNotContains {
			if strings.Contains(element.Uri, this.urlNotContains) {
				spanOpts = append(spanOpts, tracer.Tag("skipped", true))

				return nil, true, false, false
			}
		}

		// Not all requests are bulkable
		requestType := element.RequestType()
		if "bulkable" != requestType {
			spanOpts = append(spanOpts, tracer.Tag("bulkable", false))

			if AnyNotEmpty(this.buffers) {
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
		} else {
			spanOpts = append(spanOpts, tracer.Tag("bulkable", true))
			spanOpts = append(spanOpts, tracer.Tag("uri", element.Uri))
			spanOpts = append(spanOpts, tracer.Tag("routing", element.Routing))
			spanOpts = append(spanOpts, tracer.Tag("parent", element.Parent))
			spanOpts = append(spanOpts, tracer.Tag("refresh", element.Refresh))
			spanOpts = append(spanOpts, tracer.Tag("waitForCompletion", element.WaitForCompletion))
			spanOpts = append(spanOpts, tracer.Tag("conflict", element.Conflict))
			spanOpts = append(spanOpts, tracer.Tag("versionType", element.VersionType))
			spanOpts = append(spanOpts, tracer.Tag("version", element.Version))
			spanOpts = append(spanOpts, tracer.Tag("method", element.Method))
			spanOpts = append(spanOpts, tracer.Tag("index", element.Index))
			spanOpts = append(spanOpts, tracer.Tag("docType", element.DocType))
			spanOpts = append(spanOpts, tracer.Tag("docId", element.DocId))

			carrier := headerToTextMapCarrier(m.Headers)
			if spanCtx, err := tracer.Extract(carrier); err == nil {
				spanOpts = append(spanOpts, tracer.ChildOf(spanCtx))
			}
		}

		id := GetDocumentIdFromElement(&element)
		i := GetBufferIndexFromId(id, len(this.buffers))
		this.buffers[i].Add(element)

		if TotalElements(this.buffers) < this.count {
			buffer = true

			return nil, ack, buffer, false
		}

		return nil, ack, true, true
	}
}

func (this *App) handleUnbulkableRequest(ctx context.Context, requestType string, element action.Element) error {
	switch requestType {
	case "update_by_query":
		return this.handleUpdateByQuery(ctx, this.es, element, requestType)

	case "delete_by_query":
		return this.handleDeleteByQuery(ctx, this.es, element, requestType)

	case "indices_create":
		return handleIndicesCreate(ctx, this.es, element)

	case "indices_delete":
		return this.handleIndicesDelete(ctx, this.es, element)

	case "indices_alias":
		return this.handleIndicesAlias(ctx, this.es, element)

	default:
		return fmt.Errorf("unsupported request type: %s", requestType)
	}
}

func (this *App) WriteToEs(buffer *action.Container, ctx context.Context) error {
	var cancel context.CancelFunc

	bulk := this.es.Bulk().Refresh(this.refresh)
	for _, element := range buffer.Elements() {
		bulk.Add(element)
	}

	if this.bulkTimeoutString != "" {
		bulk.Timeout(this.bulkTimeoutString)
		ctx, cancel = context.WithTimeout(ctx, this.bulkTimeout)
		defer cancel()
	}

	if err := this.doFlush(ctx, bulk, buffer); nil != err {
		return err
	}

	buffer.Clear()
	return nil
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

	if !AnyNotEmpty(this.buffers) {
		return nil
	}

	acks := make(chan int)

	for i, buffer := range this.buffers {
		go func(i int, buf *action.Container) {
			err := this.WriteToEs(buf, ctx)
			if err != nil {
				this.logger.Error(err.Error())
			}
			acks <- i
		}(i, buffer)
	}

	for i := 0; i < len(this.buffers); i++ {
		<-acks
	}

	this.rabbit.onFlush()

	// reset tracing spans
	for _, span := range this.spans {
		span.Finish()
	}

	this.spans = []tracer.Span{}

	return nil
}

func (this *App) doFlush(ctx context.Context, bulk *elastic.BulkService, buffer *action.Container) error {
	for _, retry := range retriesInterval {
		this.logger.Debug("Flushing")
		res, err := bulk.Do(ctx)

		if err != nil {
			if this.isErrorRetriable(err) {
				this.logger.Info("Sleep", zap.Duration("time", retry))
				time.Sleep(retry)
				continue
			} else {
				return err
			}
		}

		this.verboseResponse(res, buffer)

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
		this.logger.Warn("failed flushing", zap.Error(err))
	}

	return retriable
}

func (this *App) verboseResponse(res *elastic.BulkResponse, buffer *action.Container) {
	for _, rItem := range res.Items {
		for riKey, riValue := range rItem {
			if riValue.Error != nil {
				relateItems := make([]action.Element, 0, buffer.Length())

				for _, item := range buffer.Elements() {
					if item.DocType == riValue.Type && item.DocId == riValue.Id && item.Index == riValue.Index {
						relateItems = append(relateItems, item)
					}
				}

				this.logger.Error(
					"failed to process item",
					zap.String("key", riKey),
					zap.String("type", riValue.Error.Type),
					zap.String("phase", riValue.Error.Phase),
					zap.String("reason", riValue.Error.Reason),
					// zap.String("relateItems", relateItems),
				)

			}
		}
	}

	this.logger.Info(
		"bulk done",
		zap.Int("res.took", res.Took),
		zap.Int("res.items", len(res.Items)),
		zap.Bool("res.errors", res.Errors),
	)
}
