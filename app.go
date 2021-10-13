package es_writer

import (
	"context"
	"sync"
	"time"

	"github.com/go1com/es-writer/action"
	"github.com/streadway/amqp"
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
	Rabbit *RabbitMqInput
	buffer *action.Buffer
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
	spans             []tracer.Span
}
