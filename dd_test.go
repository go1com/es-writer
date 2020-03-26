package es_writer

import (
	"fmt"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func Test_HeaderToTextMapCarrier(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	headers := amqp.Table{
		"x-datadog-parent-id":         "6043159913221173111",
		"x-datadog-trace-id":          "6043159913221173690",
		"x-datadog-sampling-priority": "1",
	}

	ass := assert.New(t)
	carrier := HeaderToTextMapCarrier(headers)
	spanCtx, err := tracer.Extract(carrier)

	ass.NoError(err)
	ass.Equal(headers["x-datadog-trace-id"], fmt.Sprint(spanCtx.TraceID()))
	ass.Equal(headers["x-datadog-parent-id"], fmt.Sprint(spanCtx.SpanID()))
}
