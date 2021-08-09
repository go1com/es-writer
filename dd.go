package es_writer

import (
	"github.com/streadway/amqp"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func headerToTextMapCarrier(headers amqp.Table) tracer.TextMapCarrier {
	c := tracer.TextMapCarrier{}

	for k, v := range headers {
		if stringValue, ok := v.(string); ok {
			c.Set(k, stringValue)
		}
	}

	return c
}
