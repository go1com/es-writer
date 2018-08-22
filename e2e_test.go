package es_writer

import (
	"testing"
	"context"
	"github.com/streadway/amqp"
	"io/ioutil"
	"runtime"
	"path"
	"github.com/Sirupsen/logrus"
	"time"
	"gopkg.in/olivere/elastic.v5"
	"encoding/json"
)

func NewFlagsForTest() Flags {
	f := Flags{}

	Url := "amqp://go1:go1@127.0.0.1:5672/"
	f.Url = &Url
	Kind := "topic"
	f.Kind = &Kind
	Exchange := "events"
	f.Exchange = &Exchange
	RoutingKey := "wip"
	f.RoutingKey = &RoutingKey
	PrefetchCount := 5
	f.PrefetchCount = &PrefetchCount
	PrefetchSize := 0
	f.PrefetchSize = &PrefetchSize
	TickInterval := 3 * time.Second
	f.TickInterval = &TickInterval
	QueueName := "es-writer-qa"
	f.QueueName = &QueueName
	ConsumerName := "es-writer-qa-consumer"
	f.ConsumerName = &ConsumerName
	EsUrl := "http://127.0.0.1:9200/?sniff=false"
	f.EsUrl = &EsUrl

	return f
}

func TestFlags(t *testing.T) {
	ctx := context.Background()
	f := NewFlagsForTest()
	con, err := f.RabbitMqConnection()
	if err != nil {
		t.Fatalf("failed to make rabbitMQ connection: %s", err.Error())
	} else {
		defer con.Close()
	}

	ch, err := f.RabbitMqChannel(con)
	if err != nil {
		t.Fatalf("failed to make rabbitMQ channel: %s", err.Error())
	} else {
		defer ch.Close()
	}

	es, err := f.ElasticSearchClient()
	if err != nil {
		t.Fatalf("failed to make ElasticSearch client: %s", err.Error())
	}

	bulk, err := f.ElasticSearchBulkProcessor(ctx, es)
	if err != nil {
		t.Fatalf("failed to make ElasticSearch bulk processor: %s", err.Error())
	} else {
		defer bulk.Close()
	}
}

func TestIndicesCreate(t *testing.T) {
	ctx := context.Background()
	f := NewFlagsForTest()

	con, _ := f.RabbitMqConnection()
	defer con.Close()
	ch, _ := f.RabbitMqChannel(con)
	defer ch.Close()
	es, _ := f.ElasticSearchClient()
	bulk, _ := f.ElasticSearchBulkProcessor(ctx, es)
	watcher := NewWatcher(ch, *f.PrefetchCount, es, bulk)
	go watcher.Watch(ctx, f)

	removeIndex := func() {
		elastic.
			NewIndicesDeleteService(es).Index([]string{"go1_qa"}).
			Do(ctx)
	}

	removeIndex()       // Delete index before testing
	defer removeIndex() // Clean up index after testing

	queue(ch, f, "indices/indices-create.json") // queue a message to rabbitMQ

	// Wait a bit so that the message can be consumed.
	time.Sleep(2 * time.Second)
	for {
		if 0 == watcher.UnitWorks() {
			break
		}

		time.Sleep(1 * time.Second)
	}

	res, err := elastic.NewIndicesGetService(es).
		Index("go1_qa").
		Do(ctx)

	if err != nil {
		t.Fatal("can't get new index: " + err.Error())
	}

	response := res["go1_qa"]
	expecting := `{"portal":{"_routing":{"required":true},"properties":{"id":{"type":"keyword"},"name":{"type":"keyword"},"status":{"type":"short"},"title":{"fields":{"analyzed":{"type":"text"}},"type":"keyword"}}}}`
	actual, _ := json.Marshal(response.Mappings)
	if expecting != string(actual) {
		t.Fatal("failed")
	}
}

func queue(ch *amqp.Channel, f Flags, file string) {
	err := ch.Publish(*f.Exchange, *f.RoutingKey, false, false, amqp.Publishing{
		Body: realpath(file),
	})

	if err != nil {
		logrus.WithError(err).Panicln("failed to publish message")
	}
}

func realpath(filePath string) []byte {
	_, currentFileName, _, _ := runtime.Caller(1)
	filePath = path.Dir(currentFileName) + "/fixtures/" + filePath
	body, _ := ioutil.ReadFile(filePath)

	return body
}
