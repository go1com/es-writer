package es_writer

import (
	"testing"
	"context"
	"time"
	"gopkg.in/olivere/elastic.v5"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/Sirupsen/logrus"
	"runtime"
	"path"
	"io/ioutil"
	"strings"
)

func newFlagsForTest() Flags {
	f := Flags{}

	url := env("RABBITMQ_URL", "amqp://go1:go1@127.0.0.1:5672/")
	f.Url = &url
	kind := env("RABBITMQ_KIND", "topic")
	f.Kind = &kind
	exchange := env("RABBITMQ_EXCHANGE", "events")
	f.Exchange = &exchange
	routingKey := env("RABBITMQ_ROUTING_KEY", "qa")
	f.RoutingKey = &routingKey
	prefetchCount := 5
	f.PrefetchCount = &prefetchCount
	prefetchSize := 0
	f.PrefetchSize = &prefetchSize
	tickInterval := 3 * time.Second
	f.TickInterval = &tickInterval
	queueName := "es-writer-qa"
	f.QueueName = &queueName
	ConsumerName := "es-writer-qa-consumer"
	f.ConsumerName = &ConsumerName
	esUrl := env("ELASTIC_SEARCH_URL", "http://127.0.0.1:9200/?sniff=false")
	f.EsUrl = &esUrl
	debug := true
	f.Debug = &debug

	return f
}

func waitForCompletion(w *Watcher) {
	time.Sleep(2 * time.Second)

	for {
		units := w.UnitWorks()
		if 0 == units {
			break
		} else {
			logrus.Infof("Remaining actions: %d\n", units)
		}

		time.Sleep(2 * time.Second)
	}
}

func queue(ch *amqp.Channel, f Flags, file string) {
	err := ch.Publish(*f.Exchange, *f.RoutingKey, false, false, amqp.Publishing{
		Body: fileGetContent(file),
	})

	if err != nil {
		logrus.WithError(err).Panicln("failed to publish message")
	}
}

func fileGetContent(filePath string) []byte {
	_, currentFileName, _, _ := runtime.Caller(1)
	filePath = path.Dir(currentFileName) + "/fixtures/" + filePath
	body, _ := ioutil.ReadFile(filePath)

	return body
}

func TestFlags(t *testing.T) {
	ctx := context.Background()
	f := newFlagsForTest()
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
	f := newFlagsForTest()
	con, _ := f.RabbitMqConnection()
	defer con.Close()
	ch, _ := f.RabbitMqChannel(con)
	defer ch.Close()
	es, _ := f.ElasticSearchClient()
	bulk, _ := f.ElasticSearchBulkProcessor(ctx, es)
	defer bulk.Close()
	watcher := NewWatcher(ch, *f.PrefetchCount, es, bulk, *f.Debug)
	go watcher.Watch(ctx, f)

	removeIndex := func() {
		elastic.
			NewIndicesDeleteService(es).Index([]string{"go1_qa"}).
			Do(ctx)
	}

	removeIndex()                               // Delete index before testing
	defer removeIndex()                         // Clean up index after testing
	queue(ch, f, "indices/indices-create.json") // queue a message to rabbitMQ
	waitForCompletion(watcher)                  // Wait a bit so that the message can be consumed.

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

func TestIndicesDelete(t *testing.T) {
	ctx := context.Background()
	f := newFlagsForTest()
	con, _ := f.RabbitMqConnection()
	defer con.Close()
	ch, _ := f.RabbitMqChannel(con)
	defer ch.Close()
	es, _ := f.ElasticSearchClient()
	bulk, _ := f.ElasticSearchBulkProcessor(ctx, es)
	defer bulk.Close()
	watcher := NewWatcher(ch, *f.PrefetchCount, es, bulk, *f.Debug)
	go watcher.Watch(ctx, f)

	queue(ch, f, "indices/indices-create.json") // create the index
	queue(ch, f, "indices/indices-drop.json")   // then, drop it.
	waitForCompletion(watcher)                  // Wait a bit so that the message can be consumed.

	_, err := elastic.NewIndicesGetService(es).Index("go1_qa").Do(ctx)
	if !strings.Contains(err.Error(), "[type=index_not_found_exception]") {
		t.Fatal("Index is not deleted successfully.")
	}
}

func TestCreate(t *testing.T) {
	ctx := context.Background()
	f := newFlagsForTest()
	con, _ := f.RabbitMqConnection()
	defer con.Close()
	ch, _ := f.RabbitMqChannel(con)
	defer ch.Close()
	es, _ := f.ElasticSearchClient()
	bulk, _ := f.ElasticSearchBulkProcessor(ctx, es)
	defer bulk.Close()
	watcher := NewWatcher(ch, *f.PrefetchCount, es, bulk, *f.Debug)
	go watcher.Watch(ctx, f)

	queue(ch, f, "indices/indices-drop.json")       // Delete index before testing
	queue(ch, f, "indices/indices-create.json")     // create the index
	queue(ch, f, "portal/portal-index.json")        // create portal object
	defer queue(ch, f, "indices/indices-drop.json") // then, drop it.
	waitForCompletion(watcher)                      // Wait a bit so that the message can be consumed.

	stats := bulk.Stats()
	if stats.Succeeded == 0 {
		t.Error("action failed to process")
	}

	res, _ := elastic.
		NewGetService(es).
		Index("go1_qa").
		Routing("go1_qa").
		Type("portal").
		Id("111").
		FetchSource(true).
		Do(ctx)

	source, _ := json.Marshal(res.Source)
	correctTitle := strings.Contains(string(source), `"title":"qa.mygo1.com"`)
	correctStatus := strings.Contains(string(source), `"status":1`)
	if !correctTitle || !correctStatus {
		t.Error("failed to load portal document")
	}
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	f := newFlagsForTest()
	con, _ := f.RabbitMqConnection()
	defer con.Close()
	ch, _ := f.RabbitMqChannel(con)
	defer ch.Close()
	es, _ := f.ElasticSearchClient()
	bulk, _ := f.ElasticSearchBulkProcessor(ctx, es)
	defer bulk.Close()
	watcher := NewWatcher(ch, *f.PrefetchCount, es, bulk, *f.Debug)
	go watcher.Watch(ctx, f)

	queue(ch, f, "indices/indices-drop.json")       // Delete index before testing
	queue(ch, f, "indices/indices-create.json")     // create the index
	queue(ch, f, "portal/portal-index.json")        // create portal object
	queue(ch, f, "portal/portal-update.json")       // update portal object
	defer queue(ch, f, "indices/indices-drop.json") // then, drop it.
	waitForCompletion(watcher)                      // Wait a bit so that the message can be consumed.

	stats := bulk.Stats()
	if stats.Succeeded == 0 {
		t.Error("action failed to process")
	}

	res, _ := elastic.
		NewGetService(es).
		Index("go1_qa").
		Routing("go1_qa").
		Type("portal").
		Id("111").
		FetchSource(true).
		Do(ctx)

	source, _ := json.Marshal(res.Source)
	correctTitle := strings.Contains(string(source), `"title":"qa.mygo1.com"`)
	correctStatus := strings.Contains(string(source), `"status":0`)
	if !correctTitle || !correctStatus {
		t.Error("failed to load portal document")
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	f := newFlagsForTest()
	con, _ := f.RabbitMqConnection()
	defer con.Close()
	ch, _ := f.RabbitMqChannel(con)
	defer ch.Close()
	es, _ := f.ElasticSearchClient()
	bulk, _ := f.ElasticSearchBulkProcessor(ctx, es)
	defer bulk.Close()
	watcher := NewWatcher(ch, *f.PrefetchCount, es, bulk, *f.Debug)
	go watcher.Watch(ctx, f)

	queue(ch, f, "indices/indices-drop.json")       // Delete index before testing
	queue(ch, f, "indices/indices-create.json")     // create the index
	queue(ch, f, "portal/portal-index.json")        // create portal object
	queue(ch, f, "portal/portal-delete.json")       // update portal object
	defer queue(ch, f, "indices/indices-drop.json") // then, drop it.
	waitForCompletion(watcher)                      // Wait a bit so that the message can be consumed.

	stats := bulk.Stats()
	if stats.Succeeded == 0 {
		t.Error("action failed to process")
	}

	_, err := elastic.
		NewGetService(es).
		Index("go1_qa").
		Routing("go1_qa").
		Type("portal").
		Id("111").
		FetchSource(true).
		Do(ctx)

	if !strings.Contains(err.Error(), "Error 404 (Not Found)") {
		t.Error("failed to delete portal")
	}
}

// update_by_query
// delete_by_query
