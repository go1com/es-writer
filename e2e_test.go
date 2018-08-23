package es_writer

import (
	"testing"
	"context"
	"time"
	"gopkg.in/olivere/elastic.v5"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/sirupsen/logrus"
	"runtime"
	"path"
	"io/ioutil"
	"strings"
	"math/rand"
	"fmt"
)

func flags() Flags {
	f := Flags{}

	url := env("RABBITMQ_URL", "amqp://go1:go1@127.0.0.1:5672/")
	f.Url = &url
	kind := env("RABBITMQ_KIND", "topic")
	f.Kind = &kind
	exchange := env("RABBITMQ_EXCHANGE", "events")
	f.Exchange = &exchange
	routingKey := env("RABBITMQ_ROUTING_KEY", "qa")
	f.RoutingKey = &routingKey
	prefetchCount := 50
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

func stand(w *Dog) {
	time.Sleep(2 * time.Second)
	defer time.Sleep(5 * time.Second)

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
		Body: fixture(file),
	})

	if err != nil {
		logrus.WithError(err).Panicln("failed to publish message")
	}
}

func fixture(filePath string) []byte {
	_, currentFileName, _, _ := runtime.Caller(1)
	filePath = path.Dir(currentFileName) + "/fixtures/" + filePath
	body, _ := ioutil.ReadFile(filePath)

	return body
}

func start(t *testing.T) (*Dog, context.Context, Flags, chan bool) {
	ctx := context.Background()
	f := flags()

	// RabbitMQ connection & flush messages
	con, _ := f.RabbitMqConnection()
	ch, _ := f.RabbitMqChannel(con)
	defer fmt.Println("=== Starting", t.Name(), " ===")
	defer time.Sleep(2 * time.Second)
	defer ch.QueuePurge(*f.QueueName, false)

	// ElasticSearch tools & drop testing index
	es, _ := f.ElasticSearchClient()
	bulk, _ := f.ElasticSearchBulkProcessor(ctx, es)
	elastic.NewIndicesDeleteService(es).Index([]string{"go1_qa"}).Do(ctx)

	dog := NewDog(ch, *f.PrefetchCount, es, bulk, *f.Debug)
	go dog.Start(ctx, f)

	stop := make(chan bool)
	go func() {
		logrus.Debugln("clean up services")
		<-stop
		go bulk.Close()
		go ch.Close()
		go con.Close()
		go elastic.NewIndicesDeleteService(es).Index([]string{"go1_qa"}).Do(ctx)

		time.Sleep(5 * time.Second)
	}()

	return dog, ctx, f, stop
}

func TestFlags(t *testing.T) {
	ctx := context.Background()
	f := flags()
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
	dog, ctx, f, stop := start(t)
	defer func() { stop <- true }()

	queue(dog.ch, f, "indices/indices-create.json") // queue a message to rabbitMQ
	stand(dog)                                      // Wait a bit so that the message can be consumed.

	res, err := elastic.NewIndicesGetService(dog.es).Index("go1_qa").Do(ctx)
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
	dog, ctx, f, stop := start(t)
	defer func() { stop <- true }()

	queue(dog.ch, f, "indices/indices-create.json") // create the index
	queue(dog.ch, f, "indices/indices-drop.json")   // then, drop it.
	stand(dog)                                      // Wait a bit so that the message can be consumed.

	_, err := elastic.NewIndicesGetService(dog.es).Index("go1_qa").Do(ctx)
	if !strings.Contains(err.Error(), "[type=index_not_found_exception]") {
		t.Fatal("Index is not deleted successfully.")
	}
}

func TestBulkCreate(t *testing.T) {
	dog, ctx, f, stop := start(t)
	defer func() { stop <- true }()

	queue(dog.ch, f, "indices/indices-create.json") // create the index
	queue(dog.ch, f, "portal/portal-index.json")    // create portal object
	stand(dog)                                      // Wait a bit so that the message can be consumed.

	stats := dog.bulk.Stats()
	if stats.Succeeded == 0 {
		t.Error("action failed to process")
	}

	res, _ := elastic.
		NewGetService(dog.es).
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

func TestBulkUpdate(t *testing.T) {
	dog, ctx, f, stop := start(t)
	defer func() { stop <- true }()

	queue(dog.ch, f, "indices/indices-create.json") // create the index
	queue(dog.ch, f, "portal/portal-index.json")    // portal.status = 1
	queue(dog.ch, f, "portal/portal-update.json")   // portal.status = 0
	queue(dog.ch, f, "portal/portal-update-2.json") // portal.status = 2
	queue(dog.ch, f, "portal/portal-update-3.json") // portal.status = 3
	queue(dog.ch, f, "portal/portal-update-4.json") // portal.status = 4
	stand(dog)                                      // Wait a bit so that the message can be consumed.

	stats := dog.bulk.Stats()
	if stats.Succeeded == 0 {
		t.Error("action failed to process", stats)
	}

	res, _ := elastic.
		NewGetService(dog.es).
		Index("go1_qa").
		Routing("go1_qa").
		Type("portal").
		Id("111").
		FetchSource(true).
		Do(ctx)

	source, _ := json.Marshal(res.Source)
	correctTitle := strings.Contains(string(source), `"title":"qa.mygo1.com"`)
	correctStatus := strings.Contains(string(source), `"status":4`)
	if !correctTitle || !correctStatus {
		t.Error("failed to load portal document")
	}
}

func TestBulkUpdateConflict(t *testing.T) {
	dog, ctx, f, stop := start(t)
	defer func() { stop <- true }()

	load := func() string {
		res, _ := elastic.NewGetService(dog.es).
			Index("go1_qa").Routing("go1_qa").
			Type("portal").Id("111").
			FetchSource(true).
			Do(ctx)

		source, _ := json.Marshal(res.Source)

		return string(source)
	}

	// Create the portal first.
	queue(dog.ch, f, "indices/indices-create.json") // create the index
	queue(dog.ch, f, "portal/portal-index.json")    // portal.status = 1
	stand(dog)

	fixtures := []string{
		"portal/portal-update.json",
		"portal/portal-update-2.json",
		"portal/portal-update-3.json",
		"portal/portal-update-4.json",
	}

	s := rand.NewSource(time.Now().Unix())
	r := rand.New(s) // initialize local pseudorandom generator
	for i := 1; i <= 5; i++ {
		fmt.Println("Round ", i)

		for count := 1; count <= *f.PrefetchCount; count++ {
			fixture := fixtures[r.Intn(len(fixtures))]
			queue(dog.ch, f, fixture) // portal.status = 0
		}

		queue(dog.ch, f, "portal/portal-update-4.json") // portal.status = 4
		stand(dog)
		portal := load()
		if !strings.Contains(portal, `"status":4`) {
			t.Error("failed to load portal document")
		}
	}
}

func TestBulkableDelete(t *testing.T) {
	dog, ctx, f, stop := start(t)
	defer func() { stop <- true }()

	queue(dog.ch, f, "indices/indices-create.json") // create the index
	queue(dog.ch, f, "portal/portal-index.json")    // create portal object
	queue(dog.ch, f, "portal/portal-delete.json")   // update portal object
	stand(dog)                                      // Wait a bit so that the message can be consumed.

	stats := dog.bulk.Stats()
	if stats.Succeeded == 0 {
		t.Error("action failed to process")
	}

	_, err := elastic.
		NewGetService(dog.es).
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

func TestUpdateByQuery(t *testing.T) {
	dog, ctx, f, stop := start(t)
	defer func() { stop <- true }()

	queue(dog.ch, f, "indices/indices-create.json")        // create the index
	queue(dog.ch, f, "portal/portal-index.json")           // create portal, status is 1
	queue(dog.ch, f, "portal/portal-update.json")          // update portal status to 0
	queue(dog.ch, f, "portal/portal-update-by-query.json") // update portal status to 2
	stand(dog)                                             // Wait a bit so that the message can be consumed.

	res, err := elastic.
		NewGetService(dog.es).
		Index("go1_qa").
		Routing("go1_qa").
		Type("portal").
		Id("111").
		FetchSource(true).
		Do(ctx)

	if err != nil {
		logrus.WithError(err).Fatalln("failed loading")
	} else {
		source, _ := json.Marshal(res.Source)
		correctTitle := strings.Contains(string(source), `"title":"qa.mygo1.com"`)
		correctStatus := strings.Contains(string(source), `"status":2`)

		if !correctTitle || !correctStatus {
			t.Error("failed to update portal document")
		}
	}
}

func TestDeleteByQuery(t *testing.T) {
	// delete_by_query
}
