package es_writer

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"gopkg.in/olivere/elastic.v5"
)

func flags() Flags {
	f := Flags{}

	url := env("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
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
	ConsumerName := ""
	f.ConsumerName = &ConsumerName
	esUrl := env("ELASTIC_SEARCH_URL", "http://127.0.0.1:9200/?sniff=false")
	f.EsUrl = &esUrl
	debug := true
	f.Debug = &debug
	refresh := "true"
	f.Refresh = &refresh

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

func TestFlags(t *testing.T) {
	f := flags()
	con, _ := f.queueConnection()
	defer con.Close()
	ch, _ := f.queueChannel(con)
	defer ch.Close()

	queue(ch, f, "indices-create.json")
}

func TestIndicesCreate(t *testing.T) {
	ctx := context.Background()
	f := flags()
	dog, _, stop := f.Dog()
	defer func() { stop <- true }()
	defer dog.ch.QueuePurge(*f.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.ch, f, "indices/indices-create.json") // queue a message to rabbitMQ
	stand(dog)                                      // Wait a bit so that the message can be consumed.

	res, err := elastic.NewIndicesGetService(dog.es).Index("go1_qa").Do(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	response := res["go1_qa"]
	expecting := `{"portal":{"_routing":{"required":true},"properties":{"author":{"properties":{"email":{"type":"text"},"name":{"type":"text"}}},"id":{"type":"keyword"},"name":{"type":"keyword"},"status":{"type":"short"},"title":{"fields":{"analyzed":{"type":"text"}},"type":"keyword"}}}}`
	actual, _ := json.Marshal(response.Mappings)
	if expecting != string(actual) {
		t.Fatal("failed")
	}
}

func TestIndicesDelete(t *testing.T) {
	ctx := context.Background()
	f := flags()
	dog, _, stop := f.Dog()
	defer func() { stop <- true }()
	defer dog.ch.QueuePurge(*f.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.ch, f, "indices/indices-create.json") // create the index
	queue(dog.ch, f, "indices/indices-drop.json")   // then, drop it.
	stand(dog)                                      // Wait a bit so that the message can be consumed.

	_, err := elastic.NewIndicesGetService(dog.es).Index("go1_qa").Do(ctx)
	if !strings.Contains(err.Error(), "[type=index_not_found_exception]") {
		t.Fatal("Index is not deleted successfully.")
	}
}

func TestBulkCreate(t *testing.T) {
	ctx := context.Background()
	f := flags()
	dog, _, stop := f.Dog()
	defer func() { stop <- true }()
	defer dog.ch.QueuePurge(*f.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.ch, f, "indices/indices-create.json") // create the index
	queue(dog.ch, f, "portal/portal-index.json")    // create portal object
	stand(dog)                                      // Wait a bit so that the message can be consumed.

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
	ctx := context.Background()
	f := flags()
	dog, _, stop := f.Dog()
	defer func() { stop <- true }()
	defer dog.ch.QueuePurge(*f.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.ch, f, "indices/indices-create.json") // create the index
	queue(dog.ch, f, "portal/portal-index.json")    // portal.status = 1
	queue(dog.ch, f, "portal/portal-update.json")   // portal.status = 0
	queue(dog.ch, f, "portal/portal-update-2.json") // portal.status = 2
	queue(dog.ch, f, "portal/portal-update-3.json") // portal.status = 3
	queue(dog.ch, f, "portal/portal-update-4.json") // portal.status = 4
	stand(dog)                                      // Wait a bit so that the message can be consumed.

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

func TestGracefulUpdate(t *testing.T) {
	ctx := context.Background()
	f := flags()
	dog, _, stop := f.Dog()
	defer func() { stop <- true }()
	defer dog.ch.QueuePurge(*f.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.ch, f, "indices/indices-create.json")      // create the index
	queue(dog.ch, f, "portal/portal-update.json")        // portal.status = 0
	queue(dog.ch, f, "portal/portal-update-author.json") // portal.author.name = truong
	queue(dog.ch, f, "portal/portal-index.json")         // portal.status = 1
	stand(dog)                                           // Wait a bit so that the message can be consumed.

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
		t.Error("failed gracefully update")
	}
}

func TestBulkUpdateConflict(t *testing.T) {
	ctx := context.Background()
	f := flags()
	dog, _, stop := f.Dog()
	defer func() { stop <- true }()
	defer dog.ch.QueuePurge(*f.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

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
	ctx := context.Background()
	f := flags()
	dog, _, stop := f.Dog()
	defer func() { stop <- true }()
	defer dog.ch.QueuePurge(*f.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.ch, f, "indices/indices-create.json") // create the index
	queue(dog.ch, f, "portal/portal-index.json")    // create portal object
	queue(dog.ch, f, "portal/portal-delete.json")   // update portal object
	stand(dog)                                      // Wait a bit so that the message can be consumed.

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
	ctx := context.Background()
	f := flags()
	dog, _, stop := f.Dog()
	defer func() { stop <- true }()
	defer dog.ch.QueuePurge(*f.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

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
	ctx := context.Background()
	f := flags()
	dog, _, stop := f.Dog()
	defer func() { stop <- true }()
	defer dog.ch.QueuePurge(*f.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.ch, f, "indices/indices-create.json")        // create the index
	queue(dog.ch, f, "portal/portal-index.json")           // create portal, status is 1
	queue(dog.ch, f, "portal/portal-delete-by-query.json") // update portal status to 0
	stand(dog)                                             // Wait a bit so that the message can be consumed.

	_, err := elastic.
		NewGetService(dog.es).
		Index("go1_qa").
		Routing("go1_qa").
		Type("portal").
		Id("111").
		FetchSource(true).
		Do(ctx)

	if !strings.Contains(err.Error(), "Error 404 (Not Found)") {
		t.Error("failed to delete portal document")
	}
}

func TestCreateIndexAlias(t *testing.T) {
	ctx := context.Background()
	f := flags()
	dog, _, stop := f.Dog()
	defer func() { stop <- true }()
	defer dog.ch.QueuePurge(*f.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.ch, f, "indices/indices-create.json")
	queue(dog.ch, f, "portal/portal-index.json")
	queue(dog.ch, f, "indices/indices-alias.json")
	stand(dog) // Wait a bit so that the message can be consumed.

	res, err := elastic.
		NewGetService(dog.es).
		Index("qa"). // Use 'qa', not 'go1_qa'
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
		correctStatus := strings.Contains(string(source), `"status":1`)

		if !correctTitle || !correctStatus {
			t.Error("failed to update portal document")
		}
	}
}
