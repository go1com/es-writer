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

func configugration() *Configuration {
	return &Configuration{
		TickInterval: 3 * time.Second,
		Debug:        true,
		ElasticSearch: esConfig{
			Url: env("ELASTIC_SEARCH_URL", "http://127.0.0.1:9200/?sniff=false"),
		},
		RabbitMq: rabbitmqConfig{
			Url:           env("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/"),
			Kind:          env("RABBITMQ_KIND", "topic"),
			Exchange:      env("RABBITMQ_EXCHANGE", "events"),
			RoutingKey:    env("RABBITMQ_ROUTING_KEY", "qa"),
			PrefetchCount: 0,
			QueueName:     "es-writer-qa",
		},
	}
}

func idle(w *App) {
	time.Sleep(2 * time.Second)
	defer time.Sleep(5 * time.Second)

	for {
		units := w.buffer.Length()
		if 0 == units {
			break
		} else {
			logrus.Infof("Remaining buffer: %d\n", units)
		}

		time.Sleep(2 * time.Second)
	}
}

func queue(ch *amqp.Channel, c *Configuration, file string) {
	err := ch.Publish(c.RabbitMq.Exchange, c.RabbitMq.RoutingKey, false, false, amqp.Publishing{
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
	f := configugration()
	con, _ := f.queueConnection()
	defer con.Close()
	ch, _ := f.queueChannel(con)
	defer ch.Close()

	queue(ch, f, "indices-create.json")
}

func TestIndicesCreate(t *testing.T) {
	ctx := context.Background()
	c := configugration()
	writer, _, stop := c.App()
	defer func() { stop <- true }()
	defer writer.rabbit.ch.QueuePurge(c.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(writer.es).Index([]string{"go1_qa"}).Do(ctx)
	go writer.Start(ctx, c, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(writer.rabbit.ch, c, "indices/indices-create.json") // queue a message to rabbitMQ
	idle(writer)                                              // Wait a bit so that the message can be consumed.

	res, err := elastic.NewIndicesGetService(writer.es).Index("go1_qa").Do(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	response := res["go1_qa"]
	expecting := `{"portal":{"_routing":{"required":true},"properties":{"author":{"properties":{"email":{"type":"text"},"name":{"type":"text"}}},"id":{"type":"keyword"},"name":{"type":"keyword"},"status":{"type":"short"},"title":{"fields":{"analyzed":{"type":"text"}},"type":"keyword"}}}}`
	actual, _ := json.Marshal(response.Mappings)

	if expecting != string(actual) {
		t.Fail()
	}
}

func TestIndicesDelete(t *testing.T) {
	ctx := context.Background()
	f := configugration()
	dog, _, stop := f.App()
	defer func() { stop <- true }()
	defer dog.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.rabbit.ch, f, "indices/indices-create.json") // create the index
	queue(dog.rabbit.ch, f, "indices/indices-drop.json")   // then, drop it.
	idle(dog)                                              // Wait a bit so that the message can be consumed.

	_, err := elastic.NewIndicesGetService(dog.es).Index("go1_qa").Do(ctx)
	if !strings.Contains(err.Error(), "[type=index_not_found_exception]") {
		t.Fatal("Index is not deleted successfully.")
	}
}

func TestBulkCreate(t *testing.T) {
	ctx := context.Background()
	f := configugration()
	dog, _, stop := f.App()
	defer func() { stop <- true }()
	defer dog.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.rabbit.ch, f, "indices/indices-create.json") // create the index
	queue(dog.rabbit.ch, f, "portal/portal-index.json")    // create portal object
	idle(dog)                                              // Wait a bit so that the message can be consumed.

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

func TestBulkableUpdate(t *testing.T) {
	ctx := context.Background()
	f := configugration()
	dog, _, stop := f.App()
	defer func() { stop <- true }()
	defer dog.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.rabbit.ch, f, "indices/indices-create.json") // create the index
	queue(dog.rabbit.ch, f, "portal/portal-index.json")    // portal.status = 1
	queue(dog.rabbit.ch, f, "portal/portal-update.json")   // portal.status = 0
	queue(dog.rabbit.ch, f, "portal/portal-update-2.json") // portal.status = 2
	queue(dog.rabbit.ch, f, "portal/portal-update-3.json") // portal.status = 3
	queue(dog.rabbit.ch, f, "portal/portal-update-4.json") // portal.status = 4
	idle(dog)                                              // Wait a bit so that the message can be consumed.

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
	f := configugration()
	dog, _, stop := f.App()
	defer func() { stop <- true }()
	defer dog.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.rabbit.ch, f, "indices/indices-create.json")      // create the index
	queue(dog.rabbit.ch, f, "portal/portal-update.json")        // portal.status = 0
	queue(dog.rabbit.ch, f, "portal/portal-update-author.json") // portal.author.name = truong
	queue(dog.rabbit.ch, f, "portal/portal-index.json")         // portal.status = 1
	idle(dog)                                                   // Wait a bit so that the message can be consumed.

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
	f := configugration()
	dog, _, stop := f.App()
	defer func() { stop <- true }()
	defer dog.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
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
	queue(dog.rabbit.ch, f, "indices/indices-create.json") // create the index
	queue(dog.rabbit.ch, f, "portal/portal-index.json")    // portal.status = 1
	idle(dog)

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

		for count := 1; count <= f.RabbitMq.PrefetchCount; count++ {
			fixture := fixtures[r.Intn(len(fixtures))]
			queue(dog.rabbit.ch, f, fixture) // portal.status = 0
		}

		queue(dog.rabbit.ch, f, "portal/portal-update-4.json") // portal.status = 4
		idle(dog)
		portal := load()
		if !strings.Contains(portal, `"status":4`) {
			t.Error("failed to load portal document")
		}
	}
}

func TestBulkableDelete(t *testing.T) {
	ctx := context.Background()
	f := configugration()
	dog, _, stop := f.App()
	defer func() { stop <- true }()
	defer dog.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.rabbit.ch, f, "indices/indices-create.json") // create the index
	queue(dog.rabbit.ch, f, "portal/portal-index.json")    // create portal object
	queue(dog.rabbit.ch, f, "portal/portal-delete.json")   // update portal object
	idle(dog)                                              // Wait a bit so that the message can be consumed.

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
	f := configugration()
	dog, _, stop := f.App()
	defer func() { stop <- true }()
	defer dog.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.rabbit.ch, f, "indices/indices-create.json")        // create the index
	queue(dog.rabbit.ch, f, "portal/portal-index.json")           // create portal, status is 1
	queue(dog.rabbit.ch, f, "portal/portal-update.json")          // update portal status to 0
	queue(dog.rabbit.ch, f, "portal/portal-update-by-query.json") // update portal status to 2
	idle(dog)                                                     // Wait a bit so that the message can be consumed.

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
	f := configugration()
	dog, _, stop := f.App()
	defer func() { stop <- true }()
	defer dog.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.rabbit.ch, f, "indices/indices-create.json")        // create the index
	queue(dog.rabbit.ch, f, "portal/portal-index.json")           // create portal, status is 1
	queue(dog.rabbit.ch, f, "portal/portal-delete-by-query.json") // update portal status to 0
	idle(dog)                                                     // Wait a bit so that the message can be consumed.

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
	f := configugration()
	dog, _, stop := f.App()
	defer func() { stop <- true }()
	defer dog.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(dog.es).Index([]string{"go1_qa"}).Do(ctx)
	go dog.Start(ctx, f, make(chan os.Signal))
	time.Sleep(3 * time.Second)

	queue(dog.rabbit.ch, f, "indices/indices-create.json")
	queue(dog.rabbit.ch, f, "portal/portal-index.json")
	queue(dog.rabbit.ch, f, "indices/indices-alias.json")
	idle(dog) // Wait a bit so that the message can be consumed.

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
