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
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"gopkg.in/olivere/elastic.v5"
)

func configugration() *Configuration {
	return &Configuration{
		Debug:        true,
		TickInterval: 3 * time.Second,
		ElasticSearch: esConfig{
			Url:     env("ELASTIC_SEARCH_URL", "http://127.0.0.1:9200/?sniff=false"),
			Refresh: "true",
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

func TestConfiguration(t *testing.T) {
	cnf := configugration()
	con, err := cnf.queueConnection()
	if err != nil {
		panic(err)
	}
	defer con.Close()

	ch, err := cnf.queueChannel(con)
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	queue(ch, cnf, "indices-create.json")
}

func TestIndicesCreate(t *testing.T) {
	ctx := context.Background()
	cnf := configugration()
	app, _, stop := cnf.App()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer func() { stop <- true }()
	defer app.rabbit.ch.QueuePurge(cnf.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Start(ctx, cnf, make(chan os.Signal), wg)
	wg.Wait()

	queue(app.rabbit.ch, cnf, "indices/indices-create.json") // queue a message to rabbitMQ
	idle(app)                                                // Wait a bit so that the message can be consumed.

	res, err := elastic.NewIndicesGetService(app.es).Index("go1_qa").Do(ctx)
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
	cnf := configugration()
	app, _, stop := cnf.App()
	wg := &sync.WaitGroup{}
	defer func() { stop <- true }()
	defer app.rabbit.ch.QueuePurge(cnf.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Start(ctx, cnf, make(chan os.Signal), wg)
	wg.Wait()

	queue(app.rabbit.ch, cnf, "indices/indices-create.json") // create the index
	queue(app.rabbit.ch, cnf, "indices/indices-drop.json")   // then, drop it.
	idle(app)                                                // Wait a bit so that the message can be consumed.

	_, err := elastic.NewIndicesGetService(app.es).Index("go1_qa").Do(ctx)
	if !strings.Contains(err.Error(), "[type=index_not_found_exception]") {
		t.Fatal("Index is not deleted successfully.")
	}
}

func TestBulkCreate(t *testing.T) {
	ctx := context.Background()
	cnf := configugration()
	app, _, stop := cnf.App()
	wg := &sync.WaitGroup{}
	defer func() { stop <- true }()
	defer app.rabbit.ch.QueuePurge(cnf.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Start(ctx, cnf, make(chan os.Signal), wg)
	wg.Wait()

	queue(app.rabbit.ch, cnf, "indices/indices-create.json") // create the index
	queue(app.rabbit.ch, cnf, "portal/portal-index.json")    // create portal object
	idle(app)                                                // Wait a bit so that the message can be consumed.

	res, _ := elastic.
		NewGetService(app.es).
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
	cnf := configugration()
	app, _, stop := cnf.App()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer func() { stop <- true }()
	defer app.rabbit.ch.QueuePurge(cnf.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Start(ctx, cnf, make(chan os.Signal), wg)
	wg.Done()

	queue(app.rabbit.ch, cnf, "indices/indices-create.json") // create the index
	queue(app.rabbit.ch, cnf, "portal/portal-index.json")    // portal.status = 1
	queue(app.rabbit.ch, cnf, "portal/portal-update.json")   // portal.status = 0
	queue(app.rabbit.ch, cnf, "portal/portal-update-2.json") // portal.status = 2
	queue(app.rabbit.ch, cnf, "portal/portal-update-3.json") // portal.status = 3
	queue(app.rabbit.ch, cnf, "portal/portal-update-4.json") // portal.status = 4
	idle(app)                                                // Wait a bit so that the message can be consumed.

	res, _ := elastic.
		NewGetService(app.es).
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
	cnf := configugration()
	app, _, stop := cnf.App()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer func() { stop <- true }()
	defer app.rabbit.ch.QueuePurge(cnf.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Start(ctx, cnf, make(chan os.Signal), wg)
	wg.Wait()

	queue(app.rabbit.ch, cnf, "indices/indices-create.json")      // create the index
	queue(app.rabbit.ch, cnf, "portal/portal-update.json")        // portal.status = 0
	queue(app.rabbit.ch, cnf, "portal/portal-update-author.json") // portal.author.name = truong
	queue(app.rabbit.ch, cnf, "portal/portal-index.json")         // portal.status = 1
	idle(app)                                                     // Wait a bit so that the message can be consumed.

	res, _ := elastic.
		NewGetService(app.es).
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
	cnf := configugration()
	app, _, stop := cnf.App()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer func() { stop <- true }()
	defer app.rabbit.ch.QueuePurge(cnf.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Start(ctx, cnf, make(chan os.Signal), wg)
	wg.Done()

	load := func() string {
		res, _ := elastic.NewGetService(app.es).
			Index("go1_qa").Routing("go1_qa").
			Type("portal").Id("111").
			FetchSource(true).
			Do(ctx)

		source, _ := json.Marshal(res.Source)

		return string(source)
	}

	// Create the portal first.
	queue(app.rabbit.ch, cnf, "indices/indices-create.json") // create the index
	queue(app.rabbit.ch, cnf, "portal/portal-index.json")    // portal.status = 1
	idle(app)

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

		for count := 1; count <= cnf.RabbitMq.PrefetchCount; count++ {
			fixture := fixtures[r.Intn(len(fixtures))]
			queue(app.rabbit.ch, cnf, fixture) // portal.status = 0
		}

		queue(app.rabbit.ch, cnf, "portal/portal-update-4.json") // portal.status = 4
		idle(app)
		portal := load()
		if !strings.Contains(portal, `"status":4`) {
			t.Error("failed to load portal document")
		}
	}
}

func TestBulkableDelete(t *testing.T) {
	ctx := context.Background()
	cnf := configugration()
	app, _, stop := cnf.App()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer func() { stop <- true }()
	defer app.rabbit.ch.QueuePurge(cnf.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Start(ctx, cnf, make(chan os.Signal), wg)
	wg.Done()

	queue(app.rabbit.ch, cnf, "indices/indices-create.json") // create the index
	queue(app.rabbit.ch, cnf, "portal/portal-index.json")    // create portal object
	queue(app.rabbit.ch, cnf, "portal/portal-delete.json")   // update portal object
	idle(app)                                                // Wait a bit so that the message can be consumed.

	_, err := elastic.
		NewGetService(app.es).
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
	app, _, stop := f.App()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer func() { stop <- true }()
	defer app.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Start(ctx, f, make(chan os.Signal), wg)
	wg.Wait()

	queue(app.rabbit.ch, f, "indices/indices-create.json")        // create the index
	queue(app.rabbit.ch, f, "portal/portal-index.json")           // create portal, status is 1
	queue(app.rabbit.ch, f, "portal/portal-update.json")          // update portal status to 0
	queue(app.rabbit.ch, f, "portal/portal-update-by-query.json") // update portal status to 2
	idle(app)                                                     // Wait a bit so that the message can be consumed.

	res, err := elastic.
		NewGetService(app.es).
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
	cnf := configugration()
	app, _, stop := cnf.App()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer func() { stop <- true }()
	defer app.rabbit.ch.QueuePurge(cnf.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Start(ctx, cnf, make(chan os.Signal), wg)
	wg.Done()

	queue(app.rabbit.ch, cnf, "indices/indices-create.json")        // create the index
	queue(app.rabbit.ch, cnf, "portal/portal-index.json")           // create portal, status is 1
	queue(app.rabbit.ch, cnf, "portal/portal-delete-by-query.json") // update portal status to 0
	idle(app)                                                       // Wait a bit so that the message can be consumed.

	_, err := elastic.
		NewGetService(app.es).
		Index("go1_qa").
		Routing("go1_qa").
		Type("portal").
		Id("111").
		FetchSource(true).
		Do(ctx)

	if err == nil {
		t.Error("should see error 404")
	} else if !strings.Contains(err.Error(), "Error 404 (Not Found)") {
		t.Error("failed to delete portal document")
	}
}

func TestCreateIndexAlias(t *testing.T) {
	ctx := context.Background()
	f := configugration()
	app, _, stop := f.App()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer func() { stop <- true }()
	defer app.rabbit.ch.QueuePurge(f.RabbitMq.QueueName, false)
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Start(ctx, f, make(chan os.Signal), wg)
	wg.Wait()

	queue(app.rabbit.ch, f, "indices/indices-create.json")
	queue(app.rabbit.ch, f, "portal/portal-index.json")
	queue(app.rabbit.ch, f, "indices/indices-alias.json")
	idle(app) // Wait a bit so that the message can be consumed.

	res, err := elastic.
		NewGetService(app.es).
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
