package es_writer

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"

	"gopkg.in/olivere/elastic.v5"
)

func container() Container {
	ctn := Container{}

	url := env("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
	kind := env("RABBITMQ_KIND", "topic")
	exchange := env("RABBITMQ_EXCHANGE", "events")
	routingKey := env("RABBITMQ_ROUTING_KEY", "qa")
	prefetchCount := 50
	prefetchSize := 0
	tickInterval := 1 * time.Second
	queueName := "es-writer-qa"
	urlContain := ""
	urlNotContain := ""
	consumerName := ""
	esUrl := env("ELASTIC_SEARCH_URL", "http://127.0.0.1:9200/?sniff=false&tracelog=/tmp/tracelog")
	debug := true
	refresh := "true"

	ctn.Url = &url
	ctn.Kind = &kind
	ctn.Exchange = &exchange
	ctn.RoutingKey = &routingKey
	ctn.PrefetchCount = &prefetchCount
	ctn.PrefetchSize = &prefetchSize
	ctn.TickInterval = &tickInterval
	ctn.QueueName = &queueName
	ctn.UrlContain = &urlContain
	ctn.UrlNotContain = &urlNotContain
	ctn.ConsumerName = &consumerName
	ctn.EsUrl = &esUrl
	ctn.Debug = &debug
	ctn.Refresh = &refresh
	ctn.logger = zap.NewNop()

	return ctn
}

func idle(app *App) {
	for {
		units := app.buffer.Length()
		if 0 == units {
			time.Sleep(3333 * time.Millisecond)
			break
		} else {
			time.Sleep(333 * time.Millisecond)
			app.logger.Info("idle", zap.Int("units", units))
		}
	}
}

func queue(ch *amqp.Channel, ctn Container, file string) {
	msg := amqp.Publishing{Body: fixture(file)}
	err := ch.Publish(*ctn.Exchange, *ctn.RoutingKey, false, false, msg)

	if err != nil {
		ctn.logger.Panic("failed to publish message", zap.Error(err))
	}
}

func fixture(filePath string) []byte {
	_, currentFileName, _, _ := runtime.Caller(1)
	filePath = path.Dir(currentFileName) + "/fixtures/" + filePath
	body, _ := ioutil.ReadFile(filePath)

	return body
}

func TestBulk(t *testing.T) {
	ass := assert.New(t)
	ctx := context.Background()
	ctn := container()
	app, _, stop := ctn.App()
	purge := func() { app.rabbit.ch.QueuePurge(*ctn.QueueName, false) }
	defer func() { stop <- true }()
	defer purge()
	defer elastic.NewIndicesDeleteService(app.es).Index([]string{"go1_qa"}).Do(ctx)
	go app.Run(ctx, ctn)
	time.Sleep(333 * time.Millisecond)

	t.Run("index", func(t *testing.T) {
		t.Run("create", func(t *testing.T) {
			purge()
			queue(app.rabbit.ch, ctn, "indices/indices-create.json") // queue a message to rabbitMQ
			idle(app)                                                // Wait a bit so that the message can be consumed.

			res, err := elastic.NewIndicesGetService(app.es).Index("go1_qa").Do(ctx)

			ass.NoError(err)

			response := res["go1_qa"]
			expecting := `{"portal":{"_routing":{"required":true},"properties":{"author":{"properties":{"email":{"type":"text"},"name":{"type":"text"}}},"id":{"type":"keyword"},"name":{"type":"keyword"},"status":{"type":"short"},"title":{"fields":{"analyzed":{"type":"text"}},"type":"keyword"}}}}`
			actual, _ := json.Marshal(response.Mappings)
			ass.Equal(expecting, string(actual))
		})

		t.Run("delete", func(t *testing.T) {
			purge()
			queue(app.rabbit.ch, ctn, "indices/indices-create.json") // create the index
			queue(app.rabbit.ch, ctn, "indices/indices-drop.json")   // then, drop it.
			idle(app)                                                // Wait a bit so that the message can be consumed.

			_, err := elastic.NewIndicesGetService(app.es).Index("go1_qa").Do(ctx)

			ass.Error(err)
			ass.Contains(err.Error(), "[type=index_not_found_exception]", "Index is not deleted successfully.")
		})

		t.Run("alias", func(t *testing.T) {
			purge()
			queue(app.rabbit.ch, ctn, "indices/indices-create.json")
			queue(app.rabbit.ch, ctn, "portal/portal-index.json")
			queue(app.rabbit.ch, ctn, "indices/indices-alias.json")
			idle(app) // Wait a bit so that the message can be consumed.

			res, err := elastic.
				NewGetService(app.es).
				Index("qa"). // Use 'qa', not 'go1_qa'
				Routing("go1_qa").
				Type("portal").
				Id("111").
				FetchSource(true).
				Do(ctx)

			ass.NoError(err)

			source, _ := json.Marshal(res.Source)
			ass.Contains(string(source), `"title":"qa.mygo1.com"`)
			ass.Contains(string(source), `"status":1`)
		})
	})

	t.Run("bulk", func(t *testing.T) {
		t.Run("trace", func(t *testing.T) {
			mt := mocktracer.Start()
			defer mt.Stop()

			purge()
			queue(app.rabbit.ch, ctn, "indices/indices-create.json") // create the index
			queue(app.rabbit.ch, ctn, "portal/portal-index.json")    // create portal object
			idle(app)                                                // Wait a bit so that the message can be consumed.

			spans := mt.FinishedSpans()
			for _, span := range spans {
				tags := span.Tags()
				ass.Equal("qa", tags["message.routingKey"])
			}

			ass.Equal(2, len(spans))
		})

		t.Run("create", func(t *testing.T) {
			purge()
			queue(app.rabbit.ch, ctn, "indices/indices-create.json") // create the index
			queue(app.rabbit.ch, ctn, "portal/portal-index.json")    // create portal object
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
		})

		t.Run("update", func(t *testing.T) {
			purge()
			queue(app.rabbit.ch, ctn, "indices/indices-create.json") // create the index
			queue(app.rabbit.ch, ctn, "portal/portal-index.json")    // portal.status = 1
			queue(app.rabbit.ch, ctn, "portal/portal-update.json")   // portal.status = 0
			queue(app.rabbit.ch, ctn, "portal/portal-update-2.json") // portal.status = 2
			queue(app.rabbit.ch, ctn, "portal/portal-update-3.json") // portal.status = 3
			queue(app.rabbit.ch, ctn, "portal/portal-update-4.json") // portal.status = 4
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
		})

		t.Run("update conflict", func(t *testing.T) {
			purge()
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
			queue(app.rabbit.ch, ctn, "indices/indices-create.json") // create the index
			queue(app.rabbit.ch, ctn, "portal/portal-index.json")    // portal.status = 1
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

				for count := 1; count <= *ctn.PrefetchCount; count++ {
					fixture := fixtures[r.Intn(len(fixtures))]
					queue(app.rabbit.ch, ctn, fixture) // portal.status = 0
				}

				queue(app.rabbit.ch, ctn, "portal/portal-update-4.json") // portal.status = 4
				idle(app)
				portal := load()
				if !strings.Contains(portal, `"status":4`) {
					t.Error("failed to load portal document")
					break
				}
			}
		})

		t.Run("update by query", func(t *testing.T) {
			purge()
			queue(app.rabbit.ch, ctn, "indices/indices-create.json")        // create the index
			queue(app.rabbit.ch, ctn, "portal/portal-index.json")           // create portal, status is 1
			queue(app.rabbit.ch, ctn, "portal/portal-update.json")          // update portal status to 0
			queue(app.rabbit.ch, ctn, "portal/portal-update-by-query.json") // update portal status to 2
			idle(app)                                                       // Wait a bit so that the message can be consumed.

			res, err := elastic.
				NewGetService(app.es).
				Index("go1_qa").
				Routing("go1_qa").
				Type("portal").
				Id("111").
				FetchSource(true).
				Do(ctx)

			ass.NoError(err)
			source, _ := json.Marshal(res.Source)
			ass.Contains(string(source), `"title":"qa.mygo1.com"`)
			ass.Contains(string(source), `"status":2`)
		})

		t.Run("update graceful", func(t *testing.T) {
			purge()
			queue(app.rabbit.ch, ctn, "indices/indices-create.json")      // create the index
			queue(app.rabbit.ch, ctn, "portal/portal-update.json")        // portal.status = 0
			queue(app.rabbit.ch, ctn, "portal/portal-update-author.json") // portal.author.name = truong
			queue(app.rabbit.ch, ctn, "portal/portal-index.json")         // portal.status = 1
			idle(app)                                                     // Wait a bit so that the message can be consumed.

			res, err := elastic.
				NewGetService(app.es).
				Index("go1_qa").
				Routing("go1_qa").
				Type("portal").
				Id("111").
				FetchSource(true).
				Do(ctx)

			ass.NoError(err)
			source, _ := json.Marshal(res.Source)
			ass.Contains(string(source), `"title":"qa.mygo1.com"`)
			ass.Contains(string(source), `"status":1`)
		})

		t.Run("delete", func(t *testing.T) {
			purge()
			queue(app.rabbit.ch, ctn, "indices/indices-create.json") // create the index
			queue(app.rabbit.ch, ctn, "portal/portal-index.json")    // create portal object
			queue(app.rabbit.ch, ctn, "portal/portal-delete.json")   // update portal object
			idle(app)                                                // Wait a bit so that the message can be consumed.

			_, err := elastic.
				NewGetService(app.es).
				Index("go1_qa").
				Routing("go1_qa").
				Type("portal").
				Id("111").
				FetchSource(true).
				Do(ctx)

			ass.Error(err)
			ass.Contains(err.Error(), "Error 404 (Not Found)", "failed to delete portal")
		})

		t.Run("delete by query", func(t *testing.T) {
			purge()
			queue(app.rabbit.ch, ctn, "indices/indices-create.json")        // create the index
			queue(app.rabbit.ch, ctn, "portal/portal-index.json")           // create portal, status is 1
			queue(app.rabbit.ch, ctn, "portal/portal-delete-by-query.json") // update portal status to 0
			idle(app)                                                       // Wait a bit so that the message can be consumed.

			_, err := elastic.
				NewGetService(app.es).
				Index("go1_qa").
				Routing("go1_qa").
				Type("portal").
				Id("111").
				FetchSource(true).
				Do(ctx)

			ass.Error(err)
			ass.Contains(err.Error(), "Error 404 (Not Found)", "failed to delete portal document")
		})
	})
}
