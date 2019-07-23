package es_writer

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/olivere/elastic.v5/config"

	"github.com/go1com/es-writer/action"
)

var (
	bufferMutext    sync.Mutex
	retriesInterval = []time.Duration{
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
		60 * time.Second,
		90 * time.Second,
		90 * time.Second,
		90 * time.Second,
	}
)

type Flags struct {
	Url           *string
	Kind          *string
	Exchange      *string
	RoutingKey    *string
	PrefetchCount *int
	PrefetchSize  *int
	TickInterval  *time.Duration
	QueueName     *string
	UrlContain    *string
	UrlNotContain *string
	ConsumerName  *string
	EsUrl         *string
	AdminPort     *string
	Debug         *bool
	Refresh       *string
}

func env(key string, defaultValue string) string {
	value, _ := os.LookupEnv(key)

	if "" == value {
		return defaultValue
	}

	return value
}

func NewFlags() Flags {
	var (
		duration       = env("DURATION", "5")
		iDuration, err = strconv.ParseInt(duration, 10, 64)
	)
	if err != nil {
		logrus.WithError(err).Panicln("Duration is invalid.")
	}

	prefetchCount := env("RABBITMQ_PREFETCH_COUNT", "50")
	iPrefetchCount, err := strconv.Atoi(prefetchCount)
	if err != nil {
		logrus.WithError(err).Panicln("prefetch-count is invalid.")
	}

	f := Flags{}
	f.Url = flag.String("url", env("RABBITMQ_URL", "amqp://go1:go1@127.0.0.1:5672/"), "")
	f.Kind = flag.String("kind", env("RABBITMQ_KIND", "topic"), "")
	f.Exchange = flag.String("exchange", env("RABBITMQ_EXCHANGE", "events"), "")
	f.RoutingKey = flag.String("routing-key", env("RABBITMQ_ROUTING_KEY", "es.writer.go1"), "")
	f.PrefetchCount = flag.Int("prefetch-count", iPrefetchCount, "")
	f.PrefetchSize = flag.Int("prefetch-size", 0, "")
	f.TickInterval = flag.Duration("tick-iterval", time.Duration(iDuration)*time.Second, "")
	f.QueueName = flag.String("queue-name", env("RABBITMQ_QUEUE_NAME", "es-writer"), "")
	f.UrlContain = flag.String("url-contains", env("URL_CONTAINS", ""), "")
	f.UrlNotContain = flag.String("url-not-contains", env("URL_NOT_CONTAINS", ""), "")
	f.ConsumerName = flag.String("consumer-name", env("RABBITMQ_CONSUMER_NAME", "es-writter"), "")
	f.EsUrl = flag.String("es-url", env("ELASTIC_SEARCH_URL", "http://127.0.0.1:9200/?sniff=false"), "")
	f.Debug = flag.Bool("debug", false, "Enable with care; credentials can be leaked if this is on.")
	f.AdminPort = flag.String("admin-port", env("ADMIN_PORT", ":8001"), "")
	f.Refresh = flag.String("refresh", env("ES_REFRESH", "true"), "")
	flag.Parse()

	return f
}

func (f *Flags) queueConnection() (*amqp.Connection, error) {
	url := *f.Url
	con, err := amqp.Dial(url)

	if nil != err {
		return nil, err
	}

	go func() {
		conCloseChan := con.NotifyClose(make(chan *amqp.Error))

		select
		{
		case err := <-conCloseChan:
			if err != nil {
				logrus.WithError(err).Panicln("RabbitMQ connection error.")
			}
		}
	}()

	return con, nil
}

func (f *Flags) queueChannel(con *amqp.Connection) (*amqp.Channel, error) {
	ch, err := con.Channel()
	if nil != err {
		return nil, err
	}

	if "topic" != *f.Kind && "direct" != *f.Kind {
		ch.Close()

		return nil, fmt.Errorf("unsupported channel kind: %s", *f.Kind)
	}

	err = ch.ExchangeDeclare(*f.Exchange, *f.Kind, false, false, false, false, nil)
	if nil != err {
		ch.Close()

		return nil, err
	}

	err = ch.Qos(*f.PrefetchCount, *f.PrefetchSize, false)
	if nil != err {
		ch.Close()

		return nil, err
	}

	return ch, nil
}

func (f *Flags) elasticSearchClient() (*elastic.Client, error) {
	cfg, err := config.Parse(*f.EsUrl)
	if err != nil {
		logrus.Fatalf("failed to parse URL: %s", err.Error())

		return nil, err
	}

	client, err := elastic.NewClientFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (f *Flags) App() (*App, error, chan bool) {
	con, err := f.queueConnection()
	if err != nil {
		return nil, err, nil
	}

	ch, err := f.queueChannel(con)
	if err != nil {
		return nil, err, nil
	}

	es, err := f.elasticSearchClient()
	if err != nil {
		return nil, err, nil
	}

	stop := make(chan bool)

	go func() {
		<-stop
		ch.Close()
		con.Close()
	}()

	return &App{
		debug: *f.Debug,
		rabbit: &RabbitMqInput{
			ch:   ch,
			tags: []uint64{},
		},
		buffer:         action.NewContainer(),
		count:          *f.PrefetchCount,
		urlContains:    *f.UrlContain,
		urlNotContains: *f.UrlNotContain,
		es:             es,
		refresh:        *f.Refresh,
	}, nil, stop
}
