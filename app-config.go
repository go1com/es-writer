package es_writer

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/olivere/elastic.v5/config"
	"gopkg.in/yaml.v2"

	"github.com/go1com/es-writer/action"
)

type (
	Configuration struct {
		ElasticSearch esConfig       `yaml:"elasticSearch"`
		RabbitMq      rabbitmqConfig `yaml:"rabbitmq"`
		TickInterval  time.Duration  `yaml:"tickInterval"`
		Filter        filterConfig   `yaml:"filter"`
		AdminPort     string         `yaml:"adminPort"`
		Debug         bool           `yaml:"debug"`
	}

	esConfig struct {
		Url     string `yaml:"url"`
		Refresh string `yaml:"refresh"`
	}

	rabbitmqConfig struct {
		Url           string `yaml:"url"`
		Kind          string `yaml:"kind"`
		Exchange      string `yaml:"exchange"`
		ConsumerName  string `yaml:"consumerName"`
		QueueName     string `yaml:"queueName"`
		RoutingKey    string `yaml:"routingKey"`
		PrefetchCount int    `yaml:"prefetchCount"`
	}

	filterConfig struct {
		UrlContains    string `yaml:"urlContains"`
		UrlNotContains string `yaml:"urlNotContains"`
	}
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

func env(key string, defaultValue string) string {
	value, _ := os.LookupEnv(key)

	if "" == value {
		return defaultValue
	}

	return value
}

func NewConfigurationFromYamlFile(path string) (*Configuration, error) {
	var err error
	var yamlBytes []byte

	if "" != path {
		yamlBytes, err = ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}
	}

	return NewConfigurationFromYamlBytes(yamlBytes)
}

func NewConfigurationFromYamlBytes(yamlRaw []byte) (*Configuration, error) {
	yamlString := os.ExpandEnv(string(yamlRaw))

	cnf := &Configuration{}
	err := yaml.Unmarshal([]byte(yamlString), cnf)
	if err != nil {
		return nil, err
	}

	return cnf, nil
}

func (cnf *Configuration) queueConnection() (*amqp.Connection, error) {
	con, err := amqp.Dial(cnf.RabbitMq.Url)

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

func (cnf *Configuration) queueChannel(con *amqp.Connection) (*amqp.Channel, error) {
	ch, err := con.Channel()
	if nil != err {
		return nil, err
	}

	if "topic" != cnf.RabbitMq.Kind && "direct" != cnf.RabbitMq.Kind {
		ch.Close()

		return nil, fmt.Errorf("unsupported channel kind: %s", cnf.RabbitMq.Kind)
	}

	err = ch.ExchangeDeclare(cnf.RabbitMq.Exchange, cnf.RabbitMq.Kind, false, false, false, false, nil)
	if nil != err {
		ch.Close()

		return nil, err
	}

	err = ch.Qos(cnf.RabbitMq.PrefetchCount, cnf.RabbitMq.PrefetchCount, false)
	if nil != err {
		ch.Close()

		return nil, err
	}

	return ch, nil
}

func (cnf *Configuration) elasticSearchClient() (*elastic.Client, error) {
	cfg, err := config.Parse(cnf.ElasticSearch.Url)
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

func (cnf *Configuration) App() (*App, error, chan bool) {
	con, err := cnf.queueConnection()
	if err != nil {
		return nil, err, nil
	}

	ch, err := cnf.queueChannel(con)
	if err != nil {
		return nil, err, nil
	}

	es, err := cnf.elasticSearchClient()
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
		debug: cnf.Debug,
		rabbit: &RabbitMqInput{
			ch:   ch,
			tags: []uint64{},
		},
		buffer:         action.NewContainer(),
		count:          cnf.RabbitMq.PrefetchCount,
		urlContains:    cnf.Filter.UrlContains,
		urlNotContains: cnf.Filter.UrlNotContains,
		es:             es,
		refresh:        cnf.ElasticSearch.Refresh,
	}, nil, stop
}
