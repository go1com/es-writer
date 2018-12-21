package es_writer

import (
	"fmt"
	"net/http"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

var metricActionCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "es_writer_action_counter",
		Help: "The number of action",
	},
	[]string{"name"},
)

var metricFlushCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "es_writer_flush_counter",
		Help: "The number of flush",
	},
	[]string{"name"},
)

var metricFailureCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "es_writer_failure_counter",
		Help: "The number of action unsuccessful processed",
	},
	[]string{"name"},
)

var metricRetryCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "es_writer_retry_counter",
		Help: "The number of action was tried",
	},
	[]string{"name"},
)

var metricInvalidCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "es_writer_invalid_counter",
		Help: "The number of invalid message",
	},
	[]string{"name"},
)

var metricDurationHistogram = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "es_writer_push_duration",
		Help:    "Consume duration distribution",
		Buckets: []float64{1, 2, 5, 10, 20, 60},
	},
	[]string{"name"},
)

func StartPrometheusServer(addr string) {
	prometheus.MustRegister(metricDurationHistogram)
	prometheus.MustRegister(metricActionCounter)
	prometheus.MustRegister(metricFailureCounter)
	prometheus.MustRegister(metricRetryCounter)
	prometheus.MustRegister(metricFlushCounter)
	prometheus.MustRegister(metricInvalidCounter)

	logrus.
		WithField("add", addr).
		Info(fmt.Printf("starting admin HTTP server"))

	http.Handle("/metrics", promhttp.Handler())
	panic(http.ListenAndServe(addr, logRequest(http.DefaultServeMux)))
}

func logRequest(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logrus.
			WithField("RemoteAddr", r.RemoteAddr).
			WithField("Method", r.Method).
			WithField("URL", r.URL).
			Info("History")
		handler.ServeHTTP(w, r)
	})
}
