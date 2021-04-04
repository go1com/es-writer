package es_writer

import (
	"log"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"gopkg.in/olivere/elastic.v5"
	"gopkg.in/olivere/elastic.v5/config"
)

func NewClientFromConfig(cfg *config.Config, disableKeepAlive bool) (*elastic.Client, error) {
	var options []elastic.ClientOptionFunc
	if cfg != nil {
		if cfg.URL != "" {
			options = append(options, elastic.SetURL(cfg.URL))
		}
		if cfg.Errorlog != "" {
			f, err := os.OpenFile(cfg.Errorlog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil, errors.Wrap(err, "unable to initialize error log")
			}
			l := log.New(f, "", 0)
			options = append(options, elastic.SetErrorLog(l))
		}
		if cfg.Tracelog != "" {
			f, err := os.OpenFile(cfg.Tracelog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil, errors.Wrap(err, "unable to initialize trace log")
			}
			l := log.New(f, "", 0)
			options = append(options, elastic.SetTraceLog(l))
		}
		if cfg.Infolog != "" {
			f, err := os.OpenFile(cfg.Infolog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil, errors.Wrap(err, "unable to initialize info log")
			}
			l := log.New(f, "", 0)
			options = append(options, elastic.SetInfoLog(l))
		}
		if cfg.Username != "" || cfg.Password != "" {
			options = append(options, elastic.SetBasicAuth(cfg.Username, cfg.Password))
		}
		if cfg.Sniff != nil {
			options = append(options, elastic.SetSniff(*cfg.Sniff))
		}
		if cfg.Healthcheck != nil {
			options = append(options, elastic.SetHealthcheck(*cfg.Healthcheck))
		}
	}

	if disableKeepAlive {
		httpClient := &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
		}

		options = append(options, elastic.SetHttpClient(httpClient))
	}

	return elastic.NewClient(options...)
}
