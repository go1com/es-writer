package es_writer

import (
	"context"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"

	"github.com/go1com/es-writer/action"
)

func hanldeUpdateByQuery(ctx context.Context, client *elastic.Client, element action.Element, requestType string) error {
	service, err := element.UpdateByQueryService(client)
	if err != nil {
		metricFailureCounter.WithLabelValues(requestType).Inc()
		return err
	}

	conflictRetryIntervals := []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 7 * time.Second, 0}
	for _, conflictRetryInterval := range conflictRetryIntervals {
		start := time.Now()
		_, err = service.Do(ctx)
		metricDurationHistogram.
			WithLabelValues(requestType).
			Observe(time.Since(start).Seconds())

		if err == nil {
			break
		}

		if strings.Contains(err.Error(), "Error 409 (Conflict)") {
			metricFailureCounter.WithLabelValues(requestType).Inc()
			logrus.WithError(err).Errorf("writing has conflict; try again in %s.\n", conflictRetryInterval)
			time.Sleep(conflictRetryInterval)
		}
		metricRetryCounter.WithLabelValues(requestType).Inc()
	}

	if err != nil {
		metricFailureCounter.WithLabelValues(requestType).Inc()
	}

	return err
}

func hanldeDeleteByQuery(ctx context.Context, client *elastic.Client, element action.Element, requestType string) error {
	service, err := element.DeleteByQueryService(client)
	if err != nil {
		metricFailureCounter.WithLabelValues(requestType).Inc()
		return err
	}

	conflictRetryIntervals := []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 7 * time.Second, 0}
	for _, conflictRetryInterval := range conflictRetryIntervals {
		start := time.Now()
		_, err = service.Do(ctx)
		metricDurationHistogram.
			WithLabelValues(requestType).
			Observe(time.Since(start).Seconds())

		if err == nil {
			break
		}

		if strings.Contains(err.Error(), "Error 409 (Conflict)") {
			metricFailureCounter.WithLabelValues(requestType).Inc()
			logrus.WithError(err).Errorf("deleting has conflict; try again in %s.\n", conflictRetryInterval)
			time.Sleep(conflictRetryInterval)
		}

		metricRetryCounter.WithLabelValues(requestType).Inc()
	}

	if err != nil {
		metricFailureCounter.WithLabelValues(requestType).Inc()
	}

	return err
}

func hanldeIndicesCreate(ctx context.Context, client *elastic.Client, element action.Element) error {
	service, err := element.IndicesCreateService(client)
	if err != nil {
		return err
	}

	_, err = service.Do(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			logrus.WithError(err).Errorln("That's ok if the index is existing.")
			return nil
		}
	}

	return err
}

func handleIndicesDelete(ctx context.Context, client *elastic.Client, element action.Element) error {
	service, err := element.IndicesDeleteService(client)
	if err != nil {
		return err
	}

	_, err = service.Do(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "[type=index_not_found_exception]") {
			logrus.WithError(err).Infoln("That's ok if the index is not existing, already deleted somewhere.")
			return nil
		}
	}

	return err
}

func handleIndicesAlias(ctx context.Context, client *elastic.Client, element action.Element) error {
	res, err := action.CreateIndiceAlias(ctx, client, element)
	if err != nil {
		if strings.Contains(err.Error(), "index_not_found_exception") {
			logrus.WithError(err).Errorln("That's ok if the index is existing.")
			return nil
		}
	}

	logrus.
		WithError(err).
		WithField("action", "indices_alias").
		WithField("res", res).
		WithField("body", element.String()).
		Infoln("create")

	return err
}
