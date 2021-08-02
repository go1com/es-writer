package es_writer

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"
	"gopkg.in/olivere/elastic.v5"

	"github.com/go1com/es-writer/action"
)

func (this *App) handleUpdateByQuery(ctx context.Context, client *elastic.Client, element action.Element, requestType string) error {
	service, err := element.UpdateByQueryService(client)
	if err != nil {
		return err
	}

	conflictRetryIntervals := []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 7 * time.Second, 0}
	for _, conflictRetryInterval := range conflictRetryIntervals {
		_, err = service.Do(ctx)

		if err == nil {
			break
		}

		if strings.Contains(err.Error(), "Error 409 (Conflict)") {
			this.logger.Error(
				"conflict on writing doc; trying again",
				zap.Duration("sleep", conflictRetryInterval),
			)

			time.Sleep(conflictRetryInterval)
		}
	}

	return err
}

func (this *App) handleDeleteByQuery(ctx context.Context, client *elastic.Client, element action.Element, requestType string) error {
	service, err := element.DeleteByQueryService(client)
	if err != nil {
		return err
	}

	conflictRetryIntervals := []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 7 * time.Second, 0}
	for _, conflictRetryInterval := range conflictRetryIntervals {
		_, err = service.Do(ctx)

		if err == nil {
			break
		}

		if strings.Contains(err.Error(), "Error 409 (Conflict)") {
			this.logger.Error("conflict on deleting; try again", zap.Duration("sleep", conflictRetryInterval))
			time.Sleep(conflictRetryInterval)
		}
	}

	return err
}

func handleIndicesCreate(ctx context.Context, client *elastic.Client, element action.Element) error {
	service, err := element.IndicesCreateService(client)
	if err != nil {
		return err
	}

	_, err = service.Do(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil // That's ok if the index is existing.
		}
	}

	return err
}

func (this *App) handleIndicesDelete(ctx context.Context, client *elastic.Client, element action.Element) error {
	service, err := element.IndicesDeleteService(client)
	if err != nil {
		return err
	}

	_, err = service.Do(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "[type=index_not_found_exception]") {
			this.logger.Info(
				"that's ok if the index is not existing, already deleted somewhere",
				zap.Error(err),
			)

			return nil
		}
	}

	return err
}

func (this *App) handleIndicesAlias(ctx context.Context, client *elastic.Client, element action.Element) error {
	res, err := action.CreateIndiceAlias(ctx, client, element)

	if err != nil {
		if strings.Contains(err.Error(), "index_not_found_exception") {
			this.logger.Error("that's ok if the index is existing", zap.Error(err))
			return nil
		}
	}

	this.logger.Info(
		"create",
		zap.String("action", "indices_alias"),
		zap.String("body", element.String()),
		zap.Any("res", res),
	)

	return err
}
