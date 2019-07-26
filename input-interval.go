package es_writer

import (
	"context"
	"time"
)

func interval(w *App, ctx context.Context, cnf *Configuration, terminate chan bool) {
	ticker := time.NewTicker(cnf.TickInterval)

	for {
		select {
		case <-terminate:
			return

		case <-ticker.C:
			if w.buffer.Length() > 0 {
				metricFlushCounter.WithLabelValues("time").Inc()

				bufferMutext.Lock()
				w.flush(ctx)
				bufferMutext.Unlock()
			}
		}
	}
}
