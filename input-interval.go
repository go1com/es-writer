package es_writer

import (
	"context"
	"time"
)

func interval(w *App, ctx context.Context, flags Flags, terminate chan bool) {
	ticker := time.NewTicker(*flags.TickInterval)

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
