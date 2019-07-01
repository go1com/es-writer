package es_writer

import (
	"context"
	"time"
)

func interval(w *Writer, ctx context.Context, flags Flags, terminate chan bool) {
	ticker := time.NewTicker(*flags.TickInterval)

	for {
		bufferMutext.Lock()

		select {
		case <-terminate:
			return

		case <-ticker.C:
			if w.actions.Length() > 0 {
				metricFlushCounter.WithLabelValues("time").Inc()
				w.flush(ctx)
			}
		}

		bufferMutext.Unlock()
	}
}
