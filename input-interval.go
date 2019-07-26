package es_writer

import (
	"context"
	"sync"
	"time"
)

func interval(w *App, ctx context.Context, cnf *Configuration, terminate chan bool) {
	wg := sync.WaitGroup{}
	ticker := time.NewTicker(cnf.TickInterval)

	for {
		select {
		case <-terminate:
			wg.Wait() // don't terminate until data flushing is completed
			return

		case <-ticker.C:
			if w.buffer.Length() > 0 {
				metricFlushCounter.WithLabelValues("time").Inc()

				wg.Add(1)
				bufferMutext.Lock()
				w.flush(ctx)
				bufferMutext.Unlock()
				wg.Done()
			}
		}
	}
}
