package worker

import (
	"io"
	"sync"
	"time"
)

// watchdogWriter wraps a sink and records when output last flowed, so the
// idle watchdog measures real progress rather than wall clock.
type watchdogWriter struct {
	sink io.Writer

	mu   sync.Mutex
	last time.Time
}

func newWatchdogWriter(sink io.Writer, now time.Time) *watchdogWriter {
	return &watchdogWriter{sink: sink, last: now}
}

func (w *watchdogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.last = time.Now()
	w.mu.Unlock()
	return w.sink.Write(p)
}

func (w *watchdogWriter) idleFor() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return time.Since(w.last)
}
