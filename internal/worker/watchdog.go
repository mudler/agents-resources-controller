package worker

import (
	"io"
	"sync"
	"time"
)

// idleClock records when output last flowed. It is shared across however
// many writers feed it — normally one (stdout and stderr combined), but two
// when JobSpec.Stderr splits them — so the idle watchdog measures real
// progress on either stream rather than treating them as independent
// clocks: a process that only ever writes to stderr must still count as
// producing output.
type idleClock struct {
	mu   sync.Mutex
	last time.Time
}

func newIdleClock(now time.Time) *idleClock {
	return &idleClock{last: now}
}

func (c *idleClock) touch() {
	c.mu.Lock()
	c.last = time.Now()
	c.mu.Unlock()
}

func (c *idleClock) idleFor() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Since(c.last)
}

// watchdogWriter wraps a sink and touches a shared idleClock on every write,
// so the idle watchdog sees progress the moment either stream produces
// output.
type watchdogWriter struct {
	sink  io.Writer
	clock *idleClock
}

func newWatchdogWriter(sink io.Writer, clock *idleClock) *watchdogWriter {
	return &watchdogWriter{sink: sink, clock: clock}
}

func (w *watchdogWriter) Write(p []byte) (int, error) {
	w.clock.touch()
	return w.sink.Write(p)
}
