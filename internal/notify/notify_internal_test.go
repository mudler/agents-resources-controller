package notify

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// blockingSink is a minimal white-box Sink: Deliver blocks until the test
// releases it, and every successful call is recorded. It exists in this
// file (package notify, not notify_test) rather than reusing
// notify_test.go's recordingSink because the test below needs access to
// the unexported Notifier.closeRequested channel, which is only reachable
// from within this package.
type blockingSink struct {
	mu       sync.Mutex
	events   []Event
	attempts atomic.Int32
	block    chan struct{}
}

func (s *blockingSink) Deliver(ctx context.Context, e Event) error {
	s.attempts.Add(1)
	select {
	case <-s.block:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
	return nil
}

func (s *blockingSink) delivered() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

// TestCloseDrainsWhatItCan pins that a graceful Close delivers everything
// still sitting in the queue, including the tail that run()'s loop had not
// yet reached the moment Close was called.
//
// This has to be a white-box test. A black-box version (queue three events
// behind a blocking sink, wait for the first delivery attempt to start,
// call Close, release the sink) cannot reliably force run() through
// drainRemaining: without synchronizing on the unexported closeRequested
// channel there is an unavoidable window, between spawning the Close
// goroutine and that goroutine actually executing closeOnce.Do, during
// which the released delivery can loop back to run()'s per-item select
// before closeRequested is observably closed. Even after fixing run() to
// check closeRequested with priority (see run's doc comment), that window
// — vanishingly small, but real — means a purely timing-based black-box
// test still isn't a mathematical guarantee, only "very likely." Measured
// against a deliberately emptied drainRemaining, a timing-based version of
// this test caught the regression in under half of runs under -race.
//
// Blocking on <-n.closeRequested here removes the window entirely: once
// this receive returns, closeRequested is closed for good (channel closes
// don't un-happen), so run()'s very next loop iteration is guaranteed to
// take the shutdown path — regardless of goroutine scheduling, and
// regardless of -race's extra scheduling perturbation.
func TestCloseDrainsWhatItCan(t *testing.T) {
	sink := &blockingSink{block: make(chan struct{})}
	n := New(sink, Options{QueueSize: 8, Attempts: 1, Backoff: time.Millisecond})

	n.Notify(Event{Kind: KindDeviceUnhealthy})
	n.Notify(Event{Kind: KindWatchdogTrip})
	n.Notify(Event{Kind: KindJobLost})

	require.Eventually(t, func() bool { return sink.attempts.Load() >= 1 },
		time.Second, time.Millisecond, "the first delivery attempt should have started")

	closeDone := make(chan error, 1)
	go func() { closeDone <- n.Close(context.Background()) }()

	<-n.closeRequested // block until Close has definitely signalled shutdown

	close(sink.block) // let the in-flight delivery, and the rest of the drain, proceed

	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the sink was released")
	}
	require.Len(t, sink.delivered(), 3, "all three queued events should survive a graceful close")
}
