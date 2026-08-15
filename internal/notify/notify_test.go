package notify_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/notify"
	"github.com/stretchr/testify/require"
)

type recordingSink struct {
	mu       sync.Mutex
	events   []notify.Event
	failFor  int32 // fail this many calls before succeeding
	attempts atomic.Int32
	block    chan struct{}
}

func (s *recordingSink) Deliver(ctx context.Context, e notify.Event) error {
	s.attempts.Add(1)
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.attempts.Load() <= s.failFor {
		return errors.New("boom")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *recordingSink) delivered() []notify.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]notify.Event(nil), s.events...)
}

func opts() notify.Options {
	return notify.Options{QueueSize: 8, Attempts: 3, Backoff: time.Millisecond}
}

func TestNotifyDeliversAnEvent(t *testing.T) {
	sink := &recordingSink{}
	n := notify.New(sink, opts())
	defer n.Close(context.Background())

	n.Notify(notify.Event{Kind: notify.KindWatchdogTrip, Device: "gpubox:gpu0"})

	require.Eventually(t, func() bool { return len(sink.delivered()) == 1 },
		2*time.Second, 10*time.Millisecond)
	require.Equal(t, "gpubox:gpu0", sink.delivered()[0].Device)
}

func TestNotifyRetriesThenSucceeds(t *testing.T) {
	sink := &recordingSink{failFor: 2}
	n := notify.New(sink, opts())
	defer n.Close(context.Background())

	n.Notify(notify.Event{Kind: notify.KindJobLost})

	require.Eventually(t, func() bool { return len(sink.delivered()) == 1 },
		2*time.Second, 10*time.Millisecond)
	require.EqualValues(t, 3, sink.attempts.Load(), "two failures then one success")
}

func TestNotifyGivesUpAfterTheRetryBudget(t *testing.T) {
	sink := &recordingSink{failFor: 1000}
	n := notify.New(sink, opts())
	defer n.Close(context.Background())

	n.Notify(notify.Event{Kind: notify.KindJobLost})

	require.Eventually(t, func() bool { return sink.attempts.Load() >= 3 },
		2*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.EqualValues(t, 3, sink.attempts.Load(), "no attempts beyond the budget")
	require.Empty(t, sink.delivered())
}

// The property the whole design rests on.
func TestNotifyNeverBlocksWhenTheQueueIsFull(t *testing.T) {
	sink := &recordingSink{block: make(chan struct{})}
	n := notify.New(sink, notify.Options{QueueSize: 2, Attempts: 1, Backoff: time.Millisecond})
	defer func() { close(sink.block); n.Close(context.Background()) }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			n.Notify(notify.Event{Kind: notify.KindWatchdogTrip})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Notify blocked behind a stuck sink; scheduling would stall")
	}
	require.Positive(t, n.Dropped(), "a full queue must drop and count, not block")
}

func TestCloseDrainsWhatItCan(t *testing.T) {
	sink := &recordingSink{}
	n := notify.New(sink, opts())

	n.Notify(notify.Event{Kind: notify.KindDeviceUnhealthy})
	require.NoError(t, n.Close(context.Background()))
	require.Len(t, sink.delivered(), 1, "a queued event should survive a graceful close")
}

func TestNotifyAfterCloseDoesNotPanic(t *testing.T) {
	sink := &recordingSink{}
	n := notify.New(sink, opts())
	require.NoError(t, n.Close(context.Background()))

	require.NotPanics(t, func() { n.Notify(notify.Event{Kind: notify.KindJobLost}) })
}

// A nil notifier is the "no webhook configured" case and must be safe, since
// every call site would otherwise need a nil check.
func TestNilNotifierIsSafe(t *testing.T) {
	var n *notify.Notifier
	require.NotPanics(t, func() { n.Notify(notify.Event{Kind: notify.KindJobLost}) })
	require.NotPanics(t, func() { _ = n.Close(context.Background()) })
}

// Close must terminate promptly even when the caller's context is already
// cancelled, and must not leak the delivery goroutine.
func TestCloseWithCancelledContextTerminatesPromptly(t *testing.T) {
	sink := &recordingSink{block: make(chan struct{})}
	n := notify.New(sink, notify.Options{QueueSize: 8, Attempts: 3, Backoff: time.Millisecond})
	defer close(sink.block)

	// Fill the queue with an in-flight (blocked) delivery plus more queued
	// work, so a naive Close would otherwise wait on the blocked sink.
	n.Notify(notify.Event{Kind: notify.KindWatchdogTrip})
	n.Notify(notify.Event{Kind: notify.KindJobLost})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		n.Close(ctx)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not terminate promptly with an already-cancelled context")
	}
}

// Calling Close twice must not panic, and the second call should also
// return promptly rather than hang.
func TestCloseTwiceDoesNotPanic(t *testing.T) {
	sink := &recordingSink{}
	n := notify.New(sink, opts())

	require.NotPanics(t, func() { require.NoError(t, n.Close(context.Background())) })

	done := make(chan struct{})
	go func() {
		defer close(done)
		require.NotPanics(t, func() { n.Close(context.Background()) })
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second Close did not return promptly")
	}
}
