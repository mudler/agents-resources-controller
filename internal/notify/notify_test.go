package notify_test

import (
	"context"
	"encoding/json"
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

// TestCloseDrainsWhatItCan queues three events behind a sink that blocks
// until released. Because the first delivery attempt is guaranteed to
// still be in flight when Close runs, the other two events are guaranteed
// to still be sitting in the queue (not yet consumed by run's own loop)
// at that moment — this makes the drain-what's-queued path deterministic
// instead of racing against run() draining the queue before Close is ever
// called (which, left to chance, exercises the drain path in only a
// minority of runs).
func TestCloseDrainsWhatItCan(t *testing.T) {
	sink := &recordingSink{block: make(chan struct{})}
	n := notify.New(sink, opts())

	n.Notify(notify.Event{Kind: notify.KindDeviceUnhealthy})
	n.Notify(notify.Event{Kind: notify.KindWatchdogTrip})
	n.Notify(notify.Event{Kind: notify.KindJobLost})

	require.Eventually(t, func() bool { return sink.attempts.Load() >= 1 },
		time.Second, time.Millisecond, "the first delivery attempt should have started")

	closeDone := make(chan error, 1)
	go func() { closeDone <- n.Close(context.Background()) }()

	close(sink.block) // let delivery proceed now that Close is in flight

	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the sink was released")
	}
	require.Len(t, sink.delivered(), 3, "all three queued events should survive a graceful close")
}

// TestNotifyAfterCloseDoesNotPanic also pins that a post-Close Notify is
// dropped and counted, not merely non-panicking. With this Notifier's
// queue-is-never-closed design, no post-Close Notify can panic under any
// circumstances — so a bare NotPanics assertion here would stay green even
// if the closeRequested pre-check were deleted entirely. Asserting Dropped
// increases is what actually exercises that check.
func TestNotifyAfterCloseDoesNotPanic(t *testing.T) {
	sink := &recordingSink{}
	n := notify.New(sink, opts())
	require.NoError(t, n.Close(context.Background()))

	before := n.Dropped()
	require.NotPanics(t, func() { n.Notify(notify.Event{Kind: notify.KindJobLost}) })
	require.Greater(t, n.Dropped(), before, "a Notify after Close should be dropped and counted")
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

// A later task constructs the notifier conditionally on whether a webhook
// is configured, which is exactly the shape that would otherwise hand New
// a nil Sink. New must return nil rather than a Notifier whose background
// goroutine dereferences a nil Sink and crashes the whole process on the
// first delivery attempt.
func TestNewWithNilSinkReturnsNil(t *testing.T) {
	n := notify.New(nil, opts())
	require.Nil(t, n)

	// And that nil must behave exactly like the documented "no webhook
	// configured" state every other method already handles.
	require.NotPanics(t, func() { n.Notify(notify.Event{Kind: notify.KindJobLost}) })
	require.NotPanics(t, func() { _ = n.Close(context.Background()) })
}

// Event's JSON tags are a public wire format a later task documents in the
// README. Pin the exact shape here so a renamed tag fails loudly instead of
// silently changing what an operator's webhook consumer receives.
func TestEventJSONWireFormat(t *testing.T) {
	at := time.Date(2026, 8, 15, 12, 30, 0, 0, time.UTC)
	e := notify.Event{
		Kind:   notify.KindWatchdogTrip,
		Device: "gpubox:gpu0",
		Job:    "job-1",
		Reason: "no heartbeat",
		At:     at,
	}

	b, err := json.Marshal(e)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(b, &got))

	require.Equal(t, "watchdog_trip", got["event"])
	require.Equal(t, "gpubox:gpu0", got["device"])
	require.Equal(t, "job-1", got["job"])
	require.Equal(t, "no heartbeat", got["reason"])
	require.Equal(t, at.Format(time.RFC3339), got["at"])
	require.Len(t, got, 5, "no unexpected extra or renamed fields")
}

// An event whose caller never set At must not ship as the Go zero time
// ("0001-01-01T00:00:00Z") — an operator's consumer ordering events by At
// would sort a real event before every event that ever set it.
func TestNotifyStampsAtWhenZero(t *testing.T) {
	sink := &recordingSink{}
	n := notify.New(sink, opts())
	defer n.Close(context.Background())

	before := time.Now().Add(-time.Second)
	n.Notify(notify.Event{Kind: notify.KindWorkerLost})

	require.Eventually(t, func() bool { return len(sink.delivered()) == 1 },
		2*time.Second, 10*time.Millisecond)

	at := sink.delivered()[0].At
	require.False(t, at.IsZero(), "Notify should stamp At when the caller leaves it zero")
	require.False(t, at.Before(before), "the stamped At should not predate the Notify call")
	require.WithinDuration(t, time.Now(), at, 2*time.Second)
}

// A caller-supplied At must survive untouched — Notify only fills the gap,
// it does not overwrite a timestamp the caller deliberately set (e.g. a
// reaper backfilling a sweep it ran a moment ago).
func TestNotifyPreservesExplicitAt(t *testing.T) {
	sink := &recordingSink{}
	n := notify.New(sink, opts())
	defer n.Close(context.Background())

	want := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	n.Notify(notify.Event{Kind: notify.KindWorkerLost, At: want})

	require.Eventually(t, func() bool { return len(sink.delivered()) == 1 },
		2*time.Second, 10*time.Millisecond)
	require.True(t, sink.delivered()[0].At.Equal(want))
}

// timingSink records when each Deliver call happened, so a test can inspect
// the gaps between retries.
type timingSink struct {
	mu    sync.Mutex
	calls []time.Time
}

func (s *timingSink) Deliver(ctx context.Context, e notify.Event) error {
	s.mu.Lock()
	s.calls = append(s.calls, time.Now())
	s.mu.Unlock()
	return errors.New("always fails")
}

func (s *timingSink) times() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.calls...)
}

// The backoff between retries must double each time, not stay flat — a
// "backoff *= 1" mutation would still retry the right number of times (the
// existing budget tests wouldn't catch it) but would space every attempt
// the same ~100ms apart instead of 100/200/400ms.
func TestNotifyBackoffDoublesBetweenAttempts(t *testing.T) {
	sink := &timingSink{}
	n := notify.New(sink, notify.Options{QueueSize: 8, Attempts: 4, Backoff: 100 * time.Millisecond})
	defer n.Close(context.Background())

	n.Notify(notify.Event{Kind: notify.KindJobLost})

	require.Eventually(t, func() bool { return len(sink.times()) >= 4 },
		3*time.Second, 10*time.Millisecond)

	times := sink.times()
	require.Len(t, times, 4)

	gap1 := times[1].Sub(times[0])
	gap2 := times[2].Sub(times[1])
	gap3 := times[3].Sub(times[2])

	const toleranceMS = 60.0
	require.InDelta(t, 2*float64(gap1.Milliseconds()), float64(gap2.Milliseconds()), toleranceMS,
		"the second gap should roughly double the first (100ms -> 200ms)")
	require.InDelta(t, 2*float64(gap2.Milliseconds()), float64(gap3.Milliseconds()), toleranceMS,
		"the third gap should roughly double the second (200ms -> 400ms)")
}
