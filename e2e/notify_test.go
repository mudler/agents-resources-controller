// Package e2e_test's notification tests cover what stage 4 added on the
// controller side: operational events reach an operator's webhook as JSON,
// and a webhook that has stopped answering costs notifications, never jobs.
package e2e_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/notify"
	"github.com/stretchr/testify/require"
)

// TestWatchdogTripReachesTheConfiguredWebhook runs a real job past a real
// per-device runtime ceiling and proves the resulting event arrives at an
// operator's endpoint, naming the device and the job — the whole chain from
// the worker's watchdog, through the terminal report the controller reads the
// kill reason off, to an HTTP POST somewhere else entirely.
func TestWatchdogTripReachesTheConfiguredWebhook(t *testing.T) {
	rec := newWebhookRecorder(t)
	defer rec.Close()

	// Attempts: 1 — an endpoint that answers 200 needs no retry budget, and
	// leaving one in place would only slow a failing test down.
	notifier := notify.New(notify.NewWebhook(rec.URL(), nil),
		notify.Options{QueueSize: 16, Attempts: 1})

	cl, _, device := newFleet(t, 2*time.Second, withNotifier(notifier))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	job, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-a",
		Command: []string{"sh", "-c", "sleep 30"},
	})
	require.NoError(t, err)

	final, err := cl.WaitTerminal(ctx, job.ID, waitTerminalBound)
	require.NoError(t, err)
	require.Equal(t, model.JobKilled, final.State, "the device's 2s ceiling should have killed this job")

	// The event follows the terminal report, so it is genuinely asynchronous
	// from the job's own state — hence the wait.
	var trip notify.Event
	require.Eventually(t, func() bool {
		for _, e := range rec.events() {
			if e.Kind == notify.KindWatchdogTrip {
				trip = e
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond,
		"no watchdog_trip event ever reached the webhook (received: %v)", rec.kinds())

	require.Equal(t, device, trip.Device, "the event must name the device the job was killed on")
	require.Equal(t, job.ID, trip.Job, "the event must name the job that was killed")
	require.Contains(t, trip.Reason, "max_runtime exceeded",
		"the event's reason must be the watchdog's own kill reason")
	require.False(t, trip.At.IsZero(), "every event carries the time it happened")

	// And the wire format is what the README tells an operator to expect:
	// the kind travels under the "event" key, not "kind", and the whole
	// payload is one flat JSON object.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.lastBody(), &raw))
	require.Equal(t, "watchdog_trip", raw["event"])
	require.Contains(t, raw, "device")
	require.Contains(t, raw, "job")
	require.Contains(t, raw, "reason")
	require.Contains(t, raw, "at")
}

// TestJobsKeepRunningWhenTheWebhookNeverAnswers is the no-blocking guarantee
// end to end: an endpoint that accepts connections and then never responds —
// the worst case for a queue, far worse than one that refuses outright —
// must cost notifications and nothing else.
//
// The queue is deliberately one deep, and the sink's own HTTP timeout is
// deliberately far longer than this test, so the wedge cannot resolve itself
// mid-test: the first event occupies the delivery goroutine forever, the
// second fills the queue, and the third has nowhere to go.
//
// The drop count at the end is the decisive assertion, and it is there
// because the job assertions alone are NOT enough to catch a blocking
// Notify. Measured, by making Notify block on a full queue: the jobs still
// finish, because handleJobStatus emits AFTER store.Release has already
// committed the terminal state — so blocking strands the handler (and the
// worker's terminal-report retries behind it), not the job row the client is
// watching. The jobs are still asserted because "a wedged webhook costs
// nothing operational" is the promise; the drop count is what proves the
// mechanism that keeps it.
func TestJobsKeepRunningWhenTheWebhookNeverAnswers(t *testing.T) {
	var hits atomic.Int64
	unblock := make(chan struct{})
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		// Answer nothing, ever — until the test tears down, or the
		// notifier's own Close cancels the request.
		select {
		case <-unblock:
		case <-r.Context().Done():
		}
	}))
	// LIFO: unblock the handler first, so httptest's Close (which waits for
	// in-flight requests) is not the thing that hangs this test.
	defer hook.Close()
	defer close(unblock)

	// An hour-long client timeout, not the sink's 10s default: a delivery
	// that gave up and moved on halfway through would quietly turn this into
	// a test of a slow webhook rather than a wedged one.
	notifier := notify.New(notify.NewWebhook(hook.URL, &http.Client{Timeout: time.Hour}),
		notify.Options{QueueSize: 1, Attempts: 1})

	cl, _, device := newFleet(t, 2*time.Second, withNotifier(notifier))
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Three jobs, one after another, each tripping the device's 2s ceiling
	// and so producing exactly one event: one to occupy the wedged delivery,
	// one to fill the queue, one to be dropped.
	for i := range 3 {
		job, err := cl.Submit(ctx, client.SubmitOptions{
			DeviceID: device, Submitter: "agent-a",
			Command: []string{"sh", "-c", "sleep 30"},
		})
		require.NoError(t, err, "submit %d was refused while the webhook was wedged", i)

		final, err := cl.WaitTerminal(ctx, job.ID, waitTerminalBound)
		require.NoError(t, err, "job %d never reached a terminal state while the webhook was wedged", i)
		require.Equal(t, model.JobKilled, final.State)
		require.Contains(t, final.KillReason, "max_runtime")

		// The device comes back too: a wedged webhook must not strand it.
		require.Eventually(t, func() bool {
			state, err := cl.State(ctx)
			return err == nil && len(state.Devices) == 1 &&
				state.Devices[0].Device.State == model.DeviceReady
		}, 30*time.Second, 100*time.Millisecond,
			"the device never returned to the pool after job %d", i)
	}

	// Not a vacuous pass: the wedged endpoint really was reached, so the
	// jobs above really did run with a delivery stuck against it.
	require.Positive(t, hits.Load(), "the webhook was never called, so nothing was ever wedged")

	// And the overflow was dropped rather than blocking whoever emitted it.
	require.Positive(t, notifier.Dropped(),
		"three events against a one-deep queue behind a wedged sink must drop at least one")
}

// webhookRecorder is an httptest server that decodes and keeps every event
// posted to it. Assertions live in the test body, never in the handler
// goroutine: require's FailNow may only be called from the goroutine running
// the test.
type webhookRecorder struct {
	srv *httptest.Server

	mu     sync.Mutex
	got    []notify.Event
	bodies [][]byte
	errs   []error
}

func newWebhookRecorder(t *testing.T) *webhookRecorder {
	t.Helper()
	r := &webhookRecorder{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		r.mu.Lock()
		defer r.mu.Unlock()
		if err != nil {
			r.errs = append(r.errs, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var e notify.Event
		if err := json.Unmarshal(body, &e); err != nil {
			r.errs = append(r.errs, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.bodies = append(r.bodies, body)
		r.got = append(r.got, e)
		w.WriteHeader(http.StatusOK)
	}))
	return r
}

func (r *webhookRecorder) URL() string { return r.srv.URL }
func (r *webhookRecorder) Close()      { r.srv.Close() }

func (r *webhookRecorder) events() []notify.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]notify.Event(nil), r.got...)
}

// kinds names what has arrived so far, for a failure message that says what
// the webhook DID receive instead of only what it didn't.
func (r *webhookRecorder) kinds() []notify.Kind {
	out := []notify.Kind{}
	for _, e := range r.events() {
		out = append(out, e.Kind)
	}
	return out
}

func (r *webhookRecorder) lastBody() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.bodies) == 0 {
		return nil
	}
	return r.bodies[len(r.bodies)-1]
}
