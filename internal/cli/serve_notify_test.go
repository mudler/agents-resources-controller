package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/notify"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// recordingSink keeps every event delivered to it and never fails.
type recordingSink struct {
	mu     sync.Mutex
	events []notify.Event
}

func (r *recordingSink) Deliver(_ context.Context, e notify.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}

// newRecordingNotifier returns a real Notifier over a recording sink, plus a
// flush func that closes it and returns everything delivered, with At zeroed
// so events compare literally.
//
// Closing before asserting is what makes "emitted nothing" provable:
// sweepAndNotify has already returned by then, so anything it emitted is on
// the queue, and Close drains the queue and waits for the delivery goroutine
// to exit before returning.
func newRecordingNotifier(t *testing.T) (*notify.Notifier, func() []notify.Event) {
	t.Helper()
	sink := &recordingSink{}
	n := notify.New(sink, notify.Options{QueueSize: 64, Attempts: 1})
	require.NotNil(t, n)
	t.Cleanup(func() { _ = n.Close(context.Background()) })

	return n, func() []notify.Event {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, n.Close(ctx))
		require.Zero(t, n.Dropped(), "the notifier dropped events; the queue is too small to judge what was emitted")

		sink.mu.Lock()
		defer sink.mu.Unlock()
		out := make([]notify.Event, 0, len(sink.events))
		for _, e := range sink.events {
			require.False(t, e.At.IsZero(), "every emitted event must carry a timestamp")
			e.At = time.Time{}
			out = append(out, e)
		}
		return out
	}
}

// newSweepStore is a controller store with one worker and one device, on a
// fake clock the test drives forward — the same shape internal/store's own
// reaper tests use, so nothing here has to sleep.
func newSweepStore(t *testing.T) (*store.Store, *clock.Fake) {
	t.Helper()
	c := clock.NewFake(time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC))
	st, err := store.Open(filepath.Join(t.TempDir(), "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	require.NoError(t, st.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1", State: model.DeviceReady}},
	))
	return st, c
}

func allocateJob(t *testing.T, st *store.Store) *model.Job {
	t.Helper()
	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	return job
}

// SweepResult reports only device IDs, so which of worker_lost and
// device_unhealthy a newly quarantined device deserves is decided by the
// quarantine reason the reaper stored on it.
func TestSweepEventsSplitWorkerLostFromDeviceUnhealthy(t *testing.T) {
	res := store.SweepResult{
		DevicesUnhealthy: []string{"gpubox:gpu0", "gpubox:gpu1", "gpubox:gpu2"},
	}
	reasons := map[string]string{
		"gpubox:gpu0": "worker_lost",
		"gpubox:gpu1": "fault",
		// gpu2 has no reason recorded at all.
	}

	require.Equal(t, []notify.Event{
		{Kind: notify.KindWorkerLost, Device: "gpubox:gpu0", Reason: "worker_lost"},
		{Kind: notify.KindDeviceUnhealthy, Device: "gpubox:gpu1", Reason: "fault"},
		{Kind: notify.KindDeviceUnhealthy, Device: "gpubox:gpu2"},
	}, sweepEvents(res, reasons))
}

// A device demoted to unknown is not quarantined — it may well be back on
// the next heartbeat — so it is not news anyone should be paged for.
func TestSweepEventsIgnoreDevicesDemotedToUnknown(t *testing.T) {
	res := store.SweepResult{DevicesUnknown: []string{"gpubox:gpu0"}}
	require.Empty(t, sweepEvents(res, nil))
}

// The whole sweep path, against a real store: a worker goes silent long
// enough to be written off, and its device and its in-flight job are both
// announced.
func TestSweepAndNotifyEmitsWorkerLostAndJobLost(t *testing.T) {
	st, c := newSweepStore(t)
	job := allocateJob(t, st)
	n, flush := newRecordingNotifier(t)

	c.Advance(10 * time.Minute)
	res, err := sweepAndNotify(st, n, HeartbeatGrace, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []string{"gpubox:gpu0"}, res.DevicesUnhealthy)
	require.Equal(t, []string{job.ID}, res.JobsLost)

	require.Equal(t, []notify.Event{
		{Kind: notify.KindWorkerLost, Device: "gpubox:gpu0", Reason: "worker_lost"},
		{Kind: notify.KindJobLost, Job: job.ID, Reason: "worker lost"},
	}, flush())
}

// A worker that is still heartbeating but has stopped renewing one job's
// lease loses that lease to expiry, which is its own event and not a lost
// worker.
func TestSweepAndNotifyEmitsLeaseExpired(t *testing.T) {
	st, c := newSweepStore(t)
	job := allocateJob(t, st)
	n, flush := newRecordingNotifier(t)

	// The worker is alive and reporting; it just never names this job, so
	// nothing renews its lease.
	c.Advance(2 * time.Minute)
	require.NoError(t, st.RecordHeartbeat("w1", c.Now(), nil))

	res, err := sweepAndNotify(st, n, HeartbeatGrace, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired)
	require.Empty(t, res.DevicesUnhealthy, "the worker never went silent")

	require.Equal(t, []notify.Event{
		{Kind: notify.KindLeaseExpired, Job: job.ID, Reason: "lease expired"},
	}, flush())
}

// A sweep that changes nothing announces nothing: an idle controller must
// not talk to the webhook every ten seconds.
func TestSweepAndNotifyEmitsNothingWhenNothingChanged(t *testing.T) {
	st, _ := newSweepStore(t)
	allocateJob(t, st)
	n, flush := newRecordingNotifier(t)

	_, err := sweepAndNotify(st, n, HeartbeatGrace, 5*time.Minute)
	require.NoError(t, err)
	require.Empty(t, flush())
}

// No webhook configured is the default deployment: the notifier is nil and
// the sweep must run exactly as it always has.
func TestNoWebhookConfiguredYieldsNilNotifierAndASafeSweep(t *testing.T) {
	require.Nil(t, notify.New(webhookSink(""), notifierOptions),
		"no webhook URL must produce no notifier at all")

	st, c := newSweepStore(t)
	job := allocateJob(t, st)

	c.Advance(10 * time.Minute)
	res, err := sweepAndNotify(st, nil, HeartbeatGrace, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.JobsLost, "the sweep still did its work")
}

// The reaper runs on its own goroutine inside rc serve, and nothing else in
// this package proves that goroutine emits anything: sweepAndNotify could be
// perfect and still not be what the loop calls. This drives a real
// controller — real HTTP, real store, real webhook — with the sweep's own
// timings shortened, and waits for the event to arrive at the endpoint the
// --webhook-url flag named.
func TestReaperAnnouncesALostWorkerThroughTheConfiguredWebhook(t *testing.T) {
	delivered := make(chan notify.Event, 16)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e notify.Event
		// A body this test cannot decode is simply not delivered, and the
		// wait below fails on its own timeout with a usable message —
		// failing the test from inside a server goroutine would not.
		if err := json.NewDecoder(r.Body).Decode(&e); err == nil {
			select {
			case delivered <- e:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(hook.Close)

	shorten(t, &sweepInterval, 50*time.Millisecond)
	shorten(t, &sweepUnhealthyAfter, time.Millisecond)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	t.Setenv("RC_TOKENS", "wtok:worker,ctok:client")
	cmd := NewServeCmd()
	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--addr", addr, "--data", t.TempDir(), "--webhook-url", hook.URL})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("rc serve did not shut down after its context was cancelled")
		}
	})

	// Register a worker and then never heartbeat again, which is exactly
	// what a host that fell off the network looks like.
	require.Eventually(t, func() bool {
		return registerReaperWorker(t, "http://"+addr) == nil
	}, 5*time.Second, 50*time.Millisecond, "rc serve never came up")

	deadline := time.After(10 * time.Second)
	for {
		select {
		case e := <-delivered:
			if e.Kind == notify.KindWorkerLost {
				require.Equal(t, "reaperbox:gpu0", e.Device)
				require.Equal(t, "worker_lost", e.Reason)
				require.False(t, e.At.IsZero())
				return
			}
		case <-deadline:
			t.Fatal("the sweep's worker_lost event never reached the configured webhook")
		}
	}
}

// shorten swaps in a test-only value for one of the reaper's timings and
// restores the real one afterwards.
func shorten(t *testing.T, target *time.Duration, d time.Duration) {
	t.Helper()
	original := *target
	*target = d
	t.Cleanup(func() { *target = original })
}

func registerReaperWorker(t *testing.T, baseURL string) error {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"host":    "reaperbox",
		"devices": []map[string]string{{"name": "gpu0"}},
	})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/workers/register", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer wtok")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return nil
}

func TestResolveWebhookURLPrefersTheFlagOverTheEnvironment(t *testing.T) {
	t.Setenv("RC_WEBHOOK_URL", "http://from-env.example/hook")
	require.Equal(t, "http://from-flag.example/hook", resolveWebhookURL("http://from-flag.example/hook"))
	require.Equal(t, "http://from-env.example/hook", resolveWebhookURL(""))

	t.Setenv("RC_WEBHOOK_URL", "")
	require.Equal(t, "", resolveWebhookURL(""))
}
