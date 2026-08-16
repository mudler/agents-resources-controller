package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/notify"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/assert"
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
	}, sweepEvents(res, reasons, nil))
}

// A device demoted to unknown is not quarantined — it may well be back on
// the next heartbeat — so it is not news anyone should be paged for.
func TestSweepEventsIgnoreDevicesDemotedToUnknown(t *testing.T) {
	res := store.SweepResult{DevicesUnknown: []string{"gpubox:gpu0"}}
	require.Empty(t, sweepEvents(res, nil, nil))
}

// One device, two reasons to report it in the same sweep: it is in
// DevicesUnhealthy AND it is the device behind an expired lease. It left the
// pool once, so it gets exactly one device-level event, whichever cause the
// store ended up recording — a consumer counting unhealthy devices must not
// be told twice about one quarantine.
//
// The second case is the one that matters for the dedupe specifically: when
// the recorded reason is the expiry itself, nothing else stops the
// lease-expiry branch from announcing a device the loop above already did.
func TestSweepEventsAnnounceADeviceOnceEvenWhenBothCausesFire(t *testing.T) {
	res := store.SweepResult{
		DevicesUnhealthy: []string{"gpubox:gpu0"},
		LeasesExpired:    []string{"job-1"},
	}
	leaseDevices := map[string]string{"job-1": "gpubox:gpu0"}

	cases := []struct {
		name   string
		reason string
		want   notify.Event
	}{
		{
			name:   "recorded as worker loss",
			reason: "worker_lost",
			want:   notify.Event{Kind: notify.KindWorkerLost, Device: "gpubox:gpu0", Reason: "worker_lost"},
		},
		{
			name:   "recorded as the expiry itself",
			reason: "lease_expired",
			want:   notify.Event{Kind: notify.KindDeviceUnhealthy, Device: "gpubox:gpu0", Reason: "lease_expired"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, []notify.Event{
				tc.want,
				{Kind: notify.KindLeaseExpired, Device: "gpubox:gpu0", Job: "job-1", Reason: "lease expired"},
			}, sweepEvents(res, map[string]string{"gpubox:gpu0": tc.reason}, leaseDevices))
		})
	}
}

// A device whose lease expired but which was ALREADY out of the pool for
// another cause keeps that cause, and was announced when it happened. Only
// the expiry itself is news this time.
func TestSweepEventsDoNotReAnnounceADeviceQuarantinedForAnotherCause(t *testing.T) {
	res := store.SweepResult{LeasesExpired: []string{"job-1"}}
	reasons := map[string]string{"gpubox:gpu0": "fault"}
	leaseDevices := map[string]string{"job-1": "gpubox:gpu0"}

	require.Equal(t, []notify.Event{
		{Kind: notify.KindLeaseExpired, Device: "gpubox:gpu0", Job: "job-1", Reason: "lease expired"},
	}, sweepEvents(res, reasons, leaseDevices))
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

	// The lease's device is quarantined by that same expiry, and SweepResult
	// says nothing about it at all — but a device leaving the pool is the
	// whole point of this event stream, so it is recovered from the lease
	// row and announced alongside.
	devices, err := st.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State, "the expiry did quarantine the device")

	require.Equal(t, []notify.Event{
		{Kind: notify.KindLeaseExpired, Device: "gpubox:gpu0", Job: job.ID, Reason: "lease expired"},
		{Kind: notify.KindDeviceUnhealthy, Device: "gpubox:gpu0", Reason: "lease_expired"},
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
		return registerWorkerRaw(t, "http://"+addr, "reaperbox") == nil
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

// An event emitted moments before the controller stops must still be
// delivered. Nothing else in this package would notice if the notifier were
// simply never closed: the delivery goroutine would leak, rc serve would
// return while the event was still in flight, and the event would vanish
// with the process — at exactly the moment an operator most wants to know
// what happened.
//
// The webhook here answers slowly on purpose. That makes the property
// measurable without any polling: the assertion is that the event has ALREADY
// been delivered by the time cmd.Execute() returns, which can only be true
// if shutdown waited for the queue to drain.
func TestAnEventEmittedJustBeforeShutdownIsStillDelivered(t *testing.T) {
	const webhookDelay = 500 * time.Millisecond

	delivered := make(chan notify.Event, 8)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e notify.Event
		err := json.NewDecoder(r.Body).Decode(&e)
		time.Sleep(webhookDelay)
		if err == nil {
			select {
			case delivered <- e:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(hook.Close)

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
	defer cancel()

	baseURL := "http://" + addr
	require.Eventually(t, func() bool {
		return registerWorkerRaw(t, baseURL, "shutdownbox") == nil
	}, 5*time.Second, 50*time.Millisecond, "rc serve never came up")

	require.Equal(t, http.StatusOK, faultDeviceRaw(t, baseURL, "shutdownbox:gpu0",
		"verify failed: 10-vram.sh: 512 MiB still allocated"))

	// No pause here: the event is meant to still be in flight, or still
	// queued, when the controller is told to stop.
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("rc serve did not shut down")
	}

	select {
	case e := <-delivered:
		require.Equal(t, notify.KindVerifyFailed, e.Kind)
		require.Equal(t, "shutdownbox:gpu0", e.Device)
	default:
		t.Fatal("rc serve returned without delivering the event still queued at shutdown; " +
			"the notifier was not drained on the way out")
	}
}

// The third emitter is an HTTP request handler, and ListenAndServe returns
// when the LISTENER closes, not when handlers finish. So a fault POST caught
// in that window would answer 200 while its event was dropped by an
// already-closed notifier: a worker told its quarantine was recorded, and an
// operator never told a device left the pool. The same window could also
// hand a live handler a database that st.Close had already closed.
//
// The handler is held in flight without any seam in production code: the
// request's body is written in two pieces over a raw connection, leaving the
// handler blocked decoding it. A test-only delay inside the handler would
// have proven the same thing, but this way there is nothing extra in the
// binary an operator runs, and the mechanism is one a real slow client
// reproduces on its own.
func TestAnEventFromAHandlerStillInFlightAtShutdownIsStillDelivered(t *testing.T) {
	delivered := make(chan notify.Event, 8)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e notify.Event
		if err := json.NewDecoder(r.Body).Decode(&e); err == nil {
			select {
			case delivered <- e:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(hook.Close)

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
	defer cancel()

	require.Eventually(t, func() bool {
		return registerWorkerRaw(t, "http://"+addr, "inflightbox") == nil
	}, 5*time.Second, 50*time.Millisecond, "rc serve never came up")

	body := `{"reason":"verify failed: 10-vram.sh: 512 MiB still allocated"}`
	head := "POST /v1/devices/inflightbox:gpu0/fault HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Authorization: Bearer wtok\r\n" +
		"Content-Type: application/json\r\n" +
		"Content-Length: " + strconv.Itoa(len(body)) + "\r\n" +
		"Connection: close\r\n\r\n"

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(20*time.Second)))

	// Half a JSON object: the handler is now blocked inside its decode.
	_, err = conn.Write([]byte(head + body[:20]))
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	cancel()
	time.Sleep(200 * time.Millisecond)

	// Now let the handler finish. Its emit happens after the controller was
	// already told to stop.
	_, err = conn.Write([]byte(body[20:]))
	require.NoError(t, err)

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err, "the in-flight request never got a response")
	defer resp.Body.Close()
	// assert, not require: the two symptoms of the same missing join are
	// worth seeing together — the handler running against a store that was
	// closed under it, AND its event never being delivered — rather than
	// the first one hiding the second.
	assert.Equal(t, http.StatusOK, resp.StatusCode,
		"the handler ran against a store that had already been closed under it")

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("rc serve did not shut down")
	}

	select {
	case e := <-delivered:
		require.Equal(t, notify.KindVerifyFailed, e.Kind)
		require.Equal(t, "inflightbox:gpu0", e.Device)
	default:
		t.Fatal("the fault answered 200 but its event was never delivered; " +
			"shutdown drained the notifier while the handler was still running")
	}
}

// Every test that drives the reaper overrides both timings through shorten,
// so nothing else in the suite would notice if the production defaults were
// changed — the seam that made the reaper provable also made its real
// settings unasserted. These are the values the controller actually ships
// with: ten seconds between sweeps, and five minutes of silence before a
// worker is written off.
func TestReaperTimingDefaultsAreTheShippedOnes(t *testing.T) {
	require.Equal(t, 10*time.Second, sweepInterval)
	require.Equal(t, 5*time.Minute, sweepUnhealthyAfter)
}

// shorten swaps in a test-only value for one of the reaper's timings and
// restores the real one afterwards.
func shorten(t *testing.T, target *time.Duration, d time.Duration) {
	t.Helper()
	original := *target
	*target = d
	t.Cleanup(func() { *target = original })
}

// registerWorkerRaw registers a one-device worker over plain HTTP, returning
// the transport error unswallowed so a caller can poll it until the
// controller is listening.
func registerWorkerRaw(t *testing.T, baseURL, host string) error {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"host":    host,
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

func faultDeviceRaw(t *testing.T, baseURL, deviceID, reason string) int {
	t.Helper()
	body, err := json.Marshal(map[string]string{"reason": reason})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/devices/"+deviceID+"/fault", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer wtok")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestResolveWebhookURLPrefersTheFlagOverTheEnvironment(t *testing.T) {
	t.Setenv("RC_WEBHOOK_URL", "http://from-env.example/hook")
	require.Equal(t, "http://from-flag.example/hook", resolveWebhookURL("http://from-flag.example/hook"))
	require.Equal(t, "http://from-env.example/hook", resolveWebhookURL(""))

	t.Setenv("RC_WEBHOOK_URL", "")
	require.Equal(t, "", resolveWebhookURL(""))
}
