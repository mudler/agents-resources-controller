package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/logstore"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/notify"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// recordingSink is a notify.Sink that keeps every event handed to it. It
// never fails, so no retry or backoff is ever exercised here — these tests
// are about WHICH events the controller emits, not about how delivery
// behaves under failure (internal/notify already covers that).
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

func (r *recordingSink) snapshot() []notify.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]notify.Event(nil), r.events...)
}

// newNotifyingServer mirrors newServer but wires a real notify.Notifier over
// a recording sink, and hands the test the notifier itself so it can flush.
// It registers a worker with two devices: gpu0, the one every test acts on,
// and gpu1, which exists only so a test can prove an event names the right
// device rather than the only device.
func newNotifyingServer(t *testing.T) (*httptest.Server, *store.Store, *notify.Notifier, *recordingSink) {
	t.Helper()
	dir := t.TempDir()
	c := clock.NewFake(time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC))

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })

	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	sink := &recordingSink{}
	n := notify.New(sink, notify.Options{QueueSize: 64, Attempts: 1})
	require.NotNil(t, n)
	t.Cleanup(func() { _ = n.Close(context.Background()) })

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c, Notifier: n,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client", "atok": "admin"},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	resp := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}, {Name: "gpu1"}},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	return ts, st, n, sink
}

// flush closes the notifier and returns everything that was delivered, with
// At zeroed so events can be compared literally.
//
// Closing is what makes "emitted nothing" assertable at all: every HTTP
// request these tests make has already returned by this point, so any event
// its handler emitted is already on the notifier's queue, and Close drains
// that queue and waits for the delivery goroutine to exit before returning.
// What the sink holds afterwards is therefore the complete, final set — not
// a snapshot that another event might still be racing to join.
func flush(t *testing.T, n *notify.Notifier, sink *recordingSink) []notify.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, n.Close(ctx))
	require.Zero(t, n.Dropped(), "the notifier dropped events; the test's queue is too small to judge what was emitted")

	got := sink.snapshot()
	out := make([]notify.Event, 0, len(got))
	for _, e := range got {
		require.False(t, e.At.IsZero(), "every emitted event must carry a timestamp")
		e.At = time.Time{}
		out = append(out, e)
	}
	return out
}

func faultDevice(t *testing.T, ts *httptest.Server, deviceID, reason string) {
	t.Helper()
	resp := post(t, ts, "wtok", "/v1/devices/"+deviceID+"/fault", server.FaultRequest{Reason: reason})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// A verify failure IS a device going unhealthy, so the naive wiring emits
// both and a consumer counting device_unhealthy double counts. Exactly one
// event, and it is the specific one.
func TestVerifyFaultEmitsVerifyFailedAndNothingElse(t *testing.T) {
	ts, st, n, sink := newNotifyingServer(t)

	// A job that has already finished on this device. It must NOT be named
	// by a later fault: "the job that dirtied the device" means the one
	// still in flight, and blaming the last job to touch the hardware would
	// point an operator at a run that ended cleanly some time ago.
	past, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	code := 0
	require.NoError(t, st.Release(past.ID, model.JobSucceeded, &code, ""))

	// The literal prefix internal/worker/verify.go stamps on every
	// verify-sourced reason. It is the only thing that tells this fault
	// apart from a lifecycle hook's, which posts to the same endpoint.
	reason := "verify failed: 10-vram.sh: 512 MiB still allocated"
	faultDevice(t, ts, "gpubox:gpu0", reason)

	require.Equal(t, []notify.Event{
		{Kind: notify.KindVerifyFailed, Device: "gpubox:gpu0", Reason: reason},
	}, flush(t, n, sink))
}

// A fault from any other source — today, a failed on_acquire hook — is a
// plain device_unhealthy.
func TestHookFaultEmitsDeviceUnhealthy(t *testing.T) {
	ts, _, n, sink := newNotifyingServer(t)

	reason := "on_acquire hook exited 1: could not stop ollama.service"
	faultDevice(t, ts, "gpubox:gpu1", reason)

	require.Equal(t, []notify.Event{
		{Kind: notify.KindDeviceUnhealthy, Device: "gpubox:gpu1", Reason: reason},
	}, flush(t, n, sink))
}

// A verify failure happens while the job that dirtied the device is still
// in flight (the worker verifies before it reports the job terminal), so the
// event can and must name that job.
func TestVerifyFaultNamesTheJobStillOnTheDevice(t *testing.T) {
	ts, st, n, sink := newNotifyingServer(t)

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	reason := "verify failed: 10-vram.sh: 512 MiB still allocated"
	faultDevice(t, ts, "gpubox:gpu0", reason)

	require.Equal(t, []notify.Event{
		{Kind: notify.KindVerifyFailed, Device: "gpubox:gpu0", Job: job.ID, Reason: reason},
	}, flush(t, n, sink))
}

// The watchdog mapping, and the far more important half of it: which
// terminal reports must emit NOTHING. A job killed by `rc kill` or by a
// worker shutting down is not a watchdog trip, so matching on the `killed`
// state instead of on the reason the watchdog actually writes would page an
// operator for every ordinary cancellation.
func TestTerminalReportEmitsWatchdogTripOnlyForAWatchdogReason(t *testing.T) {
	cases := []struct {
		name   string
		state  model.JobState
		reason string
		want   []notify.Event
	}{
		{
			name:   "max_runtime watchdog",
			state:  model.JobKilled,
			reason: "max_runtime exceeded (4h0m0s)",
			want: []notify.Event{{
				Kind: notify.KindWatchdogTrip, Device: "gpubox:gpu0",
				Reason: "max_runtime exceeded (4h0m0s)",
			}},
		},
		{
			name:   "idle watchdog",
			state:  model.JobKilled,
			reason: "idle: no output for 10m0s",
			want: []notify.Event{{
				Kind: notify.KindWatchdogTrip, Device: "gpubox:gpu0",
				Reason: "idle: no output for 10m0s",
			}},
		},
		{
			name:   "rc kill, which the worker reports as cancelled",
			state:  model.JobKilled,
			reason: "cancelled",
		},
		{
			name:   "a kill whose process group needed forcing out",
			state:  model.JobKilled,
			reason: "cancelled; survivors remained after SIGKILL",
		},
		{
			name:   "a worker shutting down, reporting no reason at all",
			state:  model.JobKilled,
			reason: "",
		},
		{
			name:   "an ordinary failure",
			state:  model.JobFailed,
			reason: "exit status 1",
		},
		{
			name:  "an ordinary success",
			state: model.JobSucceeded,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts, st, n, sink := newNotifyingServer(t)

			job, err := st.Allocate(store.AllocateRequest{
				DeviceID: "gpubox:gpu0", Command: []string{"./bench"},
				Submitter: "agent-a", LeaseTTL: time.Minute,
			})
			require.NoError(t, err)

			code := 0
			resp := post(t, ts, "wtok", "/v1/jobs/"+job.ID+"/status", server.StatusRequest{
				WorkerID: job.WorkerID, State: tc.state, ExitCode: &code, Reason: tc.reason,
			})
			require.Equal(t, http.StatusOK, resp.StatusCode)
			resp.Body.Close()

			want := tc.want
			for i := range want {
				want[i].Job = job.ID
			}
			got := flush(t, n, sink)
			if len(want) == 0 {
				require.Empty(t, got, "this terminal report must emit nothing at all")
				return
			}
			require.Equal(t, want, got)
		})
	}
}

// A "running" report is not terminal and must never be mistaken for one,
// whatever it carries.
func TestRunningReportEmitsNothing(t *testing.T) {
	ts, st, n, sink := newNotifyingServer(t)

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	resp := post(t, ts, "wtok", "/v1/jobs/"+job.ID+"/status", server.StatusRequest{
		WorkerID: job.WorkerID, State: model.JobRunning, Reason: "max_runtime exceeded (4h0m0s)",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	require.Empty(t, flush(t, n, sink))
}

// A fault the store rejected changed nothing, so it must announce nothing.
func TestFaultOnUnknownDeviceEmitsNothing(t *testing.T) {
	ts, _, n, sink := newNotifyingServer(t)

	resp := post(t, ts, "wtok", "/v1/devices/nosuchbox:gpu9/fault",
		server.FaultRequest{Reason: "verify failed: 10-vram.sh: dirty"})
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	resp.Body.Close()

	require.Empty(t, flush(t, n, sink))
}

// No webhook configured is the default deployment: Config.Notifier is nil
// and every emit site must be a no-op rather than a panic.
func TestNoNotifierConfiguredIsSafe(t *testing.T) {
	ts, st, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	faultDevice(t, ts, "gpubox:gpu0", "verify failed: 10-vram.sh: dirty")

	code := 0
	sr := post(t, ts, "wtok", "/v1/jobs/"+job.ID+"/status", server.StatusRequest{
		WorkerID: job.WorkerID, State: model.JobKilled, ExitCode: &code,
		Reason: "max_runtime exceeded (4h0m0s)",
	})
	require.Equal(t, http.StatusOK, sr.StatusCode)
	sr.Body.Close()
}
