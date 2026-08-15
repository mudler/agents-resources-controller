package e2e_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/logstore"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/notify"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

// fleetHost and fleetDevice name the one worker/device every newFleet stands
// up. Tests refer to the device by the ID newFleet returns rather than
// hard-coding this pair, but the constants are still useful for anything
// that needs to talk to the wire API directly (see registerRestart).
const (
	fleetHost   = "testbox"
	fleetDevice = "dev0"

	// shutdownGraceMargin bounds how long newFleet's cleanup waits for the
	// worker to report a clean shutdown. It must sit above
	// internal/worker/worker.go's own shutdownGrace (45s — how long Start
	// waits for an in-flight job to finish and report before abandoning it),
	// with real margin on top for network round trips: a bound below that
	// would make the harness itself time out — a false "did not shut down"
	// failure — on a test that leaves a job legitimately still draining,
	// rather than ever giving the worker's own documented grace period a
	// chance to work as designed.
	shutdownGraceMargin = 90 * time.Second
)

// newFleet wires a real store, a real controller (server.New over an
// httptest.Server), and a real worker process with one device — the same
// shape every e2e test in this package needs. It also runs the periodic
// scheduler loop `rc serve` runs in production (internal/cli/serve.go):
// handleSubmit only ever makes one scheduling pass, at submit time, so
// without this loop a job queued behind a busy device would never be handed
// out once that device later frees up — exactly the behaviour the queue
// tests in queue_test.go exist to prove.
//
// deviceMaxRuntime, if nonzero, is the runtime ceiling the worker declares
// for its one device at registration (worker.DeviceConfig.MaxRuntime),
// exercised by TestWatchdogKillsAnOverrunningJob.
//
// opts customize the fleet beyond what deviceMaxRuntime already covers — see
// fleetOption and its withX constructors, used by hold_test.go (lifecycle
// hooks), verify_test.go (a verify directory) and notify_test.go (a
// controller notifier) to exercise those features against a real worker
// rather than reimplementing this whole setup a second time.
//
// It returns a client authenticated as a client-role token, the store (for
// tests that want to assert against it directly), and the device's ID
// ("testbox:dev0"). Everything it starts is torn down via t.Cleanup, which
// Go runs LIFO: the worker's context is cancelled (and its clean shutdown
// asserted) first, then the scheduler loop is stopped and joined, then the
// httptest server is closed, and only then is the store closed — so nothing
// registered here can touch the store after it's shut.
func newFleet(t *testing.T, deviceMaxRuntime time.Duration, opts ...fleetOption) (*client.Client, *store.Store, string) {
	t.Helper()

	dir := t.TempDir()
	c := clock.Real()

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	fc := fleetConfig{
		server: server.Config{
			Store: st, Logs: logs, Clock: c,
			Tokens: map[string]string{"wtok": "worker", "ctok": "client", "atok": "admin"},
		},
		worker: worker.Config{
			Token: "wtok", Host: fleetHost,
			Devices:           []worker.DeviceConfig{{Name: fleetDevice, MaxRuntime: deviceMaxRuntime}},
			HeartbeatInterval: time.Second, PollWait: time.Second,
		},
	}
	for _, opt := range opts {
		opt(&fc)
	}

	srv := server.New(fc.server)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	if fc.urlOut != nil {
		*fc.urlOut = ts.URL
	}
	// Registered before the worker and the scheduler loop, so LIFO teardown
	// stops both of those BEFORE this runs: nothing is still producing
	// events by the time the notifier drains, which is the same ordering
	// cli.NewServeCmd's own shutdown defers are careful to get right. (An
	// event emitted after this point is dropped, not a panic — every
	// notify.Notifier method is safe post-Close.)
	if fc.server.Notifier != nil {
		t.Cleanup(func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = fc.server.Notifier.Close(closeCtx)
		})
	}

	// The scheduler loop, mirroring cli.NewServeCmd: notices a device
	// freeing up (a job finishing, a worker re-registering) and hands the
	// next queued job to it. Runs on its own context so it stops cleanly
	// before the store is closed, regardless of what the test's own
	// context does.
	schedCtx, cancelSched := context.WithCancel(context.Background())
	schedDone := make(chan struct{})
	go func() {
		defer close(schedDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-schedCtx.Done():
				return
			case <-ticker.C:
				assigned, err := st.ScheduleOnce()
				if err != nil {
					continue
				}
				for _, job := range assigned {
					srv.Poke(job.WorkerID)
				}
			}
		}
	}()
	t.Cleanup(func() {
		cancelSched()
		<-schedDone
	})

	wkCtx, cancelWk := context.WithCancel(context.Background())
	fc.worker.ControllerURL = ts.URL // known only now, so it is not an option's to set
	wk := worker.New(fc.worker)
	workerDone := make(chan error, 1)
	go func() { workerDone <- wk.Start(wkCtx) }()
	// Clean shutdown: with no in-flight job left by the time a test ends
	// (every test here either finishes or kills its jobs before returning),
	// Start should return promptly once wkCtx is cancelled, with
	// context.Canceled as its expected, non-error exit. This used to be
	// asserted only by TestEndToEndClaimRunRelease's own body; folding it
	// into the harness means every test built on newFleet now gets that
	// coverage for free.
	t.Cleanup(func() {
		cancelWk()
		select {
		case err := <-workerDone:
			require.ErrorIs(t, err, context.Canceled,
				"worker did not report ordinary shutdown as context.Canceled")
		case <-time.After(shutdownGraceMargin):
			t.Error("worker did not shut down within the shutdown-grace margin of context cancellation")
		}
	})

	cl := client.New(ts.URL, "ctok")

	registerCtx, cancelRegister := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelRegister()
	require.Eventually(t, func() bool {
		state, err := cl.State(registerCtx)
		return err == nil && len(state.Devices) == 1 && hasDetectedCPUs(state.Devices[0].Device)
	}, 15*time.Second, 100*time.Millisecond,
		"worker never finished registering its device (no detected cpus label ever landed)")

	return cl, st, fleetHost + ":" + fleetDevice
}

// hasDetectedCPUs reports whether a device carries the detected "cpus" label,
// which is what newFleet (and TestWorkerRestartQuarantinesDeviceInsteadOfFalselyReady,
// which gates itself the same way) waits for instead of the mere existence of
// a device row.
//
// Why this and not "one device exists": handleRegister
// (internal/server/worker_api.go) does four things in order inside ONE
// handler — UpsertWorker creates the device rows, SetDeviceMaxRuntime writes
// each device's runtime ceiling, applyDeviceFacts writes labels and sheets,
// publishDevices announces the lot. A gate that only waits for the device row
// is satisfied by the FIRST of those, so newFleet could return with the test
// body racing the remaining three: a test that writes labels of its own (the
// selector tests) could have them overwritten a moment later by
// applyDeviceFacts, and a test that depends on the device's runtime ceiling
// (TestWatchdogKillsAnOverrunningJob, whose worker takes its limit from the
// assignment, which comes from the device row SetDeviceMaxRuntime writes)
// could submit before that ceiling existed and watch its job run unbounded.
// Roughly 100ms of accidental slack normally hid this; a deliberate 105ms
// sleep inserted after UpsertWorker returns and before the two writes that
// follow it turned the two into 5/10 and 7/10 failures respectively.
//
// The "cpus" label is the right thing to wait on because it is written by the
// LAST store call the handler makes: builtinLabels (internal/worker/probe.go)
// emits it unconditionally from runtime.NumCPU, so it is never absent for
// environmental reasons the way mem_total_bytes or a GPU fact can be, and it
// is HOST-scoped, so applyDeviceFacts' host-then-device merge puts it on every
// device. Observing it therefore proves SetDeviceMaxRuntime landed too, which
// is what closes the watchdog exposure with the same wait.
//
// It proves only that the FIRST registration completed. ProbeInterval
// defaults to 5 minutes (internal/worker/config.go), so a worker's periodic
// label push cannot collide with anything inside a test today — but a test
// that shortened that interval would make direct label injection racy again,
// and this wait would not save it.
func hasDetectedCPUs(d model.Device) bool {
	for _, l := range d.Labels {
		if l.Key == "cpus" && l.Source == model.SourceDetected {
			return true
		}
	}
	return false
}

// fleetConfig is everything about newFleet's fleet a test is allowed to
// change: the controller's own configuration (today, whether it has a
// notifier), the worker's (its one device, its verify directory), and an
// optional out-parameter for the controller URL.
//
// The two configs are collected BEFORE anything is constructed because they
// are consumed at different moments — server.Config at server.New, before the
// httptest listener exists, and worker.Config after it, since the worker
// needs that listener's URL. Options therefore describe the fleet rather than
// mutate a live one.
type fleetConfig struct {
	server server.Config
	worker worker.Config
	urlOut *string
}

// fleetOption customizes the fleet newFleet stands up, for a test that needs
// more than the bare one-device-with-a-ceiling every other test here is
// content with.
type fleetOption func(*fleetConfig)

// withHooks arms the device's acquire/release lifecycle hooks and shortens
// its release_linger to lingerSeconds — long enough to keep the hold_test.go
// scenario (queue a job behind a hold, then release it) from tripping the
// linger's own "another job landed, skip the release" rule, but short enough
// that the test can still watch the release actually fire on a real, though
// generous, require.Eventually bound instead of waiting out the 30s
// production default.
func withHooks(onAcquire, onRelease string, linger time.Duration) fleetOption {
	return func(fc *fleetConfig) {
		d := &fc.worker.Devices[0]
		d.OnAcquire = onAcquire
		d.OnRelease = onRelease
		d.ReleaseLinger = linger
	}
}

// withLabels gives newFleet's device declared labels (worker.yaml's
// devices[].labels), so a test that needs facts distinguishing this device
// from another can get them through registration — the way an operator would
// — instead of writing them into the store behind the controller's back.
//
// This is an ADDITION, not a replacement for the store-side injection the
// selector tests do: those deliberately exercise the DETECTED source, which
// no configuration can assert, and the registration wait above is what makes
// writing that source directly sound. Use this when the source does not
// matter and you just want the fact to be there before the test body starts.
func withLabels(labels map[string]string) fleetOption {
	return func(fc *fleetConfig) {
		fc.worker.Devices[0].Labels = labels
	}
}

// withVerifyDir points the worker's post-job verify pass at dir. A worker
// whose VerifyDir does not exist runs no verify pass at all (the feature is
// off unless scripts exist), which is what every other test here gets: the
// default is /etc/rc/verify.d, which is absent on a test machine.
func withVerifyDir(dir string) fleetOption {
	return func(fc *fleetConfig) {
		fc.worker.VerifyDir = dir
	}
}

// withNotifier gives the controller a notifier, so a test can watch the
// events an operator's webhook would receive. newFleet closes it during
// teardown, after the worker and scheduler have stopped.
func withNotifier(n *notify.Notifier) fleetOption {
	return func(fc *fleetConfig) {
		fc.server.Notifier = n
	}
}

// withControllerURL captures the URL of the httptest controller newFleet
// stands up. It exists for the one route the client package does not wrap:
// the admin-only POST /v1/devices/{id}/clear, which verify_test.go calls
// over the wire (with the harness's "atok" admin token) rather than reaching
// into the store, so what it proves is the operator-facing recovery path and
// not a store method no operator can call.
func withControllerURL(dst *string) fleetOption {
	return func(fc *fleetConfig) {
		fc.urlOut = dst
	}
}
