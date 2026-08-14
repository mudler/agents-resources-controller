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
// It returns a client authenticated as a client-role token, the store (for
// tests that want to assert against it directly), and the device's ID
// ("testbox:dev0"). Everything it starts is torn down via t.Cleanup, which
// Go runs LIFO: the worker's context is cancelled (and its clean shutdown
// asserted) first, then the scheduler loop is stopped and joined, then the
// httptest server is closed, and only then is the store closed — so nothing
// registered here can touch the store after it's shut.
func newFleet(t *testing.T, deviceMaxRuntime time.Duration) (*client.Client, *store.Store, string) {
	t.Helper()

	dir := t.TempDir()
	c := clock.Real()

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client"},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

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
	wk := worker.New(worker.Config{
		ControllerURL: ts.URL, Token: "wtok", Host: fleetHost,
		Devices:           []worker.DeviceConfig{{Name: fleetDevice, MaxRuntime: deviceMaxRuntime}},
		HeartbeatInterval: time.Second, PollWait: time.Second,
	})
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
		case <-time.After(10 * time.Second):
			t.Error("worker did not shut down within 10s of context cancellation")
		}
	})

	cl := client.New(ts.URL, "ctok")

	registerCtx, cancelRegister := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelRegister()
	require.Eventually(t, func() bool {
		state, err := cl.State(registerCtx)
		return err == nil && len(state.Devices) == 1
	}, 15*time.Second, 100*time.Millisecond, "worker never registered its device")

	return cl, st, fleetHost + ":" + fleetDevice
}
