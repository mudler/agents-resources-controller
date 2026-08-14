// Package e2e_test wires a real controller, a real worker, and a fake device
// (an ordinary shell command standing in for a GPU job) end to end: submit,
// hand out, run, stream, release, and reuse. It is the one test in this repo
// that proves the whole point of the project — that a device held by one job
// refuses a second claimant — rather than exercising any single package in
// isolation.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/logstore"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

// waitTerminalBound is generous on purpose: this test asserts behaviour, not
// speed, and a slow CI runner failing on a tight timeout is a worse outcome
// than a slow-but-correct pass.
const waitTerminalBound = 30 * time.Second

// Real controller, real worker, one fake device: submit -> run -> release,
// then prove the device is reusable and that a second claim is refused while
// the first is live.
func TestEndToEndClaimRunRelease(t *testing.T) {
	cl, _, device := newFleet(t, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 2. A job is submitted and handed to the worker. sleep 5, not 1: the
	// busy/holder assertions below run a few HTTP round trips after this
	// submit and need enough margin that the job is still reliably holding
	// the device by the time they execute — a tight margin here is already
	// the safe direction (a flake would read as failure, never a false
	// pass), but still worth avoiding.
	job, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID:  device,
		Command:   []string{"sh", "-c", "echo hello-from-device; sleep 5"},
		Submitter: "agent-a",
	})
	require.NoError(t, err, "first submit for the free device must succeed")
	require.Equal(t, device, job.DeviceID)

	// 3. THE CORE INVARIANT: while that job holds the device, a second submit
	// for the same device must never be handed the device too — not silently
	// accepted onto a second worker slot. Stage 2 queues a busy device by
	// default rather than refusing outright, so this uses NoWait to assert
	// the sharper claim: even opting out of the queue, the device does not
	// change hands. This is the entire reason the project exists — two jobs
	// must never hold one GPU.
	_, err = cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Command: []string{"true"}, Submitter: "agent-b", NoWait: true,
	})
	require.ErrorIs(t, err, client.ErrNoDevice,
		"a second submit for a held device must be refused with ErrNoDevice, not accepted")

	// Confirm the state view agrees: the device is busy and held by agent-a,
	// not just that the second Submit call happened to fail.
	state, err := cl.State(ctx)
	require.NoError(t, err)
	require.Len(t, state.Devices, 1)
	require.Equal(t, model.DeviceBusy, state.Devices[0].Device.State,
		"device must show busy while agent-a's job is live")
	require.Equal(t, "agent-a", state.Devices[0].Holder,
		"device view must report the actual holder, not the refused second claimant")

	// The job's output must reach the client through the live log stream.
	var out logBuffer
	require.NoError(t, cl.StreamLogs(ctx, job.ID, &out))
	require.Contains(t, out.String(), "hello-from-device",
		"log stream did not carry the job's stdout; got: %q", out.String())

	// 4. The job reaches a terminal state with the correct exit code.
	final, err := cl.WaitTerminal(ctx, job.ID, waitTerminalBound)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, final.State,
		"job %s ended in state %s (kill_reason=%q), want succeeded", final.ID, final.State, final.KillReason)
	require.NotNil(t, final.ExitCode, "terminal job must carry an exit code")
	require.Equal(t, 0, *final.ExitCode)

	// 5. The device returns to the pool afterwards...
	require.Eventually(t, func() bool {
		state, err := cl.State(ctx)
		return err == nil && len(state.Devices) == 1 && state.Devices[0].Device.State == model.DeviceReady
	}, 15*time.Second, 100*time.Millisecond, "device never returned to ready after the job finished")

	// ...and a later submit for the same device succeeds, landing a distinct job.
	again, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Command: []string{"true"}, Submitter: "agent-b",
	})
	require.NoError(t, err, "submit onto a freed device must succeed")
	require.NotEqual(t, job.ID, again.ID)

	final2, err := cl.WaitTerminal(ctx, again.ID, waitTerminalBound)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, final2.State)

	// Clean shutdown (the worker has no in-flight job left here, so Start
	// should return promptly with context.Canceled as its expected,
	// non-error exit) is asserted by newFleet's own t.Cleanup, not here —
	// see harness_test.go.
}

// TestWorkerRestartQuarantinesDeviceInsteadOfFalselyReady reproduces the
// critical review finding live: a worker crashes mid-job — nothing SIGTERMs
// the child, nothing reports a terminal state, the process just vanishes —
// and a fresh process registers under the same host-derived worker ID while
// the controller still believes the job is running. Registration must
// reconcile that stranded job rather than falsify the device as ready (an
// orphaned process from the dead worker may still be pinning the GPU) or
// leave it busy forever with nothing left to ever release it.
func TestWorkerRestartQuarantinesDeviceInsteadOfFalselyReady(t *testing.T) {
	dir := t.TempDir()
	c := clock.Real()

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	defer st.Close()

	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client"},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// wk's own context intentionally outlives this test's assertions and is
	// only cancelled at the very end: an orderly cancellation would make wk
	// SIGTERM its local job and report a terminal state, which is exactly
	// the well-behaved shutdown this test is NOT trying to exercise. A real
	// crash reports nothing and cleans up nothing, which is why the
	// mid-flight registration below must not be able to trust the device.
	wkCtx, cancelWk := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelWk()

	wk := worker.New(worker.Config{
		ControllerURL: ts.URL, Token: "wtok", Host: "restartbox", Devices: []worker.DeviceConfig{{Name: "dev0"}},
		HeartbeatInterval: time.Second, PollWait: time.Second,
	})
	go func() { _ = wk.Start(wkCtx) }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cl := client.New(ts.URL, "ctok")

	require.Eventually(t, func() bool {
		state, err := cl.State(ctx)
		return err == nil && len(state.Devices) == 1
	}, 15*time.Second, 100*time.Millisecond, "worker never registered its device")

	job, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID:  "restartbox:dev0",
		Command:   []string{"sleep", "30"},
		Submitter: "agent-a",
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		j, err := cl.Job(ctx, job.ID)
		return err == nil && j.State == model.JobRunning
	}, 15*time.Second, 100*time.Millisecond, "job never reached running")

	// Simulate the crash-restart on the wire: a fresh /v1/workers/register
	// call under the same host-derived worker ID, with nothing having
	// reported the old job's outcome. This is exactly what a real `rc
	// worker` restart looks like to the controller. wk's local "sleep 30" —
	// standing in for an orphaned CUDA process — is deliberately still alive
	// right now (its context is not cancelled until this test's cleanup),
	// which is precisely why the device must not come back as ready.
	registerRestart(t, ts.URL, "wtok", "restartbox", []string{"dev0"})

	state, err := cl.State(ctx)
	require.NoError(t, err)
	require.Len(t, state.Devices, 1)
	require.NotEqual(t, model.DeviceReady, state.Devices[0].Device.State,
		"a restarted worker must never leave the device ready while its lease was live")
	require.Equal(t, model.DeviceUnhealthy, state.Devices[0].Device.State,
		"the device must be quarantined pending an operator clear, not merely marked non-ready")
	require.Empty(t, state.Devices[0].Holder, "no live lease should remain once the stranded job is reaped")

	final, err := cl.Job(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, final.State)

	// And the invariant holds end to end: nothing can claim the quarantined
	// device until an operator clears it. NoWait for the same reason as
	// above: a quarantined device is never free, so even the queue-opt-out
	// path must fail rather than park a job behind a device that will never
	// become schedulable again on its own.
	_, err = cl.Submit(ctx, client.SubmitOptions{
		DeviceID: "restartbox:dev0", Command: []string{"true"}, Submitter: "agent-b", NoWait: true,
	})
	require.ErrorIs(t, err, client.ErrNoDevice,
		"a quarantined device must refuse a new claim, not silently hand out a GPU nobody has proven is clean")
}

// registerRestart posts a raw /v1/workers/register call, the same request a
// genuinely restarted `rc worker` process sends. It bypasses the worker
// package's own long-running Start loop so the test can register a second
// "process" for the same host without disturbing the first one's still-live
// local job.
func registerRestart(t *testing.T, baseURL, token, host string, devices []string) {
	t.Helper()
	specs := make([]server.DeviceSpec, 0, len(devices))
	for _, name := range devices {
		specs = append(specs, server.DeviceSpec{Name: name})
	}
	payload, err := json.Marshal(server.RegisterRequest{Host: host, Devices: specs})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/workers/register", bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

type logBuffer struct{ b []byte }

func (l *logBuffer) Write(p []byte) (int, error) {
	l.b = append(l.b, p...)
	return len(p), nil
}

func (l *logBuffer) String() string { return string(l.b) }
