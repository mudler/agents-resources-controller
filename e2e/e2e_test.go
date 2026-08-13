// Package e2e_test wires a real controller, a real worker, and a fake device
// (an ordinary shell command standing in for a GPU job) end to end: submit,
// hand out, run, stream, release, and reuse. It is the one test in this repo
// that proves the whole point of the project — that a device held by one job
// refuses a second claimant — rather than exercising any single package in
// isolation.
package e2e_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/mudler/resource-controller/internal/worker"
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL: ts.URL, Token: "wtok", Host: "testbox", Devices: []string{"dev0"},
		HeartbeatInterval: time.Second, PollWait: time.Second,
	})
	workerDone := make(chan error, 1)
	go func() { workerDone <- wk.Start(ctx) }()

	cl := client.New(ts.URL, "ctok")

	// 1. The worker registers and its device becomes visible.
	require.Eventually(t, func() bool {
		state, err := cl.State(ctx)
		return err == nil && len(state.Devices) == 1
	}, 15*time.Second, 100*time.Millisecond, "worker never registered its device")

	// 2. A job is submitted and handed to the worker.
	job, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID:  "testbox:dev0",
		Command:   []string{"sh", "-c", "echo hello-from-device; sleep 1"},
		Submitter: "agent-a",
	})
	require.NoError(t, err, "first submit for the free device must succeed")
	require.Equal(t, "testbox:dev0", job.DeviceID)

	// 3. THE CORE INVARIANT: while that job holds the device, a second submit
	// for the same device must be refused, not queued and not silently
	// accepted onto a second worker slot. This is the entire reason the
	// project exists — two jobs must never hold one GPU.
	_, err = cl.Submit(ctx, client.SubmitOptions{
		DeviceID: "testbox:dev0", Command: []string{"true"}, Submitter: "agent-b",
	})
	require.ErrorIs(t, err, client.ErrNoDevice,
		"a second submit for a held device must be refused with ErrNoDevice, not accepted or queued")

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
		DeviceID: "testbox:dev0", Command: []string{"true"}, Submitter: "agent-b",
	})
	require.NoError(t, err, "submit onto a freed device must succeed")
	require.NotEqual(t, job.ID, again.ID)

	final2, err := cl.WaitTerminal(ctx, again.ID, waitTerminalBound)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, final2.State)

	// Clean shutdown: the worker has no in-flight job left, so Start should
	// return promptly once ctx is cancelled, with context.Canceled as its
	// (expected, non-error) exit.
	cancel()
	select {
	case err := <-workerDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not shut down within 10s of context cancellation")
	}
}

type logBuffer struct{ b []byte }

func (l *logBuffer) Write(p []byte) (int, error) {
	l.b = append(l.b, p...)
	return len(p), nil
}

func (l *logBuffer) String() string { return string(l.b) }
