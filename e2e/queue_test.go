// Package e2e_test's queue tests cover what changed in stage 2: a busy
// device now means "you are next" instead of "try again", per-device runtime
// ceilings are enforced by a real watchdog on a real worker (not a mocked
// clock), and rc kill actually removes a job from the queue before it ever
// runs. All three run against the same real store, server, and worker that
// e2e_test.go's happy path uses, via the shared newFleet harness.
package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/stretchr/testify/require"
)

// Two jobs, one device: the second waits and then runs. The marker files
// prove they never overlapped — which is the whole point of the system.
func TestQueuedJobRunsAfterTheFirstFinishes(t *testing.T) {
	cl, _, device := newFleet(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dir := t.TempDir()
	markers := filepath.Join(dir, "markers")

	first, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-a",
		Command: []string{"sh", "-c", "echo a-start >> " + markers + "; sleep 3; echo a-end >> " + markers},
	})
	require.NoError(t, err)

	second, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-b",
		Command: []string{"sh", "-c", "echo b-start >> " + markers + "; echo b-end >> " + markers},
	})
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, second.State, "the device is busy, so the second job queues")

	view, err := cl.JobView(ctx, second.ID)
	require.NoError(t, err)
	require.Equal(t, 1, view.QueuePosition)

	require.Eventually(t, func() bool {
		v, err := cl.JobView(ctx, second.ID)
		return err == nil && v.Job.State == model.JobSucceeded
	}, 60*time.Second, 250*time.Millisecond, "the queued job never ran")

	firstFinal, err := cl.Job(ctx, first.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, firstFinal.State)

	body, err := os.ReadFile(markers)
	require.NoError(t, err)
	require.Equal(t, "a-start\na-end\nb-start\nb-end\n", string(body),
		"the jobs interleaved, so they held the device at the same time")
}

// A device declaring a 2s ceiling must not let a 30s job outlive it.
func TestWatchdogKillsAnOverrunningJob(t *testing.T) {
	cl, _, device := newFleet(t, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	job, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-a",
		Command: []string{"sh", "-c", "sleep 30"},
	})
	require.NoError(t, err)

	var final *model.Job
	require.Eventually(t, func() bool {
		j, err := cl.Job(ctx, job.ID)
		if err != nil || !j.State.Terminal() {
			return false
		}
		final = j
		return true
	}, 60*time.Second, 250*time.Millisecond, "the watchdog never fired")

	require.Equal(t, model.JobKilled, final.State)
	require.Contains(t, final.KillReason, "max_runtime")

	require.Eventually(t, func() bool {
		state, err := cl.State(ctx)
		return err == nil && len(state.Devices) == 1 &&
			state.Devices[0].Device.State == model.DeviceReady
	}, 30*time.Second, 250*time.Millisecond, "the device did not return to the pool")
}

func TestKillCancelsAQueuedJobEndToEnd(t *testing.T) {
	cl, _, device := newFleet(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")

	_, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-a",
		Command: []string{"sh", "-c", "sleep 4"},
	})
	require.NoError(t, err)

	queued, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-b",
		Command: []string{"sh", "-c", "echo ran > " + marker},
	})
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, queued.State)

	require.NoError(t, cl.Kill(ctx, queued.ID, "agent-b"))

	killed, err := cl.Job(ctx, queued.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobKilled, killed.State)

	// Wait past the point where it would have been scheduled had it survived.
	require.Eventually(t, func() bool {
		state, err := cl.State(ctx)
		return err == nil && len(state.Queued) == 0 &&
			len(state.Devices) == 1 && state.Devices[0].Device.State == model.DeviceReady
	}, 60*time.Second, 250*time.Millisecond, "the queue never drained")

	_, statErr := os.Stat(marker)
	require.True(t, os.IsNotExist(statErr), "a killed job must never execute")
}
