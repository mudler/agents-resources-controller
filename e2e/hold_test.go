// Package e2e_test's hold test covers what stage 3 added on top of the
// queue: rc hold occupies a device like any job would (it IS a job, with
// kind "hold" — see internal/store/allocate.go's assignQueued), an ordinary
// job submitted while the hold is live queues behind it exactly like it
// would behind any other job, and releasing the hold lets that queued job
// run — all while the device's acquire/release lifecycle hooks (task 5)
// fire exactly once each across the whole sequence, not once per job.
package e2e_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/stretchr/testify/require"
)

// TestHoldQueuesAJobBehindItAndFiresHooksOnce is the end-to-end proof of the
// task 8 design note: a hold is a job, not a second lease mechanism, so it
// goes through the exact same allocation, queue, and lifecycle-hook path an
// ordinary job does.
func TestHoldQueuesAJobBehindItAndFiresHooksOnce(t *testing.T) {
	dir := t.TempDir()
	acquireLog := filepath.Join(dir, "acquire.log")
	releaseLog := filepath.Join(dir, "release.log")
	acquireScript := filepath.Join(dir, "on-acquire.sh")
	releaseScript := filepath.Join(dir, "on-release.sh")
	require.NoError(t, os.WriteFile(acquireScript,
		[]byte("#!/bin/sh\necho acquire >> "+acquireLog+"\n"), 0o700))
	require.NoError(t, os.WriteFile(releaseScript,
		[]byte("#!/bin/sh\necho release >> "+releaseLog+"\n"), 0o700))

	// linger is short (well under a production release_linger) but still
	// generous relative to how fast a kill flag actually reaches the worker
	// (README: "normally reaches the worker within milliseconds") and the
	// scheduler's 100ms tick: the queued job below needs to land on the
	// freed device comfortably before this elapses, or its own on_acquire
	// would NOT be skipped and the "exactly once" assertions below would be
	// legitimately violated, not flaky.
	const linger = 5 * time.Second
	cl, _, device := newFleet(t, 0, withHooks(acquireScript, releaseScript, linger))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Every worker declaring an on_release hook runs it once, unconditionally,
	// before it ever polls for work (README: "At startup ... the worker runs
	// the on_release hook of every device that declares one") — this worker
	// is no exception, and that startup pass writes to releaseLog before the
	// hold in this test ever exists. Wait for that one-time line and then
	// clear it, so every count from here on is about THIS test's own
	// sequence, not the harness's unrelated startup reconciliation.
	require.Eventually(t, func() bool {
		return countLines(t, releaseLog) == 1
	}, 30*time.Second, 100*time.Millisecond, "the startup release-hook reconciliation pass never ran")
	require.NoError(t, os.Remove(releaseLog))
	requireAbsent(t, acquireLog, "on_acquire must not run at startup, only on_release")

	hold, err := cl.Hold(ctx, client.HoldOptions{
		DeviceID: device, Submitter: "human-a", Reason: "manual profiling", TTL: 60 * time.Second,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		j, err := cl.Job(ctx, hold.ID)
		return err == nil && j.State == model.JobRunning
	}, 20*time.Second, 100*time.Millisecond, "the hold was never granted")

	// The device view must show a hold occupying it, with the reason it was
	// taken for — the same "holder (reason)" shape rc devices and the
	// dashboard render.
	state, err := cl.State(ctx)
	require.NoError(t, err)
	require.Len(t, state.Devices, 1)
	require.Equal(t, "human-a", state.Devices[0].Holder)
	require.Equal(t, model.LeaseKindHold, state.Devices[0].Kind)
	require.Equal(t, "manual profiling", state.Devices[0].Reason)

	// A job submitted while the hold is live queues behind it exactly like
	// it would behind any other job holding the device.
	queuedJob, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-b",
		Command: []string{"sh", "-c", "echo ran-after-hold"},
	})
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, queuedJob.State,
		"a job submitted against a device held (not just leased to a job) must queue")

	// Exactly one acquire so far: the hold's own, before it was granted. The
	// queued job has not run yet, so nothing else could have fired one.
	require.Equal(t, 1, countLines(t, acquireLog))
	requireAbsent(t, releaseLog, "nothing has released the device yet")

	require.NoError(t, cl.Release(ctx, hold.ID, "human-a"))

	require.Eventually(t, func() bool {
		j, err := cl.Job(ctx, queuedJob.ID)
		return err == nil && j.State == model.JobSucceeded
	}, 30*time.Second, 250*time.Millisecond, "the job queued behind the hold never ran once it was released")

	var out logBuffer
	require.NoError(t, cl.StreamLogs(ctx, queuedJob.ID, &out))
	require.Contains(t, out.String(), "ran-after-hold")

	// The queued job landed on the freed device well inside the release
	// linger (see the constant's comment above), so the pending release was
	// cancelled and the queued job's own on_acquire was skipped — one
	// acquire for the whole burst, exactly as the README's "lease lifecycle
	// hooks" section documents for back-to-back jobs on one device.
	require.Equal(t, 1, countLines(t, acquireLog),
		"the job queued behind the hold must not have fired its own on_acquire")

	// Now that the device has genuinely gone idle (nothing landed on it
	// after the queued job finished), the pending release fires once the
	// linger elapses.
	require.Eventually(t, func() bool {
		return countLines(t, releaseLog) == 1
	}, 30*time.Second, 250*time.Millisecond, "the release hook never fired")

	// Give a wrongly-repeated hook a real chance to show up before declaring
	// the count final: comfortably longer than the linger itself, so a bug
	// re-arming or re-firing the release would very likely have appeared.
	time.Sleep(3 * linger)
	require.Equal(t, 1, countLines(t, acquireLog),
		"acquire fired more than once across the whole hold-then-job sequence")
	require.Equal(t, 1, countLines(t, releaseLog),
		"release fired more than once across the whole hold-then-job sequence")
}

// countLines returns how many non-empty lines a hook's marker file holds, or
// 0 if the file does not exist yet — a hook that hasn't fired and a hook
// file with nothing in it read the same way here.
func countLines(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	n := 0
	for _, line := range bytes.Split(b, []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			n++
		}
	}
	return n
}

// requireAbsent asserts a hook's marker file has not been written to yet.
func requireAbsent(t *testing.T, path, msg string) {
	t.Helper()
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "%s (marker file %s unexpectedly exists)", msg, path)
}
