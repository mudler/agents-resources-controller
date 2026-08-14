package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeScript writes an executable shell script to a temp dir and returns
// its path.
func writeScript(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "hook.sh")
	require.NoError(t, os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o700))
	return p
}

func TestRunHookSucceeds(t *testing.T) {
	w := New(Config{})
	script := writeScript(t, `echo did-run; exit 0`)

	out, err := w.runHook(context.Background(), script, "acquire", "gpubox:gpu0", "job1", "agent-a", 5*time.Second)
	require.NoError(t, err)
	require.Contains(t, out, "did-run")
}

func TestRunHookNonZeroExitIsFailure(t *testing.T) {
	w := New(Config{})
	script := writeScript(t, `echo boom 1>&2; exit 3`)

	out, err := w.runHook(context.Background(), script, "acquire", "gpubox:gpu0", "job1", "agent-a", 5*time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exited 3")
	require.Contains(t, out, "boom")
}

func TestRunHookTimeoutIsFailure(t *testing.T) {
	w := New(Config{})
	script := writeScript(t, `sleep 30`)

	start := time.Now()
	_, err := w.runHook(context.Background(), script, "acquire", "gpubox:gpu0", "job1", "agent-a", 200*time.Millisecond)
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second,
		"a hook that exceeds its timeout must be killed like any job's process group, not left to run")
}

// TestRunHookEnvironment guards the exact environment contract a hook
// receives: RC_EVENT, RC_DEVICE, RC_JOB_ID, RC_SUBMITTER, and
// CUDA_VISIBLE_DEVICES derived the same way job execution derives it.
func TestRunHookEnvironment(t *testing.T) {
	script := writeScript(t, `
echo "RC_EVENT=$RC_EVENT"
echo "RC_DEVICE=$RC_DEVICE"
echo "RC_JOB_ID=$RC_JOB_ID"
echo "RC_SUBMITTER=$RC_SUBMITTER"
echo "CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES"
`)
	w := New(Config{})
	out, err := w.runHook(context.Background(), script, "release", "gpubox:gpu1", "job42", "agent-x", 5*time.Second)
	require.NoError(t, err)
	require.Contains(t, out, "RC_EVENT=release")
	require.Contains(t, out, "RC_DEVICE=gpubox:gpu1")
	require.Contains(t, out, "RC_JOB_ID=job42")
	require.Contains(t, out, "RC_SUBMITTER=agent-x")
	require.Contains(t, out, "CUDA_VISIBLE_DEVICES=1")
}

// TestRunHookEnvironmentEmptyForStartup guards the startup-reconciliation
// contract: RC_JOB_ID/RC_SUBMITTER are empty when nothing triggered the
// hook.
func TestRunHookEnvironmentEmptyForStartup(t *testing.T) {
	script := writeScript(t, `echo "JOB=[$RC_JOB_ID] SUB=[$RC_SUBMITTER]"`)
	w := New(Config{})
	out, err := w.runHook(context.Background(), script, "release", "gpubox:gpu0", "", "", 5*time.Second)
	require.NoError(t, err)
	require.Contains(t, out, "JOB=[] SUB=[]")
}

func testSpec(t *testing.T, timeout, linger time.Duration) (hookSpec, string, string) {
	t.Helper()
	dir := t.TempDir()
	acquireLog := filepath.Join(dir, "acquire.log")
	releaseLog := filepath.Join(dir, "release.log")
	acquire := writeScript(t, fmt.Sprintf(`echo "acquire $RC_JOB_ID" >> %s`, acquireLog))
	release := writeScript(t, fmt.Sprintf(`echo "release $RC_JOB_ID" >> %s`, releaseLog))
	return hookSpec{onAcquire: acquire, onRelease: release, timeout: timeout, releaseLinger: linger}, acquireLog, releaseLog
}

func readFileOrEmpty(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return ""
	}
	require.NoError(t, err)
	return string(b)
}

// TestBackToBackJobsWithinLingerProduceOneAcquireOneRelease is the property
// the linger exists for: two jobs run back-to-back, well inside the linger
// window between them, and across the whole burst the device sees exactly
// one acquire and one release — not one pair per job.
func TestBackToBackJobsWithinLingerProduceOneAcquireOneRelease(t *testing.T) {
	spec, acquireLog, releaseLog := testSpec(t, 5*time.Second, 300*time.Millisecond)
	w := New(Config{})
	const device = "gpubox:gpu0"

	acq1 := w.acquireDevice(context.Background(), device, spec, "job1", "agent-a")
	require.NoError(t, acq1.err)
	require.True(t, acq1.ranHook)
	w.scheduleRelease(device, spec, "job1", "agent-a")

	// Second job arrives well within the linger window: its acquire must be
	// skipped (the device was never released), and the pending release
	// timer from job1 must be cancelled.
	time.Sleep(50 * time.Millisecond)
	acq2 := w.acquireDevice(context.Background(), device, spec, "job2", "agent-a")
	require.NoError(t, acq2.err)
	require.False(t, acq2.ranHook, "the acquire hook must not run again while the device is still held")
	w.scheduleRelease(device, spec, "job2", "agent-a")

	require.Equal(t, "acquire job1\n", readFileOrEmpty(t, acquireLog),
		"exactly one acquire across the whole back-to-back burst")

	// Nothing must have released yet: job2's linger has not elapsed.
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, readFileOrEmpty(t, releaseLog), "must not release while jobs keep arriving inside the linger window")

	// Now let job2's linger actually elapse.
	require.Eventually(t, func() bool {
		return readFileOrEmpty(t, releaseLog) != ""
	}, 2*time.Second, 20*time.Millisecond)

	require.Equal(t, "release job2\n", readFileOrEmpty(t, releaseLog),
		"exactly one release, naming the job that actually triggered it (the last one)")
}

// TestJobAfterLingerElapsedGetsItsOwnAcquire is the other half of the
// property: once the linger has genuinely elapsed and the release hook has
// run, a later job on the same device gets a fresh acquire.
func TestJobAfterLingerElapsedGetsItsOwnAcquire(t *testing.T) {
	spec, acquireLog, releaseLog := testSpec(t, 5*time.Second, 100*time.Millisecond)
	w := New(Config{})
	const device = "gpubox:gpu0"

	acq1 := w.acquireDevice(context.Background(), device, spec, "job1", "agent-a")
	require.NoError(t, acq1.err)
	w.scheduleRelease(device, spec, "job1", "agent-a")

	require.Eventually(t, func() bool {
		return readFileOrEmpty(t, releaseLog) != ""
	}, 2*time.Second, 10*time.Millisecond, "release must fire once the linger elapses with nothing else arriving")

	acq2 := w.acquireDevice(context.Background(), device, spec, "job2", "agent-a")
	require.NoError(t, acq2.err)
	require.True(t, acq2.ranHook, "a job arriving after the device was genuinely released must get its own acquire")

	require.Equal(t, "acquire job1\nacquire job2\n", readFileOrEmpty(t, acquireLog))
}

// TestAcquireHookFailureLeavesDeviceUnheld guards that a failed acquire
// never marks the device held — there would be nothing to correctly
// release, since the operator's service was never actually stopped.
func TestAcquireHookFailureLeavesDeviceUnheld(t *testing.T) {
	dir := t.TempDir()
	acquireLog := filepath.Join(dir, "acquire.log")
	fail := writeScript(t, fmt.Sprintf(`echo "acquire $RC_JOB_ID" >> %s; exit 1`, acquireLog))
	spec := hookSpec{onAcquire: fail, timeout: 2 * time.Second, releaseLinger: 50 * time.Millisecond}
	w := New(Config{})
	const device = "gpubox:gpu0"

	acq := w.acquireDevice(context.Background(), device, spec, "job1", "agent-a")
	require.Error(t, acq.err)

	// scheduleRelease must be a no-op: nothing was ever actually acquired.
	w.scheduleRelease(device, spec, "job1", "agent-a")
	st := w.hookState(device)
	st.mu.Lock()
	held := st.held
	timer := st.timer
	st.mu.Unlock()
	require.False(t, held)
	require.Nil(t, timer)

	// A second attempt on the same device must try the acquire hook again,
	// not treat it as already (wrongly) held.
	acq2 := w.acquireDevice(context.Background(), device, spec, "job2", "agent-a")
	require.Error(t, acq2.err)
	require.True(t, acq2.ranHook)
	require.Equal(t, "acquire job1\nacquire job2\n", readFileOrEmpty(t, acquireLog))
}

// TestDeviceWithNoHooksConfiguredIsANoOp guards that a device with neither
// on_acquire nor on_release costs nothing: acquireDevice/scheduleRelease
// must not even allocate hook state for it.
func TestDeviceWithNoHooksConfiguredIsANoOp(t *testing.T) {
	w := New(Config{})
	acq := w.acquireDevice(context.Background(), "gpubox:gpu0", hookSpec{}, "job1", "agent-a")
	require.NoError(t, acq.err)
	require.False(t, acq.ranHook)
	w.scheduleRelease("gpubox:gpu0", hookSpec{}, "job1", "agent-a")

	w.hooksMu.Lock()
	_, exists := w.deviceHooks["gpubox:gpu0"]
	w.hooksMu.Unlock()
	require.False(t, exists, "a device with no hooks configured should never get a deviceHookState")
}

// TestOnlyReleaseHookConfigured guards the "either hook alone" case: a
// device with only on_release still tracks held state (nothing to run at
// acquire time) so the release still fires once, after the linger.
func TestOnlyReleaseHookConfigured(t *testing.T) {
	dir := t.TempDir()
	releaseLog := filepath.Join(dir, "release.log")
	release := writeScript(t, fmt.Sprintf(`echo "release $RC_JOB_ID" >> %s`, releaseLog))
	spec := hookSpec{onRelease: release, timeout: 2 * time.Second, releaseLinger: 50 * time.Millisecond}
	w := New(Config{})
	const device = "gpubox:gpu0"

	acq := w.acquireDevice(context.Background(), device, spec, "job1", "agent-a")
	require.NoError(t, acq.err)
	require.False(t, acq.ranHook, "no on_acquire script to run")

	w.scheduleRelease(device, spec, "job1", "agent-a")
	require.Eventually(t, func() bool {
		return readFileOrEmpty(t, releaseLog) != ""
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, "release job1\n", readFileOrEmpty(t, releaseLog))
}
