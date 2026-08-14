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

// TestNewAppliesHookDefaults guards the fix for a Config that never passed
// through LoadConfig (an embedded worker, a test, any future flag path):
// a zero hook timeout means no MaxRuntime, so a wedged hook runs forever
// and the startup pass blocks Start from ever polling. "Unset" must resolve
// to the documented default wherever the config arrived from.
func TestNewAppliesHookDefaults(t *testing.T) {
	w := New(Config{
		Host:    "gpubox",
		Devices: []DeviceConfig{{Name: "gpu0", OnAcquire: "/bin/true", OnRelease: "/bin/true"}},
	})

	spec := w.hooks["gpubox:gpu0"]
	require.Equal(t, defaultHookTimeout, spec.timeout,
		"a zero hook timeout must never reach the hook runner: it would mean no MaxRuntime at all")
	require.Equal(t, defaultReleaseLinger, spec.releaseLinger)
	require.Equal(t, defaultHeartbeatInterval, w.cfg.HeartbeatInterval)
	require.Equal(t, defaultPollWait, w.cfg.PollWait)
}

// TestHostLevelHookDefaultsReachDevicesBuiltInCode is the other half: a
// host-level hooks: block set in code (not YAML) must still be what an
// un-overridden device inherits.
func TestHostLevelHookDefaultsReachDevicesBuiltInCode(t *testing.T) {
	w := New(Config{
		Host:    "gpubox",
		Hooks:   HooksConfig{Timeout: 7 * time.Second, ReleaseLinger: 3 * time.Second},
		Devices: []DeviceConfig{{Name: "gpu0", OnRelease: "/bin/true"}},
	})
	spec := w.hooks["gpubox:gpu0"]
	require.Equal(t, 7*time.Second, spec.timeout)
	require.Equal(t, 3*time.Second, spec.releaseLinger)
}

// TestStartupReleasePassIsBounded guards the fix for a startup pass that
// could delay (or, with a zero timeout, permanently prevent) this worker
// ever polling: the pass as a whole is capped, so one slow hook costs a
// known amount of startup delay and the remaining devices are simply left
// to the next start's pass — which is safe precisely because release hooks
// are required to be idempotent.
func TestStartupReleasePassIsBounded(t *testing.T) {
	orig := startupReleaseBudget
	startupReleaseBudget = 300 * time.Millisecond
	t.Cleanup(func() { startupReleaseBudget = orig })

	slow := writeScript(t, `sleep 30`)
	w := New(Config{
		Host: "gpubox",
		Devices: []DeviceConfig{
			{Name: "gpu0", OnRelease: slow},
			{Name: "gpu1", OnRelease: slow},
		},
	})

	start := time.Now()
	w.runStartupReleaseHooks(context.Background())
	elapsed := time.Since(start)

	require.Less(t, elapsed, 5*time.Second,
		"the startup release pass must be bounded as a whole, not by each hook's own timeout summed over every device")
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
	w.scheduleRelease(device, spec, "job1", "agent-a", acq1.epoch)

	// Second job arrives well within the linger window: its acquire must be
	// skipped (the device was never released), and the pending release
	// timer from job1 must be cancelled.
	time.Sleep(50 * time.Millisecond)
	acq2 := w.acquireDevice(context.Background(), device, spec, "job2", "agent-a")
	require.NoError(t, acq2.err)
	require.False(t, acq2.ranHook, "the acquire hook must not run again while the device is still held")
	w.scheduleRelease(device, spec, "job2", "agent-a", acq2.epoch)

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

// TestDelayedReleaseFromAFinishedJobNeverFiresDuringTheNextJob drives the
// inversion directly rather than hoping for a race: job1 ends, but its
// scheduleRelease is delayed (in production: its terminal report's response
// was lost and reportTerminalWithRetry burned its retry budget while the
// controller had already freed the device and handed job2 out). By the time
// job1 gets to arm its release, job2 owns the device. Nothing may fire for
// job1 — a release landing here restarts the operator's inference server
// onto the VRAM of a running job — and job2's own end must still release
// normally.
func TestDelayedReleaseFromAFinishedJobNeverFiresDuringTheNextJob(t *testing.T) {
	spec, _, releaseLog := testSpec(t, 5*time.Second, 100*time.Millisecond)
	w := New(Config{})
	const device = "gpubox:gpu0"

	acq1 := w.acquireDevice(context.Background(), device, spec, "job1", "agent-a")
	require.NoError(t, acq1.err)
	require.True(t, acq1.ranHook)

	// job1 has ended, but its goroutine has not reached scheduleRelease yet.
	// The controller, having freed the device, hands job2 out and job2
	// acquires: the device is still held, so no acquire hook runs, but this
	// acquire is what makes job2 — not job1 — the device's current owner.
	acq2 := w.acquireDevice(context.Background(), device, spec, "job2", "agent-a")
	require.NoError(t, acq2.err)
	require.False(t, acq2.ranHook, "the device was never released, so job2 must not re-run the acquire hook")

	// Only now does job1's delayed goroutine arm its release. It is void:
	// a later acquire owns the device.
	w.scheduleRelease(device, spec, "job1", "agent-a", acq1.epoch)

	// Well past the linger: job2 is still running, so nothing may have fired.
	time.Sleep(400 * time.Millisecond)
	require.Empty(t, readFileOrEmpty(t, releaseLog),
		"a finished job's release must never fire while a later job holds the device")

	// The surviving owner still releases normally once it ends.
	w.scheduleRelease(device, spec, "job2", "agent-a", acq2.epoch)
	require.Eventually(t, func() bool {
		return readFileOrEmpty(t, releaseLog) != ""
	}, 2*time.Second, 10*time.Millisecond, "job2's own end must still release the device")
	require.Equal(t, "release job2\n", readFileOrEmpty(t, releaseLog))
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
	w.scheduleRelease(device, spec, "job1", "agent-a", acq1.epoch)

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
	w.scheduleRelease(device, spec, "job1", "agent-a", acq.epoch)
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
	w.scheduleRelease("gpubox:gpu0", hookSpec{}, "job1", "agent-a", acq.epoch)

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

	w.scheduleRelease(device, spec, "job1", "agent-a", acq.epoch)
	require.Eventually(t, func() bool {
		return readFileOrEmpty(t, releaseLog) != ""
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, "release job1\n", readFileOrEmpty(t, releaseLog))
}
