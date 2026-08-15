// Package e2e_test's verify test covers what stage 4 added on the worker
// side: a drop-in verify script runs after a job's process tree is gone and
// BEFORE the terminal report that would otherwise hand the device to the next
// job, so a device the job left dirty is quarantined instead of returning to
// the pool.
package e2e_test

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/stretchr/testify/require"
)

// verifyStall is how long the failing verify script below sleeps before it
// fails, as an `sh` sleep argument. It is the margin the ordering assertion
// rests on: comfortably longer than the client's 500ms terminal-state poll,
// short enough that the test does not pay for it twice (the passing pass does
// not stall at all).
const verifyStall = "3"

// TestVerifyFailureQuarantinesTheDeviceThenAPassingScriptReturnsIt runs one
// real job under a verify script that fails, and proves the three things the
// feature promises that no unit test can show together:
//
//  1. the JOB is unaffected — it reaches its own ordinary terminal state with
//     its own exit code. A dirty device is the device's problem, not a reason
//     to relabel a job that ran perfectly well;
//  2. the DEVICE ends `unhealthy`, not `ready` — this is the ordering the
//     whole feature exists for. Getting it backwards (report first, verify
//     after) puts a dirty device back in the pool for as long as the fault
//     POST takes, which is exactly the OOM the check is meant to prevent;
//  3. a later submit for that device is REFUSED rather than queued behind a
//     device that will never come back on its own.
//
// Then the script is made to pass, an admin clears the quarantine over the
// wire, and a second job runs and leaves the device `ready` as usual — so the
// test also shows the feature is not a one-way door.
func TestVerifyFailureQuarantinesTheDeviceThenAPassingScriptReturnsIt(t *testing.T) {
	dir := t.TempDir()
	verifyDir := filepath.Join(dir, "verify.d")
	require.NoError(t, os.MkdirAll(verifyDir, 0o700))

	// One script, whose verdict is controlled by a marker file the test
	// creates and removes: a failing pass and a passing pass in one run,
	// without restarting the worker (which would re-register and reconcile,
	// muddying what the device's state is actually proving).
	//
	// It writes its complaint to stderr and exits non-zero — the documented
	// contract: the exit code is the result, stderr is the reason.
	//
	// The failing path deliberately takes verifyStall to run. That is what
	// makes the ORDERING assertion below decidable rather than a race: the
	// verify pass sits between the job's process exiting and its terminal
	// report, so a controller that has seen the job go terminal has, in the
	// correct order, necessarily already seen the fault. Reverse the two
	// (report first, verify after) and the device is ready — for a whole
	// verifyStall, far longer than the ~500ms client poll that observes the
	// terminal state — which is precisely the window in which the scheduler
	// would hand a dirty GPU to the next job.
	dirty := filepath.Join(dir, "dirty")
	script := filepath.Join(verifyDir, "10-vram-free.sh")
	require.NoError(t, os.WriteFile(script, []byte(
		"#!/bin/sh\n"+
			"if [ -f "+dirty+" ]; then\n"+
			"  sleep "+verifyStall+"\n"+
			"  echo \"72G still allocated on $RC_DEVICE after job $RC_JOB_ID\" >&2\n"+
			"  exit 1\n"+
			"fi\n"+
			"exit 0\n"), 0o700))
	require.NoError(t, os.WriteFile(dirty, nil, 0o600))

	var controllerURL string
	cl, _, device := newFleet(t, 0, withVerifyDir(verifyDir), withControllerURL(&controllerURL))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	job, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-a",
		Command: []string{"sh", "-c", "echo job-ran; exit 0"},
	})
	require.NoError(t, err)

	// (1) The job's own outcome is untouched by the verify failure.
	final, err := cl.WaitTerminal(ctx, job.ID, waitTerminalBound)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, final.State,
		"a verify failure must not relabel the job; it says something about the device, not the run")
	require.NotNil(t, final.ExitCode)
	require.Equal(t, 0, *final.ExitCode)

	var out logBuffer
	require.NoError(t, cl.StreamLogs(ctx, job.ID, &out))
	require.Contains(t, out.String(), "job-ran")

	// (2) The device is ALREADY quarantined the moment the job is terminal —
	// asserted directly, with no Eventually, because that is the ordering
	// claim. An Eventually here would pass just as happily against a worker
	// that reported the job first and quarantined the device a few seconds
	// later, which is the bug this feature exists to prevent; see the verify
	// script's own stall above for why this is a margin and not a race.
	state, err := cl.State(ctx)
	require.NoError(t, err)
	require.Len(t, state.Devices, 1)
	require.Equal(t, model.DeviceUnhealthy, state.Devices[0].Device.State,
		"the device must already be quarantined when the job's terminal report lands, "+
			"never briefly ready with a dirty GPU in the pool")
	require.Empty(t, state.Devices[0].Holder, "the finished job must not still hold the device")

	// (3) Nothing can claim it. NoWait, because a quarantined device is never
	// free: even opting out of the queue must fail rather than park a job
	// behind a device that will not become schedulable on its own.
	_, err = cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Command: []string{"true"}, Submitter: "agent-b", NoWait: true,
	})
	require.ErrorIs(t, err, client.ErrNoDevice,
		"a device quarantined by a failed verify script must refuse the next job")

	// The device is clean again, and the operator says so: same
	// POST /v1/devices/{id}/clear an admin runs by hand (the dashboard's
	// `clear` control is this exact call). A `fault` quarantine — which is
	// what a verify failure records — is deliberately not one a reboot
	// clears, so this is the only way back.
	clearDevice(t, controllerURL, device)
	require.NoError(t, os.Remove(dirty))

	again, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-b",
		Command: []string{"sh", "-c", "echo second-job-ran"},
	})
	require.NoError(t, err, "a cleared device must accept work again")

	final2, err := cl.WaitTerminal(ctx, again.ID, waitTerminalBound)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, final2.State)

	// With the script now passing, the device returns to the pool the
	// ordinary way — the verify pass is a gate, not a trapdoor.
	require.Eventually(t, func() bool {
		state, err := cl.State(ctx)
		return err == nil && len(state.Devices) == 1 &&
			state.Devices[0].Device.State == model.DeviceReady
	}, 30*time.Second, 100*time.Millisecond,
		"the device never returned to ready after a passing verify pass")
}

// clearDevice runs the admin-only POST /v1/devices/{id}/clear against the
// controller newFleet stood up, with the harness's admin token. The client
// package wraps no such call (there is no `rc clear`; the README documents
// the raw route), so this posts it directly, the same way registerRestart
// posts a raw registration.
func clearDevice(t *testing.T, baseURL, deviceID string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost,
		baseURL+"/v1/devices/"+deviceID+"/clear", bytes.NewReader(nil))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer atok")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"clearing %s did not succeed", deviceID)
}
