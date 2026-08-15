package worker_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

func writeVerify(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755))
}

func newVerifyWorker(t *testing.T, dir string) *worker.Worker {
	t.Helper()
	return worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:       []worker.DeviceConfig{{Name: "gpu0"}},
		VerifyDir:     dir,
		VerifyTimeout: 2 * time.Second,
	})
}

func TestVerifyPassesWhenEveryScriptSucceeds(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-ok.sh", `exit 0`)
	writeVerify(t, dir, "20-ok.sh", `exit 0`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.True(t, res.OK)
	require.Empty(t, res.Reason)
}

func TestVerifyFailsAndCarriesTheScriptsStderr(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-vram.sh", `echo "72G still allocated" >&2; exit 1`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.False(t, res.OK)
	require.Contains(t, res.Reason, "72G still allocated",
		"the operator needs to know WHY the device was quarantined")
	require.Contains(t, res.Reason, "10-vram.sh", "and which script said so")
}

// A later script must still run after an earlier one passes, and one failure
// is enough to fail the whole pass.
func TestVerifyRunsEveryScriptAndFailsIfAnyFails(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")
	writeVerify(t, dir, "10-bad.sh", `exit 1`)
	writeVerify(t, dir, "20-good.sh", `touch `+marker+`; exit 0`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.False(t, res.OK)
	require.FileExists(t, marker, "a failing script must not stop the rest of the pass")
}

func TestVerifyTimesOutAsAFailure(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-hang.sh", `sleep 60`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:       []worker.DeviceConfig{{Name: "gpu0"}},
		VerifyDir:     dir,
		VerifyTimeout: 300 * time.Millisecond,
	})

	start := time.Now()
	res := w.RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.False(t, res.OK)
	require.Less(t, time.Since(start), 10*time.Second, "a hanging script must not stall the pass")
	require.Contains(t, res.Reason, "10-hang.sh")
}

// The feature is off unless a host ships scripts.
func TestVerifyPassesWhenThereAreNoScripts(t *testing.T) {
	res := newVerifyWorker(t, t.TempDir()).RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.True(t, res.OK)
}

func TestVerifyPassesWhenTheDirectoryDoesNotExist(t *testing.T) {
	res := newVerifyWorker(t, filepath.Join(t.TempDir(), "nope")).
		RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.True(t, res.OK)
}

func TestVerifyScriptSeesTheDeviceAndJob(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-env.sh", `[ "$RC_DEVICE" = "box:gpu0" ] && [ "$RC_JOB_ID" = "job1" ] || exit 1`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.True(t, res.OK, "the script did not receive RC_DEVICE and RC_JOB_ID")
}
