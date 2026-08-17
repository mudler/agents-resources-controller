package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/stretchr/testify/require"
)

// The round trip, through a real controller, a real worker, a real lease and
// a real `tar` on both ends: a script goes up, a job runs it, and its output
// comes back. Everything that could be faked separately is here at once,
// because the thing worth proving is that the pieces agree — the relay, the
// pipe stdio mode, the lease, and the archive.
func TestCpRoundTripThroughARealFleet(t *testing.T) {
	c, _, deviceID := newFleet(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	local, box := t.TempDir(), t.TempDir()

	// Executable, because a script that lands without its bit is the way
	// this feature disappoints, and here it is proven by RUNNING it.
	script := filepath.Join(local, "measure.sh")
	require.NoError(t, os.WriteFile(script,
		[]byte("#!/bin/sh\necho measured-on-the-box > \"$1\"\n"), 0o755))

	require.NoError(t, c.CopyTo(ctx, client.CopyOptions{
		DeviceID: deviceID, Local: script, Remote: box + "/", Submitter: "agent-a",
	}))

	onBox := filepath.Join(box, "measure.sh")
	fi, err := os.Stat(onBox)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), fi.Mode().Perm(),
		"the copied script arrived without its executable bit, so the job below could not run it")

	// A job runs the copied script — invoking it directly, so the executable
	// bit is load-bearing rather than decorative.
	result := filepath.Join(box, "result.txt")
	job, err := c.Submit(ctx, client.SubmitOptions{
		DeviceID: deviceID, Command: []string{onBox, result}, Submitter: "agent-a",
	})
	require.NoError(t, err)
	final, err := c.WaitTerminal(ctx, job.ID, 60*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, final.State)

	// And back off the box.
	require.NoError(t, c.CopyFrom(ctx, client.CopyOptions{
		DeviceID: deviceID, Remote: result, Local: local + "/", Submitter: "agent-a",
	}))
	got, err := os.ReadFile(filepath.Join(local, "result.txt"))
	require.NoError(t, err)
	require.Equal(t, "measured-on-the-box\n", string(got))
}

// A copy runs under a lease like any other execution, so a box somebody else
// is on is not yours to write to. Proven against a real hold rather than a
// stubbed device view: the refusal has to come from what the fleet actually
// reports, and it has to name the holder — an operator who is told only "no"
// cannot tell whether to wait or to go and ask.
func TestCpToADeviceHeldBySomeoneElseIsRefused(t *testing.T) {
	c, _, deviceID := newFleet(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	hold, err := c.Hold(ctx, client.HoldOptions{
		DeviceID: deviceID, Submitter: "agent-b", Reason: "profiling", TTL: 5 * time.Minute,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		relCtx, relCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer relCancel()
		_ = c.Release(relCtx, hold.ID, "agent-b")
	})

	require.Eventually(t, func() bool {
		state, err := c.State(ctx)
		return err == nil && len(state.Devices) == 1 && state.Devices[0].Holder == "agent-b"
	}, 30*time.Second, 100*time.Millisecond, "the hold never took the device")

	local, box := t.TempDir(), t.TempDir()
	src := filepath.Join(local, "theirs.txt")
	require.NoError(t, os.WriteFile(src, []byte("not yours"), 0o644))

	err = c.CopyTo(ctx, client.CopyOptions{
		DeviceID: deviceID, Local: src, Remote: box + "/", Submitter: "agent-a",
	})
	require.ErrorIs(t, err, client.ErrDeviceHeldByAnother)
	require.Contains(t, err.Error(), "agent-b")

	_, statErr := os.Stat(filepath.Join(box, "theirs.txt"))
	require.Error(t, statErr, "a refused copy still wrote to the box")
}
