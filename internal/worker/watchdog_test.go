package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestMaxRuntimeWatchdogKillsALongJob(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command:      []string{"sh", "-c", "sleep 60"},
		MaxRuntime:   500 * time.Millisecond,
		GraceCeiling: 500 * time.Millisecond,
	}, &out)

	require.True(t, res.Killed)
	require.Contains(t, res.Reason, "max_runtime")
}

func TestIdleWatchdogKillsASilentJob(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command:      []string{"sh", "-c", "echo starting; sleep 60"},
		IdleTimeout:  700 * time.Millisecond,
		GraceCeiling: 500 * time.Millisecond,
	}, &out)

	require.True(t, res.Killed)
	require.Contains(t, res.Reason, "idle")
	require.Contains(t, out.String(), "starting")
}

// A job that keeps producing output must never trip the idle watchdog.
func TestChattyJobIsNotTrippedByIdleWatchdog(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command:      []string{"sh", "-c", "for i in 1 2 3 4 5 6; do echo tick; sleep 0.2; done"},
		IdleTimeout:  700 * time.Millisecond,
		GraceCeiling: 500 * time.Millisecond,
	}, &out)

	require.False(t, res.Killed, "a job producing output steadily must not be killed")
	require.Equal(t, 0, res.ExitCode)
}

func TestNoWatchdogsMeansNoLimit(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command: []string{"sh", "-c", "sleep 0.3; echo done"},
	}, &out)

	require.False(t, res.Killed)
	require.Equal(t, 0, res.ExitCode)
	require.Contains(t, out.String(), "done")
}
