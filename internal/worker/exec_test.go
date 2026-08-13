package worker_test

import (
	"bytes"
	"context"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestRunCapturesOutputAndExitCode(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command: []string{"sh", "-c", "echo out; echo err >&2; exit 3"},
	}, &out)

	require.NoError(t, res.Err)
	require.Equal(t, 3, res.ExitCode)
	require.False(t, res.Killed)
	require.Contains(t, out.String(), "out")
	require.Contains(t, out.String(), "err")
}

func TestRunSetsJobEnvironment(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command: []string{"sh", "-c", "echo $RC_DEVICE/$CUDA_VISIBLE_DEVICES"},
		Env:     map[string]string{"RC_DEVICE": "gpubox:gpu0", "CUDA_VISIBLE_DEVICES": "0"},
	}, &out)

	require.NoError(t, res.Err)
	require.Equal(t, 0, res.ExitCode)
	require.Contains(t, out.String(), "gpubox:gpu0/0")
}

func TestRunRunsInRequestedDirectory(t *testing.T) {
	dir := t.TempDir()
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command: []string{"pwd"}, Cwd: dir,
	}, &out)

	require.NoError(t, res.Err)
	// macOS reports /private/var for /var; compare the resolved suffix.
	require.Contains(t, strings.TrimSpace(out.String()), strings.TrimPrefix(dir, "/private"))
}

// The one that matters: cancelling must take down the whole process tree,
// not just the shell we spawned.
func TestCancelKillsTheEntireProcessTree(t *testing.T) {
	var out syncBuf
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan worker.Result, 1)
	go func() {
		done <- worker.Run(ctx, worker.JobSpec{
			// Print the grandchild's pid, then keep both alive.
			Command:      []string{"sh", "-c", "sleep 300 & echo child:$!; wait"},
			GraceCeiling: 500 * time.Millisecond,
		}, &out)
	}()

	var childPID int
	require.Eventually(t, func() bool {
		_, after, found := strings.Cut(out.String(), "child:")
		if !found {
			return false
		}
		pid, err := strconv.Atoi(strings.TrimSpace(strings.SplitN(after, "\n", 2)[0]))
		if err != nil {
			return false
		}
		childPID = pid
		return true
	}, 5*time.Second, 20*time.Millisecond)

	cancel()

	res := <-done
	require.True(t, res.Killed)
	require.Equal(t, "cancelled", res.Reason)

	require.Eventually(t, func() bool {
		return !processAlive(childPID)
	}, 5*time.Second, 50*time.Millisecond, "grandchild %d survived the kill", childPID)
}

// kill -0 probes for existence without signalling. os.FindProcess is useless
// here: on Unix it never fails, and Process.Signal(nil) errors on the nil
// signal rather than probing, which would make this always report "dead" and
// the assertion below vacuous.
func processAlive(pid int) bool {
	return exec.Command("kill", "-0", strconv.Itoa(pid)).Run() == nil
}
