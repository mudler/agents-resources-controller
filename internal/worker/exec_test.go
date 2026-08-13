package worker_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"syscall"
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

	childPID := waitForChildPID(t, &out)

	cancel()

	res := <-done
	require.True(t, res.Killed)
	require.Equal(t, "cancelled", res.Reason)

	require.Eventually(t, func() bool {
		return !processAlive(t, childPID)
	}, 5*time.Second, 50*time.Millisecond, "grandchild %d survived the kill", childPID)
}

// The scenario the round-1 review measured directly: a grandchild that
// ignores SIGTERM and has detached from the inherited stdio (so it can't
// accidentally self-correct by holding the pipe open) must still die when
// the group leader itself exits promptly on SIGTERM. Reaping the leader is
// not the same as the group being empty.
func TestCancelKillsSurvivorsThatIgnoreSIGTERM(t *testing.T) {
	var out syncBuf
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan worker.Result, 1)
	go func() {
		done <- worker.Run(ctx, worker.JobSpec{
			// The backgrounded subshell ignores TERM and execs into sleep,
			// which inherits the ignore disposition; its stdio is
			// redirected away so it can't hold sink's pipe open. The
			// launcher has no trap of its own, so SIGTERM reaps it
			// immediately, while the grandchild lives on.
			Command:      []string{"sh", "-c", "(trap '' TERM; exec sleep 300 >/dev/null 2>&1) & echo child:$!; sleep 300"},
			GraceCeiling: 300 * time.Millisecond,
		}, &out)
	}()

	childPID := waitForChildPID(t, &out)

	cancel()

	res := <-done
	require.True(t, res.Killed)

	require.Eventually(t, func() bool {
		return !processAlive(t, childPID)
	}, 5*time.Second, 50*time.Millisecond, "SIGTERM-ignoring grandchild %d survived the kill", childPID)
}

// An OOM-killed trainer must not read as a normal exit: the whole point of
// this project is diagnosing GPU jobs, and "ExitCode 137, Killed=false" is
// indistinguishable from a job that intentionally called exit(137).
func TestSignalKilledProcessIsReportedAsKilled(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command: []string{"sh", "-c", "kill -9 $$"},
	}, &out)

	require.NoError(t, res.Err)
	require.True(t, res.Killed)
	require.Equal(t, 137, res.ExitCode)
	require.Equal(t, "signal: killed", res.Reason)
}

// Round-1 finding 3: select picks randomly between two simultaneously-ready
// cases, so a job that finishes in the same instant as cancellation could be
// mislabelled Killed=true. There is no way to force that exact coincidence
// deterministically from a black-box test — it depends on goroutine and
// syscall scheduling we don't control. What we can assert deterministically,
// on every run, win or lose the race: a job that genuinely ran to completion
// (exit code 0, no error) must never simultaneously be reported as killed.
// Looping with a near-instant command gives the race a real chance to occur
// on at least some iterations; the assertion holds regardless, so this
// cannot be flaky.
func TestCleanExitDuringCancellationIsNotMislabelled(t *testing.T) {
	const iterations = 300
	for i := 0; i < iterations; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		var out syncBuf
		done := make(chan worker.Result, 1)
		go func() {
			done <- worker.Run(ctx, worker.JobSpec{Command: []string{"true"}}, &out)
		}()
		cancel()
		res := <-done

		if res.Err == nil && res.ExitCode == 0 {
			require.Falsef(t, res.Killed,
				"iteration %d: a cleanly-exited job (code 0) was reported as killed (reason=%q)", i, res.Reason)
		}
	}
}

// waitForChildPID extracts a "child:<pid>" line the spawned shell printed
// to out, once it appears.
func waitForChildPID(t *testing.T, out *syncBuf) int {
	t.Helper()
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
	return childPID
}

// kill(pid, 0) probes for existence without signalling: ESRCH means the
// process is gone, EPERM means it exists but we can't signal it (still
// alive), and any other error is unexpected and fails the test loudly
// rather than being silently read as "dead". os.FindProcess is useless
// here: on Unix it never fails, and Process.Signal(nil) errors on the nil
// signal rather than probing, which would make this always report "dead"
// and the assertions above vacuous.
func processAlive(t *testing.T, pid int) bool {
	t.Helper()
	err := syscall.Kill(pid, 0)
	switch {
	case err == nil:
		return true
	case errors.Is(err, syscall.ESRCH):
		return false
	case errors.Is(err, syscall.EPERM):
		return true
	default:
		t.Fatalf("unexpected error probing pid %d: %v", pid, err)
		return false
	}
}
