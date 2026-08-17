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

// Round-2 finding: a single-process job that traps SIGTERM and exits 0 in
// response to cancellation must still be reported as cancelled, not as a
// successful run. Task 8 maps Killed onto the job's terminal state, so
// Killed=false, ExitCode=0 would record an operator-interrupted benchmark as
// a valid completed measurement — the one error that silently corrupts
// results instead of merely confusing someone.
func TestCancelledJobTrappingSIGTERMIsNotReportedSuccessful(t *testing.T) {
	var out syncBuf
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan worker.Result, 1)
	go func() {
		done <- worker.Run(ctx, worker.JobSpec{
			Command: []string{"sh", "-c", "trap 'exit 0' TERM; while true; do sleep 0.05; done"},
		}, &out)
	}()

	// Give the shell a moment to install its trap before cancelling, so the
	// SIGTERM is actually handled by it rather than killing an unstarted
	// process by default disposition.
	time.Sleep(100 * time.Millisecond)
	cancel()

	res := <-done
	require.NoError(t, res.Err)
	require.True(t, res.Killed)
	require.Equal(t, "cancelled", res.Reason)
	require.Equal(t, 0, res.ExitCode)
}

// Round-1 finding 3: select picks randomly between two simultaneously-ready
// cases, so a job that finishes in the same instant as cancellation could be
// mislabelled Killed=true. There is also a second, kernel-level version of
// the same coincidence that no userspace check can close: kill(2) reports
// success against a process that has already committed to exiting but not
// yet been reaped (a zombie), so a job that self-completes in the same
// nanosecond we signal it can still come back labelled cancelled. Neither
// version can be forced deterministically from a black-box test — both
// depend on goroutine/kernel scheduling outside this package's control —
// and "true" cancelled with no synchronization is exactly the coincident
// window that exercises them: it deliberately gives the race a real chance
// to land on either side on any given run.
//
// What must hold regardless of which way the race falls, on every run: the
// report has to stay internally consistent. A clean, non-killed result must
// carry the real exit code and no reason; a killed result must say why. The
// one outcome that is never acceptable is the one this whole task exists to
// prevent — a run that was actually cancelled coming back silently as a
// success (Killed=false, ExitCode=0, no indication anything was
// interrupted). A stronger assertion (pinning which side of the race must
// win) is not achievable: the kernel does not promise it.
func TestCancellationReportIsSelfConsistent(t *testing.T) {
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

		require.NoError(t, res.Err, "iteration %d", i)
		if res.Killed {
			require.Equalf(t, "cancelled", res.Reason,
				"iteration %d: killed run must say why", i)
		} else {
			require.Emptyf(t, res.Reason,
				"iteration %d: non-killed run must not carry a reason", i)
			require.Equalf(t, 0, res.ExitCode,
				"iteration %d: non-killed run must report the real (0) exit code", i)
		}
	}
}

// The straggler sweep added for the PTY's clean-exit path runs for EVERY job,
// piped ones included, and it must not turn an ordinary success into a
// cancellation. A process that has already exited but has not yet been reaped
// by its parent — a zombie — is still a member of its process group, and
// kill(-pgid, 0) answers "yes, something is there" for it exactly as it does
// for a live process. That is not a straggler: a zombie holds no memory, no
// device and no CPU, and there is nothing to kill.
//
// The shape below is completely routine (background something, let the shell
// exit) and it hits the window every time: the backgrounded child holds the
// inherited stdout pipe, so cmd.Wait() returns the instant that child exits —
// which is the instant it becomes a zombie owned by init, before init has had
// a chance to reap it. Measured at 40/40 mislabelled before the fix. The
// consequences are not cosmetic: worker.go maps Killed onto model.JobKilled,
// hooks.go treats a killed hook as a failed hook (so an on_acquire hook that
// backgrounds anything would refuse the lease), and verify.go treats a killed
// probe as a failed probe, which quarantines the device.
func TestCleanExitDoesNotMistakeAnUnreapedChildForAStraggler(t *testing.T) {
	// Repeated because the window is a race: one green pass would prove
	// nothing about whether it is reliably closed.
	for i := 0; i < 20; i++ {
		var out syncBuf
		res := worker.Run(context.Background(), worker.JobSpec{
			Command: []string{"sh", "-c", "sleep 0.05 & exit 0"},
		}, &out)

		require.NoErrorf(t, res.Err, "iteration %d", i)
		require.Equalf(t, 0, res.ExitCode, "iteration %d", i)
		require.Falsef(t, res.Killed,
			"iteration %d: a job whose backgrounded child had already exited was reported killed (%q)", i, res.Reason)
		require.Emptyf(t, res.Reason, "iteration %d", i)
	}
}

// The other half of the same rule, so the fix above cannot be "never report a
// straggler": a background job that is genuinely still running when the leader
// exits must still be reaped and still be reported. This is Run's equivalent
// of TestPTYCleanShellExitReapsBackgroundedStragglers, and it is the exact
// assertion a fix that simply stopped scanning would break.
func TestCleanExitStillReapsALiveStraggler(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		// The subshell detaches from the inherited stdio so it cannot hold
		// the pipe open — without that redirection cmd.Wait() would simply
		// block for the full 300s rather than returning to a live group.
		Command: []string{"sh", "-c", "(exec sleep 300 >/dev/null 2>&1) & echo child:$!; exit 0"},
	}, &out)

	childPID := waitForChildPID(t, &out)

	require.NoError(t, res.Err)
	require.True(t, res.Killed,
		"a clean exit that leaves a live background job must not be reported as an unremarkable success")
	require.Equal(t, "stragglers reaped after exit", res.Reason)

	require.Eventually(t, func() bool {
		return !processAlive(t, childPID)
	}, 5*time.Second, 50*time.Millisecond, "live straggler %d survived a clean exit", childPID)
}

// The round-2 PTY review found that spec.GraceCeiling's actual DURATION was
// unguarded: every other test's target process dies to SIGTERM immediately
// (default disposition), so the SIGTERM -> grace -> SIGKILL window's length
// is never on the critical path and a bug that silently substituted the
// hardcoded 10s default for spec.GraceCeiling would still leave the whole
// suite green. This pins the duration itself: a leader that traps SIGTERM
// (so the only way to end it is SIGKILL after grace elapses) with a short
// GraceCeiling must be killed close to that ceiling, not anywhere near the
// 10s default.
func TestGraceCeilingDurationIsHonored(t *testing.T) {
	var out syncBuf
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan worker.Result, 1)
	start := time.Now()
	go func() {
		done <- worker.Run(ctx, worker.JobSpec{
			Command:      []string{"sh", "-c", "trap '' TERM; while true; do sleep 0.05; done"},
			GraceCeiling: 200 * time.Millisecond,
		}, &out)
	}()

	// Give the shell a moment to install its trap before cancelling, so the
	// SIGTERM the cancellation below sends is actually ignored by it (as
	// intended) rather than killing an unstarted process by default
	// disposition — the same justification exec_test.go already uses in
	// TestCancelledJobTrappingSIGTERMIsNotReportedSuccessful.
	time.Sleep(100 * time.Millisecond)
	cancel()

	res := <-done
	elapsed := time.Since(start)

	require.True(t, res.Killed)
	require.Less(t, elapsed, 3*time.Second,
		"GraceCeiling=200ms does not appear to have been honored; a hardcoded 10s default would take far longer than this bound")
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
