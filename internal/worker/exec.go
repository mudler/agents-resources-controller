// Package worker runs jobs on a device host and reports back to the controller.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type JobSpec struct {
	Command      []string
	Cwd          string
	Env          map[string]string
	GraceCeiling time.Duration // SIGTERM -> SIGKILL window; default 10s
}

type Result struct {
	ExitCode int
	Killed   bool
	Reason   string
	Err      error
}

// stragglerWait bounds how long Run waits for a process group to empty out
// after it sends a SIGKILL because the group leader was reaped while other
// members of the group were still alive. It never blocks indefinitely.
const stragglerWait = 2 * time.Second

// Run spawns the command in its OWN process group and merges stdout and stderr
// into sink. On cancellation the whole group is signalled, so children that
// outlive their parent — the ones still holding device memory — die too.
func Run(ctx context.Context, spec JobSpec, sink io.Writer) Result {
	if len(spec.Command) == 0 {
		return Result{ExitCode: -1, Err: errors.New("empty command")}
	}
	grace := spec.GraceCeiling
	if grace <= 0 {
		grace = 10 * time.Second
	}

	cmd := exec.Command(spec.Command[0], spec.Command[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env = os.Environ()
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout = sink
	cmd.Stderr = sink
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return Result{ExitCode: -1, Err: fmt.Errorf("start: %w", err)}
	}
	pgid := cmd.Process.Pid // Setpgid makes the child its own group leader.

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	killGroup := func(sig syscall.Signal) {
		// Negative pid addresses the whole group.
		_ = syscall.Kill(-pgid, sig)
	}

	select {
	case err := <-waitErr:
		return finish(err)
	case <-ctx.Done():
		// select picks randomly when both cases are simultaneously ready, so
		// a job that finished in the same instant we observed cancellation
		// could otherwise be mislabelled as killed. Whatever cmd.Wait() sent
		// is still sitting unclaimed in the buffered channel — nothing else
		// reads it — so this non-blocking re-check reliably catches it.
		select {
		case err := <-waitErr:
			return finish(err)
		default:
		}

		cancelReason := "cancelled"
		killGroup(syscall.SIGTERM)

		var err error
		select {
		case err = <-waitErr:
		case <-time.After(grace):
			killGroup(syscall.SIGKILL)
			err = <-waitErr
		}

		res := finish(err)

		// The group leader being reaped does not mean the group is empty: a
		// grandchild that detached from the inherited stdio and ignored
		// SIGTERM (a CUDA worker, say) can outlive it. Don't declare victory
		// over a group that still has members — finish it off, bounded, and
		// say so honestly if something still won't die.
		stragglers := groupAlive(pgid)
		if stragglers {
			killGroup(syscall.SIGKILL)
			if !awaitGroupExit(pgid, stragglerWait) {
				cancelReason = "cancelled; survivors remained after SIGKILL"
			}
		}

		switch {
		case res.Killed:
			// The leader itself died on our signal; that is the
			// authoritative, more useful reason for why this job ended.
			res.Reason = cancelReason
		case stragglers:
			// The leader happened to exit cleanly on its own — a benign
			// race between its own completion and our SIGTERM — but the
			// group still had members we had to reap forcibly. That is
			// still an intervention worth reporting as a kill.
			res.Killed = true
			res.Reason = cancelReason
		}
		// Otherwise the job simply finished on its own in the window
		// between cancellation and our first signal: report the real
		// outcome rather than a cancellation that never actually landed.
		return res
	}
}

// groupAlive reports whether any process still belongs to the process group
// pgid. Signal 0 performs no signalling, only an existence/permission check;
// a live process (ours or not) yields nil, a group with no members yields
// ESRCH.
func groupAlive(pgid int) bool {
	return syscall.Kill(-pgid, 0) == nil
}

// awaitGroupExit polls, bounded by timeout, until process group pgid has no
// members left. It never blocks indefinitely.
func awaitGroupExit(pgid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !groupAlive(pgid) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return !groupAlive(pgid)
}

// finish translates the raw error from cmd.Wait() into a Result. Ground
// truth comes from the kernel's wait status, not from which code path called
// it: if the process was actually terminated by a signal, ExitCode follows
// the 128+signal convention and Killed/Reason say so — that must never read
// as an ordinary, clean exit (an OOM-killed trainer is not a job that called
// exit(137)). If it exited normally, Killed/Reason are left zero-valued;
// Run's cancellation path may still override them, e.g. when the group had
// survivors that needed a forced reap even though the leader exited cleanly.
func finish(err error) Result {
	if err == nil {
		return Result{}
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			sig := ws.Signal()
			return Result{
				ExitCode: 128 + int(sig),
				Killed:   true,
				Reason:   "signal: " + sig.String(),
			}
		}
		return Result{ExitCode: ee.ExitCode()}
	}
	return Result{ExitCode: -1, Err: err}
}
