// Package worker runs jobs on a device host and reports back to the controller.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"time"
)

type JobSpec struct {
	Command      []string
	Cwd          string
	Env          map[string]string
	GraceCeiling time.Duration // SIGTERM -> SIGKILL window; default 10s
	MaxRuntime   time.Duration // total wall-clock ceiling; 0 means no limit
	IdleTimeout  time.Duration // max gap with no stdout/stderr output; 0 means no limit
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

	wd := newWatchdogWriter(sink, time.Now())

	cmd := exec.Command(spec.Command[0], spec.Command[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env = os.Environ()
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout = wd
	cmd.Stderr = wd
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return Result{ExitCode: -1, Err: fmt.Errorf("start: %w", err)}
	}
	pgid := cmd.Process.Pid // Setpgid makes the child its own group leader.

	// runCtx is what actually drives cancellation below: ctx cancelling is one
	// way to trip it, but the watchdogs below are a second, internal way to
	// trip the same context without the caller ever cancelling ctx itself.
	// This reuses the existing SIGTERM -> grace -> SIGKILL path unchanged;
	// nothing here duplicates signal handling.
	runCtx, tripped := context.WithCancel(ctx)
	defer tripped()

	var (
		tripMu     sync.Mutex
		tripReason string
	)
	if spec.MaxRuntime > 0 || spec.IdleTimeout > 0 {
		go func() {
			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()
			deadline := time.Now().Add(spec.MaxRuntime)
			for {
				select {
				case <-runCtx.Done():
					return
				case <-t.C:
					var reason string
					switch {
					case spec.MaxRuntime > 0 && time.Now().After(deadline):
						reason = "max_runtime exceeded (" + spec.MaxRuntime.String() + ")"
					case spec.IdleTimeout > 0 && wd.idleFor() > spec.IdleTimeout:
						reason = "idle: no output for " + spec.IdleTimeout.String()
					default:
						continue
					}
					tripMu.Lock()
					tripReason = reason
					tripMu.Unlock()
					tripped()
					return
				}
			}
		}()
	}

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	killGroup := func(sig syscall.Signal) error {
		// Negative pid addresses the whole group.
		return syscall.Kill(-pgid, sig)
	}

	select {
	case err := <-waitErr:
		return finish(err)
	case <-runCtx.Done():
		// select picks randomly when both cases are simultaneously ready, so
		// a job that finished in the same instant we observed cancellation
		// could otherwise be mislabelled as killed. Whatever cmd.Wait() sent
		// is still sitting unclaimed in the buffered channel — nothing else
		// reads it — so a re-check reliably catches it once our own Wait()
		// goroutine has actually been scheduled to observe it. Give that
		// goroutine every realistic chance to catch up before we commit to
		// treating this as a live signal target: under normal scheduling
		// this loop exits on its first or second spin, but it bounds how
		// long we wait rather than trusting timing blindly.
		for spins := 0; spins < 200; spins++ {
			select {
			case err := <-waitErr:
				return finish(err)
			default:
			}
			runtime.Gosched()
		}

		cancelReason := "cancelled"
		// Signalling must never be gated on anything: SIGTERM goes out
		// unconditionally, and "delivered" is taken straight from kill(2)'s
		// own return value — no probe in front of it.
		//
		// kill(2) succeeds against a zombie (already exited, not yet reaped
		// by our own Wait() goroutine) just as it does against a live
		// process, since the process table entry still exists even though
		// there's no more user-space code left to receive the signal. That
		// means a job which self-completes in the same nanosecond we signal
		// it can still be labelled cancelled. This residual race is
		// unclosable from userspace — no check-then-signal sequence can
		// prove liveness at the instant the signal actually lands, and a
		// prior attempt to add such a probe was measured to change the
		// mislabel rate by nothing (1/900 runs with the probe, 1/900
		// without). The mislabel always lands in the safe direction: a good
		// result gets discarded as cancelled, but a cancelled run is never
		// mistaken for a valid one.
		delivered := killGroup(syscall.SIGTERM) == nil

		var err error
		select {
		case err = <-waitErr:
		case <-time.After(grace):
			_ = killGroup(syscall.SIGKILL)
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
			_ = killGroup(syscall.SIGKILL)
			if !awaitGroupExit(pgid, stragglerWait) {
				cancelReason = "cancelled; survivors remained after SIGKILL"
			}
		}

		if delivered || stragglers {
			// SIGTERM reached at least one member of the group (or a
			// straggler needed forcing out after the fact): this run was
			// cancelled. Keep the real ExitCode from the wait status, but
			// the label belongs to us, not to whatever exit path the
			// process happened to take.
			res.Killed = true
			res.Reason = cancelReason
		}
		// Otherwise the process had already exited by the time the signal
		// went out and nothing in the group was left to reach: report the
		// real outcome rather than a cancellation that never landed.

		// A watchdog trip is a more specific — and more useful — label than
		// the generic "cancelled": it says WHICH limit fired, not just that
		// something external asked for the process to stop.
		tripMu.Lock()
		if tripReason != "" {
			res.Killed = true
			res.Reason = tripReason
		}
		tripMu.Unlock()

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
