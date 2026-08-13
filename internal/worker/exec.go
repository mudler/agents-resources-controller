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
	"strings"
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

	// Held open for the leader's whole lifetime so a liveness probe right
	// before signalling costs a single pread, not an open+read+close each
	// time — keeping that probe as fast as possible matters, since every
	// microsecond it takes is more time for the target to race ahead of us.
	statFile, _ := os.Open(fmt.Sprintf("/proc/%d/stat", pgid))
	if statFile != nil {
		defer statFile.Close()
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
	case <-ctx.Done():
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
		// Whether this run counts as cancelled hinges on whether SIGTERM
		// actually reached a live process, not on how it chose to react. A
		// single-process job that traps SIGTERM and exits 0 is still a
		// cancelled run, not a successful one — the operator's interruption
		// must never be recorded as a completed measurement.
		//
		// kill(2) alone can't tell us that: it reports success against a
		// zombie (already exited, not yet reaped) too, since the process
		// entry still exists even though there's no more user-space code
		// left to receive the signal. Checking the kernel's live view of the
		// leader directly, immediately before signalling, catches that
		// window; nothing can close it to zero (there is always some gap
		// between a check and a syscall), but keeping the check to a single
		// pread on an already-open fd keeps that gap as tight as possible.
		delivered := processIsReachable(statFile) && killGroup(syscall.SIGTERM) == nil

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
		return res
	}
}

// processIsReachable reports whether the leader (whose already-open
// /proc/<pid>/stat handle is f) is still a live, signal-receiving process:
// neither fully reaped already (f is nil — the open at Start() time raced
// with an implausibly fast exit) nor sitting as a zombie (exited, awaiting
// reap, no more user-space code left to run). Either state means a signal
// sent to it has no real effect, even though kill(2) itself may still report
// success against a zombie.
func processIsReachable(f *os.File) bool {
	if f == nil {
		return false
	}
	var buf [256]byte
	n, err := f.ReadAt(buf[:], 0)
	if err != nil && n == 0 {
		return false
	}
	// Format: "pid (comm) state ...". comm may itself contain spaces or
	// parentheses, so the state field is whatever follows the LAST ')'.
	s := string(buf[:n])
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return false
	}
	return s[i+2] != 'Z'
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
