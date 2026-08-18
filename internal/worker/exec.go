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
	// Stderr, if non-nil, receives the process's stderr separately from the
	// sink passed to Run (which then carries only stdout). Nil (the
	// default) preserves today's behavior for jobs and hooks: stdout and
	// stderr both land in sink, combined, so a live `rc run` sees one
	// interleaved stream. A caller that needs to parse stdout on its own —
	// a probe emitting a single JSON object, where a stray stderr line
	// would otherwise corrupt the parse — sets this to keep the streams
	// apart.
	Stderr io.Writer
	// Stdin, if non-nil, is the process's standard input. Nil (the default)
	// leaves it as /dev/null, which is what every unattended job has always
	// had: a job nobody is watching that blocks reading a terminal nobody is
	// typing at would sit there holding the GPU until a watchdog fired.
	//
	// It is wired through an os.Pipe this package creates rather than handed
	// to os/exec directly, and that is not incidental — see Run.
	Stdin io.Reader
	// SweepJobID, when non-empty, turns on the RC_JOB_ID sweep: once the
	// process group (or PTY session) has been torn down, every remaining
	// process whose environment carries exactly this job id is signalled out
	// of existence too. That is what catches a child which called setsid(2)
	// and so left the group and the session the kill was aimed at. See
	// sweepJobSurvivors in sweep.go for the mechanism and its residual.
	//
	// It is an explicit opt-in and not read out of Env["RC_JOB_ID"], even
	// though execute() sets both to the same value, because the callers must
	// differ: a JOB gets swept, while lifecycle hooks (hooks.go) and verify
	// probes (verify.go) run with the same RC_JOB_ID in their environment and
	// must never sweep — a hook or probe hunting for processes by job id is
	// a mechanism nobody asked for, aimed at a job that is either about to
	// start or already accounted for.
	SweepJobID string
}

type Result struct {
	ExitCode int
	Killed   bool
	Reason   string
	Err      error
	// Sweep records what the RC_JOB_ID sweep found and did, when
	// JobSpec.SweepJobID asked for one. It is a report, NOT an outcome:
	// nothing about it ever touches Killed, Reason or ExitCode, because a
	// job that exited 0 and left a detached process behind is a successful
	// job. See SweepReport in sweep.go for what happened the last time a
	// straggler sweep was allowed to relabel a job.
	Sweep SweepReport
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

	// clock is shared between the stdout and stderr writers below (even when
	// they end up wrapping different underlying sinks) so the idle watchdog
	// measures real progress on EITHER stream — a job that only ever writes
	// to stderr must still count as producing output, exactly as it did
	// before stdout/stderr could be split.
	clock := newIdleClock(time.Now())
	stdoutW := newWatchdogWriter(sink, clock)

	cmd := exec.Command(spec.Command[0], spec.Command[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env = os.Environ()
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout = stdoutW
	if spec.Stderr != nil {
		cmd.Stderr = newWatchdogWriter(spec.Stderr, clock)
	} else {
		cmd.Stderr = stdoutW
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Standard input, when a caller supplies one, goes through an os.Pipe we
	// own rather than being handed to os/exec as a plain io.Reader. That
	// looks like extra work and is the difference between a job that ends
	// and one that hangs: given a non-file Reader, os/exec makes its own pipe
	// and copies in a goroutine that cmd.Wait() then WAITS FOR. The reader
	// here is a relay stream that only ends when the operator's client
	// disconnects, so `tar -cf -` — which never reads its stdin at all —
	// would exit immediately and leave Wait() blocked on a copy goroutine
	// parked in a Read that nothing is ever going to answer. With a real
	// *os.File, Wait() waits for the process and nothing else; our own copy
	// goroutine unwinds when the caller closes the stream.
	if spec.Stdin != nil {
		pr, pw, err := os.Pipe()
		if err != nil {
			return Result{ExitCode: -1, Err: fmt.Errorf("stdin pipe: %w", err)}
		}
		defer pr.Close()
		cmd.Stdin = pr
		go func() {
			defer pw.Close()
			// A write failing means the process closed its stdin or exited,
			// which is ordinary and not this side's business to report.
			_, _ = io.Copy(pw, spec.Stdin)
		}()
	}

	if err := cmd.Start(); err != nil {
		return Result{ExitCode: -1, Err: fmt.Errorf("start: %w", err)}
	}
	pgid := cmd.Process.Pid // Setpgid makes the child its own group leader.

	return runSupervised(ctx, cmd, pgidTarget(pgid), spec, clock)
}

// processTarget abstracts "the thing SIGTERM/SIGKILL are aimed at" so
// runSupervised below serves both Run's plain process group and startPTY's
// whole terminal session through the SAME sequence, rather than forking into
// two copies of it. A piped job with no controlling terminal is fully
// described by one process group (pgidTarget, in this file). An
// interactive shell attached to a PTY additionally does job control, which
// moves each backgrounded command into its own process group within the
// session — pty.go's sessionTarget reaches all of them.
type processTarget interface {
	// signal sends sig to the target and reports whether it reached
	// anything, straight from the underlying kill(2) return value.
	signal(sig syscall.Signal) (delivered bool)
	// alive reports whether anything belonging to the target still exists.
	alive() bool
}

// pgidTarget targets a single process group by id, exactly as Run has always
// killed the jobs it spawns.
type pgidTarget int

func (t pgidTarget) signal(sig syscall.Signal) bool {
	// Negative pid addresses the whole group.
	return syscall.Kill(-int(t), sig) == nil
}

func (t pgidTarget) alive() bool { return groupAlive(int(t)) }

// runSupervised is the single implementation of "kill the whole target on
// cancellation, SIGTERM -> grace -> SIGKILL, then mop up stragglers" that
// both Run (via pgidTarget) and startPTY (via sessionTarget) rely on. cmd
// must already have been started; runSupervised owns waiting on it.
//
// It is a thin wrapper over superviseTarget, which is the supervision itself,
// plus the RC_JOB_ID sweep that runs AFTER it: the sweep's whole purpose is
// to catch what is left once the process group and session have been torn
// down, so it can only run when that teardown is finished and it must run on
// every path out of it — a clean exit leaks a detached child exactly as
// readily as a cancelled one does.
//
// The result of the sweep lands in its own field and changes nothing else.
// That separation is the point; see SweepReport.
func runSupervised(ctx context.Context, cmd *exec.Cmd, target processTarget, spec JobSpec, clock *idleClock) Result {
	res := superviseTarget(ctx, cmd, target, spec, clock)
	if spec.SweepJobID != "" {
		res.Sweep = sweepJobSurvivors(spec.SweepJobID, os.Getpid())
	}
	return res
}

// superviseTarget is the supervision itself, unchanged by the sweep sitting
// on top of it: wait, and on cancellation signal the target, wait out the
// grace, force it, then mop up whatever is left INSIDE the target.
func superviseTarget(ctx context.Context, cmd *exec.Cmd, target processTarget, spec JobSpec, clock *idleClock) Result {
	grace := spec.GraceCeiling
	if grace <= 0 {
		grace = 10 * time.Second
	}

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
					case spec.IdleTimeout > 0 && clock.idleFor() > spec.IdleTimeout:
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

	select {
	case err := <-waitErr:
		return finishClean(err, target)
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
				return finishClean(err, target)
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
		delivered := target.signal(syscall.SIGTERM)

		var err error
		select {
		case err = <-waitErr:
		case <-time.After(grace):
			target.signal(syscall.SIGKILL)
			err = <-waitErr
		}

		res := finish(err)

		// The group leader being reaped does not mean the target is empty: a
		// grandchild that detached from the inherited stdio and ignored
		// SIGTERM (a CUDA worker, say) can outlive it. Don't declare victory
		// while it still has members — finish it off, bounded, and say so
		// honestly if something still won't die.
		stragglers, allGone := reapStragglers(target)
		if stragglers && !allGone {
			cancelReason = "cancelled; survivors remained after SIGKILL"
		}

		if delivered || stragglers {
			// SIGTERM reached at least one member of the target (or a
			// straggler needed forcing out after the fact): this run was
			// cancelled. Keep the real ExitCode from the wait status, but
			// the label belongs to us, not to whatever exit path the
			// process happened to take.
			res.Killed = true
			res.Reason = cancelReason
		}
		// Otherwise the process had already exited by the time the signal
		// went out and nothing in the target was left to reach: report the
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

// groupAlive reports whether any process that still belongs to the process
// group pgid is a real process rather than a zombie.
//
// Signal 0 performs no signalling, only an existence/permission check, and it
// is the fast path: a group with no members at all yields ESRCH and we are
// done in one syscall. It is not sufficient on its own, though, and the
// difference is not academic. A process that has exited but whose parent has
// not yet reaped it is still a group member and still answers kill(-pgid, 0)
// with success — while holding no memory, no device and no CPU, and having no
// user-space code left to receive a signal. Treating one as a straggler made
// every ordinary job that backgrounds anything report itself killed, because
// cmd.Wait() returns the instant the last pipe-holder exits, which is the
// instant it becomes a zombie and not one moment later. So when the cheap
// check says something is there, look at what it actually is.
//
// If /proc cannot be read at all we keep the old, coarser answer: on this
// worker's only platform that does not happen, and reporting "alive" when we
// cannot tell errs toward killing a straggler rather than leaking one.
func groupAlive(pgid int) bool {
	if syscall.Kill(-pgid, 0) != nil {
		return false
	}
	live, scanned := liveProcExists(func(st procStat) bool { return st.pgid == pgid })
	return live || !scanned
}

// awaitTargetExit polls, bounded by timeout, until target has no members
// left. It never blocks indefinitely.
func awaitTargetExit(target processTarget, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !target.alive() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return !target.alive()
}

// reapStragglers checks whether anything is still alive under target and, if
// so, force-kills it and waits (bounded by stragglerWait) for it to actually
// go away. found reports whether anything was there to reap; allGone reports
// whether it was successfully gone by the time this returned (only
// meaningful when found is true — it is unconditionally true otherwise, so a
// caller can test `found && !allGone` without a separate check).
//
// It is used both after the primary process has already been signalled (the
// cancellation path) and after a completely ordinary, un-cancelled exit (see
// finishClean) — a background job surviving its parent doesn't care which
// path ended the parent.
func reapStragglers(target processTarget) (found, allGone bool) {
	if !target.alive() {
		return false, true
	}
	target.signal(syscall.SIGKILL)
	return true, awaitTargetExit(target, stragglerWait)
}

// finishClean turns a wait error into a Result the way finish does, but also
// reaps anything still alive under target before returning. This closes the
// gap a clean, un-cancelled exit used to leave open: under Run, cmd.Wait()
// blocking on an inherited pipe until every process holding it closes tends
// to mask a leaked grandchild by accident (the pipe just doesn't reach EOF
// yet), but a PTY session has no such pipe to block on — Wait() returns the
// instant the shell itself exits. The ordinary way an interactive session
// ends is the operator typing `exit`, the leader exiting 0 immediately; that
// must not report success while a background job it spawned (`sleep 300 &`)
// keeps running and keeps holding the GPU.
func finishClean(err error, target processTarget) Result {
	res := finish(err)
	if found, allGone := reapStragglers(target); found {
		res.Killed = true
		if allGone {
			res.Reason = "stragglers reaped after exit"
		} else {
			res.Reason = "stragglers remained after exit"
		}
	}
	return res
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
