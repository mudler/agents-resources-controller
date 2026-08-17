// Package cli holds the cobra commands.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/spf13/cobra"
)

// exitCodeError carries a job's exit status up to main so `rc run` exits with
// the code the job produced, keeping it drop-in for scripts and CI.
type exitCodeError struct{ code int }

func (e exitCodeError) Error() string { return fmt.Sprintf("job exited %d", e.code) }
func (e exitCodeError) ExitCode() int { return e.code }

// sigintExitCode is the conventional shell exit status for a process
// stopped by SIGINT (128 + signal number 2). rc run reports it when it
// detaches on Ctrl-C, since the job's own exit code is not yet known — the
// job is still running.
const sigintExitCode = 130

// waitTerminalTimeout bounds the confirmation read after log streaming
// ends. StreamLogs already blocks until the job reaches a terminal state
// (see server.handleStreamLogs), so in the ordinary path this call returns
// on its very first poll. The bound exists only so rc can never hang
// forever if that assumption is ever violated — e.g. a job stuck in
// "assigned" because no worker ever attached to the device.
const waitTerminalTimeout = 5 * time.Minute

// killTimeout bounds the kill request rc run issues when it gives up on a
// still-queued job (Ctrl-C, or --timeout). The client's http.Client
// deliberately has no global timeout (see client.New — log streaming has
// to run as long as the job does), so without a bound here a wedged
// controller connection would hang this specific call indefinitely. It is
// called only after stop() has already unregistered signal interception,
// so a hang here is a bounded annoyance, not a process a second Ctrl-C
// can't get out of.
const killTimeout = 10 * time.Second

func defaultSubmitter() string {
	// RC_SUBMITTER is for agents: a session says who it is once, in its
	// environment, instead of repeating --as on every run, hold, kill and
	// release. Forgetting it on the kill is the failure that matters —
	// the run succeeds under one identity and the kill is then refused
	// with not_job_owner. Whitespace-only is treated as unset: that is
	// the shape of `export RC_SUBMITTER=$SOMETHING_UNSET`, a mistake
	// rather than a request for a blank identity, and a blank submitter
	// would make the ownership check vacuous for every job on the fleet.
	// An explicit --as still wins over this; see the flag's callers.
	if s := strings.TrimSpace(os.Getenv("RC_SUBMITTER")); s != "" {
		return s
	}
	if s := userConfigValue(func(c userConfig) string { return c.Submitter }); s != "" {
		return s
	}
	user := os.Getenv("USER")
	host, _ := os.Hostname()
	if session := os.Getenv("CLAUDE_SESSION_ID"); session != "" {
		return fmt.Sprintf("%s@%s/%s", user, host, session)
	}
	return fmt.Sprintf("%s@%s", user, host)
}

func NewRunCmd() *cobra.Command {
	var (
		device      string
		selector    string
		explain     bool
		cwd         string
		as          string
		priority    int
		maxRuntime  time.Duration
		idleTimeout time.Duration
		timeout     time.Duration
		noWait      bool
		tty         bool
	)

	cmd := &cobra.Command{
		Use:   "run (-d <device> | --select <selector>) -- <command>...",
		Short: "Claim a device, run a command on it, stream the output",
		// --explain needs no command at all — it never submits anything —
		// so the usual "a command is required" check only applies when it
		// is not set. explain is read here via closure, exactly like RunE
		// below, so it reflects the flags cobra has already parsed by the
		// time Args runs.
		Args: func(cmd *cobra.Command, args []string) error {
			// --explain never submits, and --tty with no command means "a
			// shell", which is the whole point of asking for one.
			if explain || tty {
				return nil
			}
			return cobra.MinimumNArgs(1)(cmd, args)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Rejected before any request is made, and before defaultSubmitter
			// or the client are even constructed: --select and -d name two
			// different, incompatible ways to pick a device, and letting one
			// silently win would submit against whichever the caller did not
			// intend.
			if device != "" && selector != "" {
				return fmt.Errorf("--select and -d/--device are mutually exclusive (got --select=%q and -d/--device=%q)", selector, device)
			}

			c := client.New(controllerURL(), controllerToken())

			// --explain answers "what would this selector do" and stops
			// there: it must be impossible to accidentally submit alongside
			// it, so this branch returns before defaultSubmitter, Submit, or
			// anything else that could create a job even runs.
			if explain {
				if selector == "" {
					return errors.New("--explain requires --select (it explains a selector, not a single device)")
				}
				resp, err := c.Explain(cmd.Context(), selector)
				if err != nil {
					return err
				}
				return renderExplain(cmd.OutOrStdout(), resp)
			}

			if device == "" && selector == "" {
				return errors.New("-d/--device or --select is required")
			}
			// target names whichever the caller gave, for messages that used
			// to only ever have -d's value to report.
			target := device
			if target == "" {
				target = selector
			}

			// A local copy: NewRunCmd's closures are shared by every RunE
			// call on this *cobra.Command, so writing the computed default
			// back into `as` would leak the first invocation's identity into
			// any later one that also left --as unset.
			submitter := as
			if submitter == "" {
				submitter = defaultSubmitter()
			}

			stderr := cmd.ErrOrStderr()

			// ctx carries SIGINT/SIGTERM so Ctrl-C interrupts log streaming
			// promptly. It does NOT cancel the job: the worker, not this
			// client, owns the process, so a disconnected `rc run` is a lost
			// terminal, not a lost run. That is deliberate — the design doc
			// is explicit that a dropped client must not affect a running
			// job, since tying a lease's lifetime to a fragile client
			// session is exactly the failure mode this project replaces
			// flock to avoid. Server-side cancellation (`rc kill`) is a
			// later stage.
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			// A --tty run with no command asks for a shell. The command is
			// still an ordinary job command recorded on the job row, so
			// `rc ps` shows what is on the box rather than a blank.
			command := args
			stdio := model.StdioLogs
			if tty {
				stdio = model.StdioTTY
				if len(command) == 0 {
					command = defaultTTYCommand
				}
			}

			job, err := c.Submit(ctx, client.SubmitOptions{
				DeviceID:       device,
				Selector:       selector,
				Command:        command,
				Stdio:          stdio,
				Cwd:            cwd,
				Submitter:      submitter,
				IdempotencyKey: uuid.NewString(),
				Priority:       priority,
				MaxRuntime:     maxRuntime,
				IdleTimeout:    idleTimeout,
				NoWait:         noWait,
			})
			if errors.Is(err, client.ErrNoDevice) {
				// --no-wait opts out of stage 2's queue: a busy device fails
				// fast here instead of the job sitting queued behind it.
				return fmt.Errorf("%s is busy and could not be queued", target)
			}
			if err != nil {
				return err
			}

			if job.State == model.JobQueued {
				waitCtx := ctx
				if timeout > 0 {
					var cancelWait context.CancelFunc
					waitCtx, cancelWait = context.WithTimeout(ctx, timeout)
					defer cancelWait()
				}

				scheduled, err := c.WaitScheduled(waitCtx, job.ID, func(pos int) {
					fmt.Fprintf(stderr, "rc: queued at position %d for %s\n", pos, target)
				})
				switch {
				case err == nil:
					job = scheduled
				case ctx.Err() != nil:
					// Ctrl-C while queued cancels outright: nothing is
					// running yet, so there is no lease to protect.
					cancelQueuedJob(c, stop, stderr, job, submitter)
					return exitCodeError{code: sigintExitCode}
				case waitCtx.Err() != nil:
					// The caller's own --timeout bound elapsed. This is
					// deliberately checked against waitCtx's own Err()
					// directly, NOT by inspecting err for a wrapped
					// context.DeadlineExceeded: WaitScheduled bounds each
					// individual poll with its own per-request timeout, so
					// a hung controller (accepted connection, never
					// answered) produces that exact same error with no
					// caller deadline in sight at all — unwrapping it here
					// would kill a job whose only crime was that the
					// controller went quiet, which is precisely the bug
					// this switch exists to prevent. waitCtx's own Err()
					// is only ever non-nil here because its deadline
					// genuinely passed (when timeout<=0, waitCtx is ctx
					// itself, already ruled out by the case above).
					cancelQueuedJob(c, stop, stderr, job, submitter)
					return fmt.Errorf("gave up waiting for %s: %w", target, err)
				default:
					// Any other error — most notably WaitScheduled
					// exhausting its own retry budget against a wedged or
					// erroring controller — is NOT license to kill: by the
					// time this surfaces, the job may already have been
					// scheduled and be running on real hardware. A
					// client-side network blip must cost the caller
					// patience, never someone else's GPU job. Report the
					// error and leave the job alone.
					return fmt.Errorf(
						"lost track of job %s while it was queued (it may still be queued, or already running — check `rc ps`): %w",
						job.ID, err)
				}

				// Leaving the queue is not the same as being scheduled: a job
				// can also leave it by being killed — someone else's `rc
				// kill`, an admin cancelling the backlog — and WaitScheduled
				// returns on any state change, not just a happy one.
				// Announcing "rc: job X on gpu0" and then streaming a log
				// that will never have anything in it tells the user the
				// opposite of what happened. A job that actually started
				// (started_at set, however briefly) still goes down the
				// normal path below, so a fast job's output is never skipped.
				if job.State.Terminal() && job.StartedAt == nil {
					reason := job.KillReason
					if reason == "" {
						reason = "no further detail available"
					}
					return fmt.Errorf("job %s was %s while queued and never started on %s: %s",
						job.ID, job.State, job.DeviceID, reason)
				}
			}

			fmt.Fprintf(stderr, "rc: job %s on %s\n", job.ID, job.DeviceID)

			// An interactive session replaces the log stream with the
			// terminal: the job's bytes go through the relay and are never
			// written to the log store, so there would be nothing to stream.
			//
			// Attaching here, after the queue wait, rather than the instant
			// the job has an ID: the relay holds both connect orders and
			// would carry type-ahead through a wait, but raw mode during a
			// queue that prints its position on every change is a worse
			// experience than waiting for the shell, and `rc cp` is the
			// caller that genuinely needs the early attach.
			streamErr := error(nil)
			if tty {
				streamErr = attachTerminal(ctx, c, job.ID, stderr)
			} else {
				streamErr = c.StreamLogs(ctx, job.ID, os.Stdout)
			}
			if derr := detachIfInterrupted(ctx, stop, stderr, job); derr != nil {
				return derr
			}
			if streamErr != nil {
				fmt.Fprintf(stderr, "rc: log stream ended: %v\n", streamErr)
			}

			final, err := c.WaitTerminal(ctx, job.ID, waitTerminalTimeout)
			if derr := detachIfInterrupted(ctx, stop, stderr, job); derr != nil {
				return derr
			}
			if err != nil {
				return err
			}

			if final.ExitCode != nil && *final.ExitCode != 0 {
				// The exit code alone doesn't explain a supervisor-level
				// failure (e.g. "start: no such file"); the program's own
				// output wouldn't have said that either, so tell the user
				// here rather than leaving a bare non-zero exit.
				if final.KillReason != "" {
					fmt.Fprintf(stderr, "rc: job %s: %s\n", final.ID, final.KillReason)
				}
				return exitCodeError{code: safeExitCode(*final.ExitCode)}
			}
			if final.State != model.JobSucceeded {
				reason := final.KillReason
				if reason == "" {
					reason = "no further detail available"
				}
				return fmt.Errorf("job %s: %s: %s", final.ID, final.State, reason)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&device, "device", "d", "", "device ID, e.g. gpubox:gpu0")
	cmd.Flags().StringVar(&selector, "select", "", "device selector, e.g. vram>=40G (mutually exclusive with -d)")
	cmd.Flags().BoolVar(&explain, "explain", false,
		"with --select, report which devices match, how many are free, and the queue depth, then exit without submitting")
	cmd.Flags().BoolVar(&tty, "tty", false,
		"run the command under a real terminal and attach to it; with no command, gives you a shell on the device")
	cmd.Flags().StringVar(&cwd, "cwd", "", "working directory on the device host")
	cmd.Flags().StringVar(&as, "as", "", "identity shown in rc ps (defaults to $RC_SUBMITTER, else user@host/session)")
	cmd.Flags().IntVar(&priority, "priority", 0, fmt.Sprintf(
		"queue priority, %d..%d; higher runs sooner within a device's queue",
		server.MinPriority, server.MaxPriority))
	cmd.Flags().DurationVar(&maxRuntime, "max-runtime", 0, "kill the job if it runs longer than this (0 = device default)")
	cmd.Flags().DurationVar(&idleTimeout, "idle-timeout", 0, "kill the job if it produces no output for this long (0 = device default)")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "give up waiting in the queue after this long and cancel the job (0 = wait forever)")
	cmd.Flags().BoolVar(&noWait, "no-wait", false, "fail immediately if the device is busy instead of queueing")
	return cmd
}

// detachIfInterrupted reports whether ctx ended because of a signal rather
// than because the call it wrapped finished normally. If so it stops
// intercepting signals immediately — so a second Ctrl-C, or the SIGTERM
// that typically follows in CI, kills rc outright instead of appearing to
// hang — tells the user the truth (the job is still running and still
// holds the device; nothing cancels it from here), and returns the
// conventional SIGINT exit status. There is nothing left to wait for on
// this path: the job is not cancelled, so blocking further would just mean
// waiting for as long as the job keeps running.
func detachIfInterrupted(ctx context.Context, stop context.CancelFunc, stderr io.Writer, job *model.Job) error {
	if ctx.Err() == nil {
		return nil
	}
	stop()
	fmt.Fprintf(stderr,
		"rc: detached from job %s — it is STILL RUNNING on %s and holds it until it finishes. Watch it with: rc ps\n",
		job.ID, job.DeviceID)
	return exitCodeError{code: sigintExitCode}
}

// cancelQueuedJob is called when rc run has given up waiting for a still-
// queued job — via Ctrl-C or --timeout — and needs to cancel it. There is
// no lease yet at this point, so cancelling is always safe to attempt; the
// one thing that can go wrong is the job having started running in the
// tiny window since the last poll, which the controller reports back as a
// 409 (client.ErrNotCancellable). That must NOT be reported as an ordinary
// "could not cancel": the job may now actually be consuming a device with
// no client watching it, and the user needs Stage 1's honest "STILL
// RUNNING" warning, not a bland failure message — see
// reportUncancellableQueuedJob.
//
// stop is called first, before any network call: a second Ctrl-C (or the
// SIGTERM that typically follows it in CI) must kill rc outright rather
// than appear to hang while this call is in flight — the same reasoning
// detachIfInterrupted already applies to the running-job detach path. The
// kill request itself is bounded by killTimeout for the same reason: the
// client's http.Client deliberately has no global timeout.
func cancelQueuedJob(c *client.Client, stop context.CancelFunc, stderr io.Writer, job *model.Job, submitter string) {
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), killTimeout)
	defer cancel()

	err := c.Kill(ctx, job.ID, submitter)
	switch {
	case err == nil:
		reportCancelledQueuedJob(ctx, c, stderr, job)
	case errors.Is(err, client.ErrNotCancellable):
		reportUncancellableQueuedJob(ctx, c, stderr, job)
	default:
		fmt.Fprintf(stderr, "rc: could not cancel queued job %s: %v\n", job.ID, err)
	}
}

// reportCancelledQueuedJob is called after a successful kill of a job that
// was last seen queued. A successful kill does not by itself say whether
// the job was cancelled outright (it was still queued: see
// store.CancelQueued, which finishes it synchronously) or whether it had
// already started running in the meantime and was instead flagged for the
// worker to terminate (store.RequestKill, asynchronous — see
// server.handleKill). Re-fetching and reporting the actual outcome avoids
// unconditionally claiming "cancelled queued job" when what really
// happened is a running job was just flagged for kill and has not stopped
// yet.
func reportCancelledQueuedJob(ctx context.Context, c *client.Client, stderr io.Writer, job *model.Job) {
	current, err := c.Job(ctx, job.ID)
	if err != nil {
		// Best effort only — the kill call itself already succeeded.
		fmt.Fprintf(stderr, "rc: kill requested for job %s\n", job.ID)
		return
	}
	switch current.State {
	case model.JobAssigned, model.JobRunning:
		fmt.Fprintf(stderr,
			"rc: job %s started running on %s just as it was being cancelled; a kill was requested and it is being terminated now\n",
			current.ID, current.DeviceID)
	default:
		fmt.Fprintf(stderr, "rc: cancelled queued job %s\n", job.ID)
	}
}

// reportUncancellableQueuedJob is called when the controller refuses to
// cancel a job rc run last saw queued (409 not_cancellable): the job raced
// onto a different state between the last poll and this kill attempt. That
// can mean two very different things — it started running (and is now
// consuming a device with nobody watching it), or it simply finished on
// its own — so this re-fetches the job to tell them apart rather than
// reporting a bland "could not cancel" for both.
func reportUncancellableQueuedJob(ctx context.Context, c *client.Client, stderr io.Writer, job *model.Job) {
	current, err := c.Job(ctx, job.ID)
	if err == nil && (current.State == model.JobAssigned || current.State == model.JobRunning) {
		fmt.Fprintf(stderr,
			"rc: could not cancel job %s — it is STILL RUNNING on %s and holds it until it finishes. Watch it with: rc ps\n",
			current.ID, current.DeviceID)
		return
	}
	fmt.Fprintf(stderr, "rc: job %s finished before it could be cancelled\n", job.ID)
}

// safeExitCode guards against os.Exit's mod-256 wraparound on Unix: a
// reported exit code that happens to be an exact multiple of 256 (e.g. 256,
// 512) would otherwise present as a 0 and look like success. Stage 1 has no
// documented way to produce such a code today, but this mapping must never
// silently manufacture a success.
func safeExitCode(code int) int {
	if code != 0 && code%256 == 0 {
		return 1
	}
	return code
}

// renderExplain prints what `rc run --explain` promises: which devices
// match the selector, how many of those are free right now, and how deep
// the queue already is behind the ones that aren't — everything a caller
// needs to decide whether to submit, without ever submitting on its behalf.
func renderExplain(w io.Writer, resp *server.ExplainResponse) error {
	free := make(map[string]bool, len(resp.Free))
	for _, id := range resp.Free {
		free[id] = true
	}

	fmt.Fprintf(w, "selector: %s\n", resp.Selector)
	if len(resp.Matching) == 0 {
		fmt.Fprintln(w, "matching: (no device matches this selector)")
	} else {
		fmt.Fprintf(w, "matching (%d):\n", len(resp.Matching))
		for _, id := range resp.Matching {
			status := "busy"
			if free[id] {
				status = "free"
			}
			fmt.Fprintf(w, "  %s  %s\n", id, status)
		}
	}
	fmt.Fprintf(w, "free: %d/%d\n", len(resp.Free), len(resp.Matching))
	fmt.Fprintf(w, "queue depth: %d\n", resp.QueueDepth)
	return nil
}

func controllerURL() string {
	if v := os.Getenv("RC_CONTROLLER"); v != "" {
		return v
	}
	if v := userConfigValue(func(c userConfig) string { return c.Controller }); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func controllerToken() string {
	if v := os.Getenv("RC_TOKEN"); v != "" {
		return v
	}
	return userConfigValue(func(c userConfig) string { return c.Token })
}
