// Package cli holds the cobra commands.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/mudler/agents-resources-controller/internal/model"
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
		cwd         string
		as          string
		priority    int
		maxRuntime  time.Duration
		idleTimeout time.Duration
		timeout     time.Duration
		noWait      bool
	)

	cmd := &cobra.Command{
		Use:   "run -d <device> -- <command>...",
		Short: "Claim a device, run a command on it, stream the output",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if device == "" {
				return errors.New("-d/--device is required in stage 1")
			}
			// A local copy: NewRunCmd's closures are shared by every RunE
			// call on this *cobra.Command, so writing the computed default
			// back into `as` would leak the first invocation's identity into
			// any later one that also left --as unset.
			submitter := as
			if submitter == "" {
				submitter = defaultSubmitter()
			}

			c := client.New(controllerURL(), controllerToken())
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

			job, err := c.Submit(ctx, client.SubmitOptions{
				DeviceID:       device,
				Command:        args,
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
				return fmt.Errorf("%s is busy and could not be queued", device)
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
					fmt.Fprintf(stderr, "rc: queued at position %d for %s\n", pos, device)
				})
				switch {
				case err == nil:
					job = scheduled
				case ctx.Err() != nil:
					// Ctrl-C while queued cancels outright: nothing is
					// running yet, so there is no lease to protect.
					cancelQueuedJob(c, stop, stderr, job, submitter)
					return exitCodeError{code: sigintExitCode}
				case errors.Is(err, context.DeadlineExceeded):
					// A genuine --timeout expiry: same reasoning as
					// Ctrl-C above, nothing is running yet.
					cancelQueuedJob(c, stop, stderr, job, submitter)
					return fmt.Errorf("gave up waiting for %s: %w", device, err)
				default:
					// An error WaitScheduled itself gave up on (e.g. a run
					// of transient poll failures surviving past its own
					// retry budget) is NOT license to kill: by the time
					// this surfaces, the job may already have been
					// scheduled and be running on real hardware. A
					// client-side network blip must cost the caller
					// patience, never someone else's GPU job. Report the
					// error and leave the job alone.
					return fmt.Errorf(
						"lost track of job %s while it was queued (it may still be queued, or already running — check `rc ps`): %w",
						job.ID, err)
				}
			}

			fmt.Fprintf(stderr, "rc: job %s on %s\n", job.ID, job.DeviceID)

			streamErr := c.StreamLogs(ctx, job.ID, os.Stdout)
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
	cmd.Flags().StringVar(&cwd, "cwd", "", "working directory on the device host")
	cmd.Flags().StringVar(&as, "as", "", "identity shown in rc ps (defaults to user@host/session)")
	cmd.Flags().IntVar(&priority, "priority", 0, "queue priority; higher runs sooner")
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

func controllerURL() string {
	if v := os.Getenv("RC_CONTROLLER"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func controllerToken() string { return os.Getenv("RC_TOKEN") }
