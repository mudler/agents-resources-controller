// Package cli holds the cobra commands.
package cli

import (
	"context"
	"errors"
	"fmt"
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
		device string
		cwd    string
		as     string
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
			})
			if errors.Is(err, client.ErrNoDevice) {
				// rc run does not set NoWait, so a plain busy device queues
				// rather than reaching this branch; it stays here as the
				// correct response if that ever changes (e.g. a future
				// --no-wait flag).
				return fmt.Errorf("%s is busy and could not be queued", device)
			}
			if err != nil {
				return err
			}

			fmt.Fprintf(os.Stderr, "rc: job %s on %s\n", job.ID, job.DeviceID)

			streamErr := c.StreamLogs(ctx, job.ID, os.Stdout)
			if derr := detachIfInterrupted(ctx, stop, job); derr != nil {
				return derr
			}
			if streamErr != nil {
				fmt.Fprintf(os.Stderr, "rc: log stream ended: %v\n", streamErr)
			}

			final, err := c.WaitTerminal(ctx, job.ID, waitTerminalTimeout)
			if derr := detachIfInterrupted(ctx, stop, job); derr != nil {
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
					fmt.Fprintf(os.Stderr, "rc: job %s: %s\n", final.ID, final.KillReason)
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
func detachIfInterrupted(ctx context.Context, stop context.CancelFunc, job *model.Job) error {
	if ctx.Err() == nil {
		return nil
	}
	stop()
	fmt.Fprintf(os.Stderr,
		"rc: detached from job %s — it is STILL RUNNING on %s and holds it until it finishes. Watch it with: rc ps\n",
		job.ID, job.DeviceID)
	return exitCodeError{code: sigintExitCode}
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
