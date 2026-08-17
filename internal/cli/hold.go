package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/spf13/cobra"
)

// NewHoldCmd claims a device for a human, not a job: "I need a shell on
// this box", not "run this command". Under the hood it is a job with kind
// "hold" whose command the worker itself chooses (a sleeper) — see
// internal/worker's execute — so it goes through the exact same
// allocation, queue, and expiry machinery as rc run, and shows up in rc ps
// and rc devices with no new rendering. See the task 8 design note for why
// this is deliberately not a second lease mechanism.
//
// It blocks until granted, prints the device and when it expires, then
// stays attached: unlike rc run, Ctrl-C on a GRANTED hold releases it
// (see the WaitTerminal branch below) rather than merely detaching,
// because a hold's whole purpose is that a human is present, and leaving
// means they're done with it.
func NewHoldCmd() *cobra.Command {
	var (
		selector string
		ttl      time.Duration
		reason   string
		as       string
	)

	cmd := &cobra.Command{
		Use:   "hold (<device> | --select <selector>) --ttl <duration>",
		Short: "Take a device for yourself — for a shell, not a job",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var device string
			if len(args) == 1 {
				device = args[0]
			}
			if device != "" && selector != "" {
				return fmt.Errorf("--select and a device ID are mutually exclusive (got --select=%q and device=%q)", selector, device)
			}
			if device == "" && selector == "" {
				return errors.New("a device ID or --select is required")
			}
			// Checked before --ttl on purpose: a caller missing both should
			// not be told to fix the ttl, fix it, and only then learn about
			// the reason.
			//
			// Required, unlike on rc run, because a hold is the one lease
			// with nothing to show for itself. A job's command says what it
			// is doing; a hold runs a sleeper, so without this rc devices
			// shows a bare submitter name and a held box is indistinguishable
			// from a forgotten one. On this fleet holds took ~98% of all
			// leased GPU time and 3 of the first 7 carried no reason at all.
			if strings.TrimSpace(reason) == "" {
				return errors.New("--reason is required: say why you are holding the device, " +
					"because a hold with no reason cannot be told apart from a leak")
			}
			if ttl <= 0 {
				return errors.New("--ttl is required")
			}

			target := device
			if target == "" {
				target = selector
			}

			submitter := as
			if submitter == "" {
				submitter = defaultSubmitter()
			}

			stderr := cmd.ErrOrStderr()

			// Like rc run, Ctrl-C/SIGTERM interrupt this context. Unlike rc
			// run, a hold acts on it even once granted — see the
			// WaitTerminal branch below.
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			c := client.New(controllerURL(), controllerToken())

			// Hold never sets NoWait — a hold always blocks in the queue
			// rather than failing fast on a busy device — so unlike rc run
			// there is no client.ErrNoDevice case to handle here: the
			// server can only ever return that 409 in response to NoWait.
			job, err := c.Hold(ctx, client.HoldOptions{
				DeviceID:  device,
				Selector:  selector,
				Submitter: submitter,
				Reason:    reason,
				TTL:       ttl,
			})
			if err != nil {
				return err
			}

			if job.State == model.JobQueued {
				scheduled, err := c.WaitScheduled(ctx, job.ID, func(pos int) {
					fmt.Fprintf(stderr, "rc: queued at position %d for %s\n", pos, target)
				})
				switch {
				case err == nil:
					job = scheduled
				case ctx.Err() != nil:
					// Nothing granted yet: there is no lease to protect, so
					// cancel outright, exactly like rc run does for a job
					// that is still queued when Ctrl-C lands.
					cancelQueuedJob(c, stop, stderr, job, submitter)
					return exitCodeError{code: sigintExitCode}
				default:
					return fmt.Errorf(
						"lost track of hold %s while it was queued (it may still be queued, or already granted — check `rc ps`): %w",
						job.ID, err)
				}

				if job.State.Terminal() && job.StartedAt == nil {
					killReason := job.KillReason
					if killReason == "" {
						killReason = "no further detail available"
					}
					return fmt.Errorf("hold %s was %s while queued and never granted on %s: %s",
						job.ID, job.State, job.DeviceID, killReason)
				}
			}

			if job.State.Terminal() {
				return fmt.Errorf("hold %s is already %s", job.ID, job.State)
			}

			// started_at is set the moment the controller hands the job out
			// (store.MarkRunning at assignment time — see handleAssignments),
			// which can lag a moment behind Submit returning if no worker
			// has polled yet; falling back to "now" keeps this an honest
			// estimate either way rather than blocking on a poll.
			expiresAt := time.Now().Add(ttl)
			if job.StartedAt != nil {
				expiresAt = job.StartedAt.Add(ttl)
			}
			fmt.Fprintf(stderr, "rc: hold %s granted on %s, expires around %s\n",
				job.ID, job.DeviceID, expiresAt.Format(time.RFC3339))
			fmt.Fprintf(stderr, "rc: end it early with `rc release %s`, or Ctrl-C here\n", job.ID)

			final, err := c.WaitTerminal(ctx, job.ID, 0)
			if ctx.Err() != nil {
				// Granted, and a human is here pressing Ctrl-C: release it.
				// This is the one place rc hold's Ctrl-C handling is the
				// opposite of rc run's — see the doc comment above.
				stop()
				relCtx, cancel := context.WithTimeout(context.Background(), killTimeout)
				defer cancel()
				if err := c.Release(relCtx, job.ID, submitter); err != nil {
					fmt.Fprintf(stderr, "rc: could not release hold %s: %v\n", job.ID, err)
					return exitCodeError{code: sigintExitCode}
				}
				fmt.Fprintf(stderr, "rc: released hold %s\n", job.ID)
				return exitCodeError{code: sigintExitCode}
			}
			if err != nil {
				return err
			}

			endReason := final.KillReason
			if endReason == "" {
				endReason = string(final.State)
			}
			fmt.Fprintf(stderr, "rc: hold %s ended: %s\n", final.ID, endReason)
			return nil
		},
	}

	cmd.Flags().StringVar(&selector, "select", "", "device selector, e.g. vram>=40G (mutually exclusive with a positional device ID)")
	cmd.Flags().DurationVar(&ttl, "ttl", 0, "how long to hold the device — required, capped by the device's max_runtime")
	cmd.Flags().StringVar(&reason, "reason", "", "why you're holding it, shown in rc devices and the dashboard (required)")
	cmd.Flags().StringVar(&as, "as", "", "identity shown in rc ps (defaults to $RC_SUBMITTER, else user@host/session)")
	return cmd
}

// NewReleaseCmd ends a hold — or, for that matter, any job — early. It is a
// thin alias over rc kill's ownership-checked path (see
// (*client.Client).Release), named separately because "release" is the
// verb that matches what rc hold prints and what a human reaches for once
// they're done with a device, not "kill".
func NewReleaseCmd() *cobra.Command {
	var as string
	cmd := &cobra.Command{
		Use:   "release <job-id>",
		Short: "End a hold (or any job) early",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			submitter := as
			if submitter == "" {
				submitter = defaultSubmitter()
			}
			c := client.New(controllerURL(), controllerToken())
			if err := c.Release(cmd.Context(), args[0], submitter); err != nil {
				return err
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "rc: released %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&as, "as", "", "identity to authorise the release (defaults to $RC_SUBMITTER, else user@host/session)")
	return cmd
}
