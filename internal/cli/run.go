// Package cli holds the cobra commands.
package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/mudler/resource-controller/internal/client"
	"github.com/spf13/cobra"
)

// exitCodeError carries a job's exit status up to main so `rc run` exits with
// the code the job produced, keeping it drop-in for scripts and CI.
type exitCodeError struct{ code int }

func (e exitCodeError) Error() string { return fmt.Sprintf("job exited %d", e.code) }
func (e exitCodeError) ExitCode() int { return e.code }

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
			if as == "" {
				as = defaultSubmitter()
			}

			c := client.New(controllerURL(), controllerToken())

			// Cancelling the client cancels the job, matching `flock -c`.
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			job, err := c.Submit(ctx, client.SubmitOptions{
				DeviceID:       device,
				Command:        args,
				Cwd:            cwd,
				Submitter:      as,
				IdempotencyKey: uuid.NewString(),
			})
			if errors.Is(err, client.ErrNoDevice) {
				return fmt.Errorf("%s is busy (stage 1 has no queue — retry or pick another device)", device)
			}
			if err != nil {
				return err
			}

			fmt.Fprintf(os.Stderr, "rc: job %s on %s\n", job.ID, job.DeviceID)

			if err := c.StreamLogs(ctx, job.ID, os.Stdout); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "rc: log stream ended: %v\n", err)
			}

			// Use a fresh context: the job's final state is still worth
			// reporting even when the user just pressed Ctrl-C.
			final, err := c.WaitTerminal(context.Background(), job.ID)
			if err != nil {
				return err
			}
			if final.ExitCode != nil && *final.ExitCode != 0 {
				return exitCodeError{code: *final.ExitCode}
			}
			if final.State != "succeeded" {
				return fmt.Errorf("job %s: %s", final.State, final.KillReason)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&device, "device", "d", "", "device ID, e.g. gpubox:gpu0")
	cmd.Flags().StringVar(&cwd, "cwd", "", "working directory on the device host")
	cmd.Flags().StringVar(&as, "as", "", "identity shown in rc ps (defaults to user@host/session)")
	return cmd
}

func controllerURL() string {
	if v := os.Getenv("RC_CONTROLLER"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func controllerToken() string { return os.Getenv("RC_TOKEN") }
