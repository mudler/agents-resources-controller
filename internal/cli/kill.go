package cli

import (
	"fmt"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/spf13/cobra"
)

// NewKillCmd cancels a queued job outright, or asks the worker to terminate
// a running one. The controller enforces ownership: only the submitter (or
// an admin token) may kill a given job.
func NewKillCmd() *cobra.Command {
	var as string
	cmd := &cobra.Command{
		Use:   "kill <job-id>",
		Short: "Cancel a queued job or terminate a running one",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			submitter := as
			if submitter == "" {
				submitter = defaultSubmitter()
			}
			c := client.New(controllerURL(), controllerToken())
			if err := c.Kill(cmd.Context(), args[0], submitter); err != nil {
				return err
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "rc: kill requested for %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&as, "as", "", "identity to authorise the kill (defaults to user@host/session)")
	return cmd
}

// NewAttachCmd re-streams a job's output from the beginning. It is read-only:
// unlike `rc run`, exiting attach (Ctrl-C or otherwise) never affects the
// job — there is no lease to protect and nothing to detach from, since
// attach never held anything in the first place. Ctrl-C is therefore the
// normal, expected way to stop watching: it exits quietly with the
// conventional SIGINT status instead of surfacing the raw "context
// canceled" plumbing error that StreamLogs returns once its request is
// aborted.
func NewAttachCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "attach <job-id>",
		Short: "Re-stream a running job's output from the beginning",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			c := client.New(controllerURL(), controllerToken())
			err := c.StreamLogs(ctx, args[0], cmd.OutOrStdout())
			if err != nil && ctx.Err() != nil {
				return exitCodeError{code: sigintExitCode}
			}
			return err
		},
	}
}
