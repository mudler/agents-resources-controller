package cli

import (
	"fmt"
	"os"

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
			fmt.Fprintf(os.Stderr, "rc: kill requested for %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&as, "as", "", "identity to authorise the kill (defaults to user@host/session)")
	return cmd
}

// NewAttachCmd re-streams a job's output from the beginning. It is read-only:
// unlike `rc run`, exiting attach (Ctrl-C or otherwise) never affects the
// job — there is no lease to protect and nothing to detach from, since
// attach never held anything in the first place.
func NewAttachCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "attach <job-id>",
		Short: "Re-stream a running job's output from the beginning",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.New(controllerURL(), controllerToken())
			return c.StreamLogs(cmd.Context(), args[0], os.Stdout)
		},
	}
}
