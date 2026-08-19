package cli

import (
	"github.com/mudler/resource-controller/internal/client"
	"github.com/spf13/cobra"
)

// NewLogsCmd reads output that the controller stored for a job. By default it
// returns the current snapshot; --follow replays that snapshot and then waits
// for new output until the job finishes.
func NewLogsCmd() *cobra.Command {
	var follow bool
	cmd := &cobra.Command{
		Use:   "logs <job-id>",
		Short: "Read stored job output",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			err := client.New(controllerURL(), controllerToken()).Logs(ctx, args[0], cmd.OutOrStdout(), follow)
			if err != nil && follow && ctx.Err() != nil {
				return exitCodeError{code: sigintExitCode}
			}
			return err
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "replay stored output and follow until completion")
	return cmd
}
