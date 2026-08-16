package cli

import (
	"fmt"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/spf13/cobra"
)

// NewRetireCmd removes a device from the fleet: the card was pulled, the box
// was decommissioned. It is the counterpart to a worker registering one, and
// it exists because the only alternative was editing the controller's
// database by hand.
func NewRetireCmd() *cobra.Command {
	var yes bool

	cmd := &cobra.Command{
		Use:   "retire <device>",
		Short: "Remove a device from the fleet for good (admin)",
		Long: "Remove a device from the fleet for good — the card was pulled, or the\n" +
			"box was decommissioned. Needs an admin token, and is refused while\n" +
			"anything holds the device.\n\n" +
			"Job history is kept: what ran on a device stays answerable after the\n" +
			"device is gone, which is when it is usually asked.\n\n" +
			"If a worker still declares this device, it comes back on that worker's\n" +
			"next registration — remove it from the worker's config first.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]

			// Deleting inventory is not something to do on a typo, and unlike
			// a kill there is no queue position or exit code to undo it from.
			if !yes {
				fmt.Fprintf(cmd.ErrOrStderr(),
					"This removes %s from the fleet. Re-run with --yes to confirm.\n", id)
				return fmt.Errorf("not confirmed")
			}

			c := client.New(controllerURL(), controllerToken())
			note, err := c.Retire(cmd.Context(), id)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "retired %s\n", id)
			if note != "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "note: %s\n", note)
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&yes, "yes", false, "confirm the removal (required)")
	return cmd
}
