package cli

import (
	"fmt"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/spf13/cobra"
)

// NewClearCmd returns a quarantined device to the pool.
//
// The endpoint has existed since quarantine did, but only the dashboard ever
// called it, so clearing a device from a terminal meant hand-writing a curl
// with an admin token — which is exactly what happened on 2026-08-17 after a
// worker restart quarantined dgx:gpu0 mid-lease. Documentation told operators
// a human had to clear it and then gave them no command to do it with.
//
// Deliberately NOT reachable with a client token: a device is quarantined
// because something went wrong, and deciding the hardware is healthy again is
// an operator's call, not a queued job's.
func NewClearCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clear <device>",
		Short: "Return a quarantined device to the pool (admin)",
		Long: "Return a quarantined device to the pool. Needs an admin token.\n\n" +
			"Quarantine means a job left the device in an unknown state — pinned\n" +
			"VRAM, a failed verify probe, or a worker that died mid-lease. Clearing\n" +
			"it asserts the hardware is healthy again, so check first: `rc describe\n" +
			"<device>` prints why it was taken out of the pool.\n\n" +
			"Refused while a lease is still live, rather than contradicting the\n" +
			"lease table.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			c := client.New(controllerURL(), controllerToken())
			if err := c.Clear(cmd.Context(), id); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "cleared %s\n", id)
			return nil
		},
	}
	return cmd
}
