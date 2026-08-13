package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/spf13/cobra"
)

// RenderDevices prints the fleet as a table. Heartbeat age is shown for any
// device whose worker has gone quiet for longer than HeartbeatGrace,
// regardless of the device's own state — a device can be marked unhealthy,
// have the network partition heal, and start heartbeating again well before
// anyone runs `rc devices clear`; annotating it "no contact 0s" forever in
// that window would contradict the freshest information this command has.
//
// It returns tabwriter's Flush error rather than discarding it: tabwriter
// buffers every row until Flush, so on a broken pipe (`rc devices | head`)
// or a full disk the table can be silently dropped while this function (and
// so `rc devices`) would otherwise still report success.
func RenderDevices(w io.Writer, views []server.DeviceView) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "DEVICE\tSTATE\tHOLDER\tELAPSED\tCOMMAND")

	for _, v := range views {
		state := string(v.Device.State)
		age := time.Duration(v.HeartbeatAgeSeconds) * time.Second
		if age > HeartbeatGrace {
			state = fmt.Sprintf("%s (no contact %s)", state, age)
		}

		holder, elapsed, command := "-", "-", "-"
		if v.Holder != "" {
			holder = v.Holder
			elapsed = (time.Duration(v.ElapsedSeconds) * time.Second).String()
		}
		if len(v.Command) > 0 {
			command = strings.Join(v.Command, " ")
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", v.Device.ID, state, holder, elapsed, command)
	}
	return tw.Flush()
}

func NewDevicesCmd() *cobra.Command {
	var asJSON bool

	cmd := &cobra.Command{
		Use:   "devices",
		Short: "List devices, their state, and who holds them",
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.New(controllerURL(), controllerToken())
			state, err := c.State(cmd.Context())
			if err != nil {
				return err
			}
			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(state.Devices)
			}
			return RenderDevices(cmd.OutOrStdout(), state.Devices)
		},
	}
	cmd.Flags().BoolVarP(&asJSON, "json", "o", false, "output JSON")
	return cmd
}

func NewPsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ps",
		Short: "Show running jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.New(controllerURL(), controllerToken())
			state, err := c.State(cmd.Context())
			if err != nil {
				return err
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "JOB\tDEVICE\tSTATE\tSUBMITTER\tCOMMAND")
			for _, j := range state.Jobs {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
					j.ID, j.DeviceID, j.State, j.Submitter, strings.Join(j.Command, " "))
			}
			return tw.Flush()
		},
	}
}
