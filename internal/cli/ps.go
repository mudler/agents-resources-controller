package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/spf13/cobra"
)

// RenderDevices prints the fleet as a table. Heartbeat age is shown for any
// device that is not reporting, because a silent worker looks identical to a
// healthy one otherwise.
func RenderDevices(w io.Writer, views []server.DeviceView) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "DEVICE\tSTATE\tHOLDER\tELAPSED\tCOMMAND")

	for _, v := range views {
		state := string(v.Device.State)
		if v.Device.State != model.DeviceReady && v.Device.State != model.DeviceBusy {
			state = fmt.Sprintf("%s (no contact %s)", state,
				time.Duration(v.HeartbeatAgeSeconds)*time.Second)
		} else if v.HeartbeatAgeSeconds > 30 {
			state = fmt.Sprintf("%s (no contact %s)", state,
				time.Duration(v.HeartbeatAgeSeconds)*time.Second)
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
	tw.Flush()
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
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(state.Devices)
			}
			RenderDevices(os.Stdout, state.Devices)
			return nil
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
			tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "JOB\tDEVICE\tSTATE\tSUBMITTER\tCOMMAND")
			for _, j := range state.Jobs {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
					j.ID, j.DeviceID, j.State, j.Submitter, strings.Join(j.Command, " "))
			}
			return tw.Flush()
		},
	}
}
