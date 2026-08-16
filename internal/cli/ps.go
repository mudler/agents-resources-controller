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
			// A hold's reason rides the lease row precisely so it can be
			// shown here instead of a bare holder name next to a
			// meaningless "hold" command — see assignQueued's doc comment
			// in internal/store/allocate.go.
			if v.Reason != "" {
				holder = fmt.Sprintf("%s (%s)", holder, v.Reason)
			}
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
	var selector string

	cmd := &cobra.Command{
		Use:   "devices",
		Short: "List devices, their state, and who holds them",
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.New(controllerURL(), controllerToken())
			state, err := c.State(cmd.Context())
			if err != nil {
				return err
			}

			devices := state.Devices
			if selector != "" {
				// Filtering happens here, client-side, but the matching
				// itself is not reimplemented: it comes straight from
				// /v1/explain, the exact same store.MatchingDevices the
				// scheduler uses, so this table can never disagree with what
				// a real --select submit would actually match.
				explain, err := c.Explain(cmd.Context(), selector)
				if err != nil {
					return err
				}
				matching := make(map[string]bool, len(explain.Matching))
				for _, id := range explain.Matching {
					matching[id] = true
				}
				filtered := make([]server.DeviceView, 0, len(devices))
				for _, v := range devices {
					if matching[v.Device.ID] {
						filtered = append(filtered, v)
					}
				}
				devices = filtered
			}

			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(devices)
			}
			return RenderDevices(cmd.OutOrStdout(), devices)
		},
	}
	cmd.Flags().BoolVarP(&asJSON, "json", "o", false, "output JSON")
	cmd.Flags().StringVar(&selector, "select", "", "filter to devices matching this selector, e.g. vram>=40G")
	return cmd
}

// RenderJobs prints the active jobs followed by the queue. Queued jobs are
// listed because `rc ps` is the only way to find a job's ID, and `rc kill`
// (which the README tells operators to reach for) needs one: a queued job
// nobody can name is a job nobody can cancel except by waiting for it to
// start first.
//
// Position is computed here rather than fetched per job: state.Queued already
// arrives in scheduling order (priority, then FIFO), so counting along it per
// device gives the same 1-based, per-device position the controller reports
// for a single job — without one HTTP request per queued job.
//
// It returns tabwriter's Flush error for the same reason RenderDevices does:
// the whole table is buffered until Flush, so a discarded error means a
// silently empty `rc ps` on a broken pipe.
func RenderJobs(w io.Writer, state *server.StateResponse) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB\tDEVICE\tSTATE\tSUBMITTER\tCOMMAND")
	for _, j := range state.Jobs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			j.ID, j.DeviceID, j.State, j.Submitter, strings.Join(j.Command, " "))
	}

	position := map[string]int{}
	for _, j := range state.Queued {
		position[j.DeviceID]++
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			j.ID, j.DeviceID, fmt.Sprintf("queued (#%d)", position[j.DeviceID]),
			j.Submitter, strings.Join(j.Command, " "))
	}
	return tw.Flush()
}

func NewPsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ps",
		Short: "Show running and queued jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.New(controllerURL(), controllerToken())
			state, err := c.State(cmd.Context())
			if err != nil {
				return err
			}
			return RenderJobs(cmd.OutOrStdout(), state)
		},
	}
}
