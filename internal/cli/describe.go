package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/spf13/cobra"
)

// sourceRank orders a label's sources for display: detected first, since it
// is what the scheduler actually trusts (see store.LabelSnapshot), with
// declared right below it — close enough that a conflicting hand-entered
// value cannot be missed, never hidden behind the one that "won".
var sourceRank = map[string]int{model.SourceDetected: 0, model.SourceDeclared: 1}

func NewDescribeCmd() *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "describe <device-id>",
		Short: "Show everything known about a device before writing commands for it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.New(controllerURL(), controllerToken())
			resp, err := c.Describe(cmd.Context(), args[0])
			if err != nil {
				return err
			}

			if strings.EqualFold(output, "json") {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(resp)
			}
			return RenderDescribe(cmd.OutOrStdout(), resp)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "text", `output format: "text" or "json"`)
	return cmd
}

// RenderDescribe prints everything an agent needs to trust (or distrust) a
// device before writing commands for it: the device and its state and
// holder; every label grouped by key with its source and age, a conflicting
// value shown rather than hidden; the usage sheet and its age; and recent
// job history with outcome and duration.
//
// Every write goes through one tabwriter so a broken pipe or full disk on
// the final Flush is reported rather than silently dropped, matching
// RenderDevices and RenderJobs.
func RenderDescribe(w io.Writer, out *server.DescribeResponse) error {
	now := time.Now()
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)

	fmt.Fprintf(tw, "%s\t%s\n", out.Device.ID, out.Device.State)
	if out.Holder != "" {
		elapsed := (time.Duration(out.ElapsedSeconds) * time.Second).String()
		fmt.Fprintf(tw, "  held by %s\tjob %s, running %s\n", out.Holder, out.JobID, elapsed)
	} else {
		fmt.Fprintln(tw, "  free\t")
	}
	fmt.Fprintf(tw, "  heartbeat\t%s\n", formatAge(time.Duration(out.HeartbeatAgeSeconds)*time.Second))

	fmt.Fprintln(tw)
	fmt.Fprintln(tw, "LABELS")
	renderLabels(tw, out.Labels, now)

	fmt.Fprintln(tw)
	fmt.Fprintln(tw, "USAGE SHEET")
	renderSheet(tw, out.Sheet, out.SheetUpdatedAt, out.SheetIsHostWide, now)

	fmt.Fprintln(tw)
	fmt.Fprintln(tw, "RECENT JOBS")
	renderRecentJobs(tw, out.RecentJobs, now)

	return tw.Flush()
}

func renderLabels(w io.Writer, labels []model.Label, now time.Time) {
	if len(labels) == 0 {
		fmt.Fprintln(w, "  (none)")
		return
	}
	// A defensive, stable sort: the server already returns key,source order,
	// but rendering must not depend on that for the display grouping and
	// detected-before-declared ordering it wants.
	sorted := append([]model.Label(nil), labels...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Key != sorted[j].Key {
			return sorted[i].Key < sorted[j].Key
		}
		return sourceRank[sorted[i].Source] < sourceRank[sorted[j].Source]
	})
	for _, l := range sorted {
		fmt.Fprintf(w, "  %s=%s\t%s %s\n", l.Key, l.Value, l.Source, formatAge(now.Sub(l.UpdatedAt)))
	}
}

func renderSheet(w io.Writer, body string, updatedAt time.Time, hostWide bool, now time.Time) {
	if body == "" && updatedAt.IsZero() {
		fmt.Fprintln(w, "  (none on file)")
		return
	}
	// Which scope this note applies to matters as much as its age: an
	// agent reading "don't run more than two jobs here" needs to know
	// whether that's a rule for the whole box or just this card.
	scope := "device note"
	if hostWide {
		scope = "host-wide note"
	}
	fmt.Fprintf(w, "  %s, updated %s\n", scope, formatAge(now.Sub(updatedAt)))
	for _, line := range strings.Split(body, "\n") {
		fmt.Fprintf(w, "  %s\n", line)
	}
}

func renderRecentJobs(w io.Writer, jobs []model.Job, now time.Time) {
	if len(jobs) == 0 {
		fmt.Fprintln(w, "  (none)")
		return
	}
	fmt.Fprintln(w, "  JOB\tSTATE\tDURATION\tCOMMAND")
	for _, j := range jobs {
		duration := "-"
		if j.StartedAt != nil {
			end := now
			if j.FinishedAt != nil {
				end = *j.FinishedAt
			}
			duration = end.Sub(*j.StartedAt).Round(time.Second).String()
		}
		state := string(j.State)
		if j.KillReason != "" {
			state = fmt.Sprintf("%s (%s)", state, j.KillReason)
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", j.ID, state, duration, strings.Join(j.Command, " "))
	}
}

// formatAge renders a duration the way an agent deciding whether to trust a
// fact needs to see it: coarse enough to read at a glance, but never
// omitted — a label unconfirmed for a week and a sheet last edited months
// ago are exactly the ages that must not be rounded away or hidden.
func formatAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	case d < 30*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	default:
		return fmt.Sprintf("%dmo ago", int(d.Hours()/24/30))
	}
}
