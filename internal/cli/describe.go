package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
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
	renderLabels(tw, out.Labels, out.LabelAgeSeconds)

	fmt.Fprintln(tw)
	fmt.Fprintln(tw, "USAGE SHEET")
	renderSheet(tw, out.Sheet, out.SheetAgeSeconds, out.SheetIsHostWide)

	fmt.Fprintln(tw)
	fmt.Fprintln(tw, "RECENT JOBS")
	// Job history has no server-computed age field of its own: a still-
	// running job's displayed duration is bounded by "now" only for the
	// UI's benefit (it has no FinishedAt yet), not presented as a fact whose
	// staleness an agent must trust the way a label's or sheet's age is.
	renderRecentJobs(tw, out.RecentJobs, time.Now())

	return tw.Flush()
}

// unknownAge is what renders in place of a computed duration when the
// controller reported no age at all for something that does exist — a map
// miss for a label, or a nil SheetAgeSeconds for a non-empty sheet. That
// combination means an older controller answered a newer CLI's request
// (this field did not exist on the wire yet), not that the fact is fresh:
// a Go map miss and a nil *int's zero value are indistinguishable from a
// real zero without checking presence first, and rendering that as "0s
// ago" would recreate this task's own defect — a stale fact reading as
// current — with version skew standing in for clock skew.
const unknownAge = "age unknown (controller may be out of date)"

// renderLabels renders each label's age from ages, keyed by
// key+"/"+source exactly as DescribeResponse.LabelAgeSeconds documents —
// never by re-deriving it from the label's own UpdatedAt against this
// process's clock, which is the whole defect this field exists to fix.
// Looked up with the two-value map form specifically: ages[key] alone
// cannot tell "the controller said 0" from "the controller said nothing",
// and a plain lookup would render the latter as the former — see unknownAge.
func renderLabels(w io.Writer, labels []model.Label, ages map[string]int) {
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
		ageText := unknownAge
		if age, ok := ages[l.Key+"/"+l.Source]; ok {
			ageText = formatAge(time.Duration(age) * time.Second)
		}
		fmt.Fprintf(w, "  %s=%s\t%s %s\n", l.Key, l.Value, l.Source, ageText)
	}
}

// renderSheet renders the sheet's age from ageSeconds — the controller's
// own computation, per the same rule renderLabels follows — never from
// now.Sub(updatedAt) against this process's clock.
//
// The "nothing on file" guard is on body alone, not on any timestamp: a
// real worker's registration almost never leaves a sheet's timestamp at the
// zero value even when there is no host.md to read (see handleDescribe's
// own host-wide-fallback comment for why), so gating on a zero timestamp —
// as an earlier version of this function did — left the guard untested by
// anything a real worker produces and, in production, never fired: every
// host simply lacking a host.md rendered a phantom "host-wide note, updated
// 1h ago" followed by a blank line instead of "(none on file)". An empty
// body has nothing to show regardless of what timestamp got attached to
// recording that emptiness, so it alone is the right and sufficient signal.
func renderSheet(w io.Writer, body string, ageSeconds *int, hostWide bool) {
	if body == "" {
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
	ageText := unknownAge
	if ageSeconds != nil {
		ageText = formatAge(time.Duration(*ageSeconds) * time.Second)
	}
	fmt.Fprintf(w, "  %s, updated %s\n", scope, ageText)
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

// ageMagnitude renders a non-negative duration's coarse size — "5m", "3h",
// "90d" — with no directional word attached. It exists only so formatAge's
// "ago" and "in the future" phrasings share one ladder and can never drift
// out of step with each other (a bug fixed here: an earlier version of
// formatAge built the future case by appending " in the future (clock
// skew?)" onto its OWN "...ago" output, producing the self-contradictory
// "5m ago in the future (clock skew?)" — that string was never itself
// asserted by any test, only checked with Contains("... ago"), which the
// broken phrasing still satisfied).
func ageMagnitude(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	case d < 30*24*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	default:
		return fmt.Sprintf("%dmo", int(d.Hours()/24/30))
	}
}

// formatAge renders a duration the way an agent deciding whether to trust a
// fact needs to see it: coarse enough to read at a glance, but never
// omitted — a label unconfirmed for a week and a sheet last edited months
// ago are exactly the ages that must not be rounded away or hidden.
//
// A negative duration means the timestamp is AHEAD of the clock that
// produced the age — normally impossible, reachable when a worker's clock
// runs fast relative to the controller's. Clamping that to zero, as an
// earlier version of this function did, would render it as "0s ago": the
// freshest possible reading, when a future-stamped fact is actually the
// LEAST trustworthy one on the page. So it is reported as what it is
// instead of being silently laundered into looking current — as
// "<magnitude> in the future (clock skew?)", never with "ago" anywhere in
// that phrase.
func formatAge(d time.Duration) string {
	if d < 0 {
		return fmt.Sprintf("%s in the future (clock skew?)", ageMagnitude(-d))
	}
	return ageMagnitude(d) + " ago"
}
