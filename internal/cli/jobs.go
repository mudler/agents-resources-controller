package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/spf13/cobra"
)

func NewJobsCmd() *cobra.Command {
	var opts client.JobsOptions
	var state string
	var output string

	cmd := &cobra.Command{
		Use:   "jobs",
		Short: "Show job history",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			format := strings.ToLower(output)
			if format != "text" && format != "json" {
				return fmt.Errorf("unknown output format %q (want text or json)", output)
			}
			opts.State = model.JobState(state)
			jobs, err := client.New(controllerURL(), controllerToken()).Jobs(cmd.Context(), opts)
			if err != nil {
				return err
			}
			if format == "json" {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(jobs)
			}
			return RenderJobHistory(cmd.OutOrStdout(), jobs, time.Now())
		},
	}
	cmd.Flags().IntVarP(&opts.Limit, "limit", "n", 20, "maximum number of jobs")
	cmd.Flags().StringVar(&opts.DeviceID, "device", "", "filter by device ID")
	cmd.Flags().StringVar(&opts.Submitter, "submitter", "", "filter by submitter")
	cmd.Flags().StringVar(&state, "state", "", "filter by job state")
	cmd.Flags().StringVarP(&output, "output", "o", "text", `output format: "text" or "json"`)
	return cmd
}

func RenderJobHistory(w io.Writer, jobs []model.Job, now time.Time) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB\tDEVICE\tSTATE\tSUBMITTER\tSTARTED\tDURATION\tCOMMAND")
	for _, job := range jobs {
		state := valueOrDash(string(job.State))
		if job.KillReason != "" {
			state += " (" + job.KillReason + ")"
		}
		started, duration := "-", "-"
		if job.StartedAt != nil {
			started = job.StartedAt.UTC().Format(time.RFC3339)
			end := now
			if job.FinishedAt != nil {
				end = *job.FinishedAt
			}
			duration = end.Sub(*job.StartedAt).Round(time.Second).String()
		}
		command := valueOrDash(strings.Join(job.Command, " "))
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			valueOrDash(job.ID), valueOrDash(job.DeviceID), state,
			valueOrDash(job.Submitter), started, duration, command)
	}
	return tw.Flush()
}

func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
