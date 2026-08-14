package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/cli"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// describeServer stands up a stub controller answering exactly one describe
// response for gpubox:gpu0, so each test only has to build the response it
// cares about.
func describeServer(t *testing.T, resp server.DescribeResponse) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/devices/gpubox:gpu0/describe" {
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func runDescribe(t *testing.T, ts *httptest.Server, args ...string) (string, error) {
	t.Helper()
	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewDescribeCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs(append([]string{"gpubox:gpu0"}, args...))
	err := cmd.Execute()
	return out.String(), err
}

// TestDescribeRendersLabelWithSourceAndAge pins the design's own example
// verbatim: a label must show its value, its source, and a relative age
// computed from how long ago it was actually confirmed — not the moment
// describe happened to run.
func TestDescribeRendersLabelWithSourceAndAge(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Labels: []model.Label{
			{Key: "vram", Value: "80G", Source: model.SourceDetected, UpdatedAt: time.Now().Add(-4 * time.Minute)},
		},
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	require.Contains(t, out, "vram")
	require.Contains(t, out, "80G")
	require.Contains(t, out, "detected")
	require.Contains(t, out, "4m ago", "the label's age must be rendered as a relative age, not a raw timestamp")
}

// TestDescribeShowsConflictingDeclaredValueBesideDetected is the guard
// against exactly the failure mode the brief warns about: an operator's
// hand-declared vram figure that disagrees with what was actually detected
// must be visible, not silently dropped in favor of the "winning" value the
// scheduler uses.
func TestDescribeShowsConflictingDeclaredValueBesideDetected(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Labels: []model.Label{
			{Key: "vram", Value: "80G", Source: model.SourceDetected, UpdatedAt: time.Now().Add(-4 * time.Minute)},
			{Key: "vram", Value: "40G", Source: model.SourceDeclared, UpdatedAt: time.Now().Add(-9 * 24 * time.Hour)},
		},
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	require.Contains(t, out, "80G", "the detected value must be shown")
	require.Contains(t, out, "40G", "the conflicting declared value must be shown, not hidden behind the winning detected one")
	require.Contains(t, out, "declared")
	require.Contains(t, out, "detected")
}

// TestDescribeShowsStaleSheetAge pins the "a sheet last edited in March"
// scenario from the design: an old sheet's age must actually read as old,
// not be silently rounded away or omitted because it's inconvenient.
func TestDescribeShowsStaleSheetAge(t *testing.T) {
	ts := describeServer(t, server.DescribeResponse{
		Device:         model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		Sheet:          "shared rack A1, ask #infra before recabling",
		SheetUpdatedAt: time.Now().Add(-100 * 24 * time.Hour),
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	require.Contains(t, out, "shared rack A1")
	require.Contains(t, out, "ago", "the sheet's age must be shown, however old")
	require.NotContains(t, out, "4m ago", "a sheet edited months ago must not read as minutes old")
}

// TestDescribeJSONOutputParsesBackIntoDescribeResponse pins the machine-
// readable path: `-o json` must emit the response verbatim, round-trippable
// by any caller (an agent's own tooling) that wants the raw structure
// instead of the rendered text.
func TestDescribeJSONOutputParsesBackIntoDescribeResponse(t *testing.T) {
	want := server.DescribeResponse{
		Device:              model.Device{ID: "gpubox:gpu0", State: model.DeviceBusy},
		Holder:              "agent-a",
		JobID:               "job1",
		ElapsedSeconds:      42,
		HeartbeatAgeSeconds: 3,
		Labels: []model.Label{
			{Key: "vram", Value: "80G", Source: model.SourceDetected, UpdatedAt: time.Now().Add(-4 * time.Minute).Truncate(time.Second)},
		},
		Sheet:          "notes",
		SheetUpdatedAt: time.Now().Add(-time.Hour).Truncate(time.Second),
		RecentJobs: []model.Job{
			{ID: "job0", DeviceID: "gpubox:gpu0", State: model.JobSucceeded, Command: []string{"./bench"}},
		},
	}
	ts := describeServer(t, want)

	out, err := runDescribe(t, ts, "-o", "json")
	require.NoError(t, err)

	var got server.DescribeResponse
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Equal(t, want.Device, got.Device)
	require.Equal(t, want.Holder, got.Holder)
	require.Equal(t, want.JobID, got.JobID)
	require.Equal(t, want.ElapsedSeconds, got.ElapsedSeconds)
	require.Equal(t, want.HeartbeatAgeSeconds, got.HeartbeatAgeSeconds)
	require.Len(t, got.Labels, len(want.Labels))
	for i := range want.Labels {
		require.Equal(t, want.Labels[i].Key, got.Labels[i].Key)
		require.Equal(t, want.Labels[i].Value, got.Labels[i].Value)
		require.Equal(t, want.Labels[i].Source, got.Labels[i].Source)
		require.True(t, want.Labels[i].UpdatedAt.Equal(got.Labels[i].UpdatedAt))
	}
	require.Equal(t, want.Sheet, got.Sheet)
	require.True(t, want.SheetUpdatedAt.Equal(got.SheetUpdatedAt))
	require.Equal(t, want.RecentJobs, got.RecentJobs)
}

// TestDescribeShowsRecentJobsWithOutcomeAndDuration pins the job-history
// section: each recent job must show what happened to it and how long it
// ran, not just an ID nobody can act on.
func TestDescribeShowsRecentJobsWithOutcomeAndDuration(t *testing.T) {
	started := time.Now().Add(-90 * time.Second)
	finished := time.Now().Add(-30 * time.Second)
	ts := describeServer(t, server.DescribeResponse{
		Device: model.Device{ID: "gpubox:gpu0", State: model.DeviceReady},
		RecentJobs: []model.Job{
			{
				ID: "job-xyz", DeviceID: "gpubox:gpu0", State: model.JobFailed,
				Command: []string{"./train"}, StartedAt: &started, FinishedAt: &finished,
			},
		},
	})

	out, err := runDescribe(t, ts)
	require.NoError(t, err)
	require.Contains(t, out, "job-xyz")
	require.Contains(t, out, "failed")
	require.Contains(t, out, "1m0s", "the job's actual run duration must be shown")
}
