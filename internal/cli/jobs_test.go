package cli_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

func TestRenderJobHistoryFormatsRowsAndAllMissingValues(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	started := time.Date(2026, 8, 19, 11, 57, 57, 600_000_000, time.FixedZone("west", -7*60*60))
	finished := started.Add(2*time.Minute + 3*time.Second + 600*time.Millisecond)
	var out bytes.Buffer

	err := cli.RenderJobHistory(&out, []model.Job{
		{ID: "job1", DeviceID: "gpu0", State: model.JobKilled, KillReason: "idle timeout", Submitter: "alice", StartedAt: &started, FinishedAt: &finished, Command: []string{"sh", "-c", "echo hi"}},
		{},
	}, now)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 3)
	require.Contains(t, lines[0], "JOB")
	require.Contains(t, lines[1], "2026-08-19T18:57:57Z")
	require.Contains(t, lines[1], "2m4s")
	require.Contains(t, lines[1], "killed (idle timeout)")
	require.Contains(t, lines[1], "sh -c echo hi")
	require.Equal(t, "-  -  -  -  -  -  -", strings.Join(strings.Fields(lines[2]), "  "), "every missing displayed value, including JOB, must use the placeholder")
}

func TestRenderJobHistoryPropagatesWriteError(t *testing.T) {
	wantErr := errors.New("disk full")
	require.ErrorIs(t, cli.RenderJobHistory(failWriter{wantErr}, []model.Job{{ID: "job1"}}, time.Now()), wantErr)
}

func TestJobsCommandPassesFlagsAndWritesJSON(t *testing.T) {
	queryCh := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queryCh <- r.URL.RawQuery
		_ = json.NewEncoder(w).Encode(server.JobsResponse{Jobs: []model.Job{{ID: "job1", State: model.JobSucceeded}}})
	}))
	defer ts.Close()
	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewJobsCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"-n", "5", "--device", "gpu0", "--submitter", "alice", "--state", "succeeded", "-o", "json"})
	require.NoError(t, cmd.Execute())
	require.Equal(t, "device=gpu0&limit=5&state=succeeded&submitter=alice", <-queryCh)
	var jobs []model.Job
	require.NoError(t, json.Unmarshal(out.Bytes(), &jobs))
	require.Equal(t, "job1", jobs[0].ID)
}

func TestJobsCommandRejectsUnknownOutputLocally(t *testing.T) {
	called := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer ts.Close()
	t.Setenv("RC_CONTROLLER", ts.URL)

	cmd := cli.NewJobsCmd()
	cmd.SetArgs([]string{"--output", "yaml"})
	err := cmd.Execute()
	require.ErrorContains(t, err, "unknown output format")
	require.False(t, called)
}
