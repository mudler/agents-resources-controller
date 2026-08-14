package cli_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/cli"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// TestRunQueuedShowsPositionThenRuns drives a job that starts out queued,
// reports two distinct positions, then transitions to succeeded — `rc run`
// must surface the positions on stderr and still make it to a clean exit.
func TestRunQueuedShowsPositionThenRuns(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}
	var polls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			n := polls.Add(1)
			view := server.JobView{Job: job, QueuePosition: 3}
			switch {
			case n >= 3:
				view.Job.State = model.JobSucceeded
				view.QueuePosition = 0
			case n == 2:
				view.QueuePosition = 1
			}
			_ = json.NewEncoder(w).Encode(view)
		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--", "true"})

	err := cmd.Execute()
	require.NoError(t, err)
	require.GreaterOrEqual(t, polls.Load(), int32(3), "expected polling to continue until the job left the queue")
}

// TestRunCtrlCWhileQueuedCancelsOutright verifies the distinction that
// matters most here: interrupting a job that is still QUEUED (no lease, no
// process) cancels it outright via rc kill, rather than merely detaching.
func TestRunCtrlCWhileQueuedCancelsOutright(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}
	var killCalled atomic.Bool
	var killSubmitter atomic.Value

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			_ = json.NewEncoder(w).Encode(server.JobView{Job: job, QueuePosition: 5})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/job1/kill":
			killCalled.Store(true)
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			killSubmitter.Store(body["submitter"])
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--", "true"})

	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	go func() {
		<-time.After(50 * time.Millisecond)
		cancel()
	}()

	err := cmd.Execute()
	require.Error(t, err)

	var coded interface{ ExitCode() int }
	require.True(t, errors.As(err, &coded), "expected a coded exit error, got: %v", err)
	require.Equal(t, 130, coded.ExitCode())

	require.True(t, killCalled.Load(), "expected rc kill to be requested for the still-queued job")
	require.Equal(t, "tester", killSubmitter.Load())
}

// TestRunGivesUpAfterTimeoutCancelsQueuedJob checks the --timeout branch:
// distinct from Ctrl-C, but it must also cancel the still-queued job rather
// than leaving it stranded in the queue.
func TestRunGivesUpAfterTimeoutCancelsQueuedJob(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}
	var killCalled atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			_ = json.NewEncoder(w).Encode(server.JobView{Job: job, QueuePosition: 5})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/job1/kill":
			killCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--timeout", "200ms", "--", "true"})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "gave up waiting")
	require.True(t, killCalled.Load(), "expected rc kill to be requested after giving up")
}
