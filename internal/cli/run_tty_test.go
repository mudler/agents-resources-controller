package cli_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// What `rc run --tty` puts on the wire. The attach itself needs a real
// terminal and is verified by hand (see the plan's Task 4); what a test can
// pin is that the submission asks for one at all, and that asking for a shell
// without naming a command still records a command on the job row — `rc ps`
// showing a blank for the one job someone is sitting inside would be the
// worst row to lose.
func TestRunTTYSubmitsAnInteractiveJobWithADefaultShell(t *testing.T) {
	var (
		mu  sync.Mutex
		got server.SubmitRequest
	)
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobSucceeded, ExitCode: intPtr(0)}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			mu.Lock()
			_ = json.NewDecoder(r.Body).Decode(&got)
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.URL.Path == "/v1/jobs/job1":
			_ = json.NewEncoder(w).Encode(server.JobView{Job: job})
		// The terminal attach: refused here, because this test has no
		// terminal to attach. rc run reports that and stops, which is all
		// this test needs — the submission has already happened.
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
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--tty"})
	_ = cmd.Execute()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, model.StdioTTY, got.Stdio,
		"--tty submitted an ordinary job: its output would go to the log store and no terminal would ever exist")
	require.NotEmpty(t, got.Command, "--tty with no command must still record what it is running")
}

// Without --tty nothing changes: the same submission carries no stdio mode at
// all, so every job that exists today keeps going to the log store.
func TestRunWithoutTTYAsksForNoStdioMode(t *testing.T) {
	var (
		mu  sync.Mutex
		got server.SubmitRequest
	)
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobSucceeded, ExitCode: intPtr(0)}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			mu.Lock()
			_ = json.NewDecoder(r.Body).Decode(&got)
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v1/jobs/job1":
			_ = json.NewEncoder(w).Encode(server.JobView{Job: job})
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
	require.NoError(t, cmd.Execute())

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, model.StdioLogs, got.Stdio)
	require.Equal(t, []string{"true"}, got.Command)
}
