package cli_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/stretchr/testify/require"
)

func intPtr(i int) *int { return &i }

// TestRunExitsWithJobExitCode drives NewRunCmd end to end against a stub
// controller and checks that the job's own exit code survives as the
// error's ExitCode() — the property that lets `rc run` sit where `flock
// ... -c '...'` used to.
func TestRunExitsWithJobExitCode(t *testing.T) {
	cases := []struct {
		name      string
		state     model.JobState
		exitCode  *int
		wantErr   bool
		wantCoded bool
		wantCode  int
	}{
		{name: "succeeds with exit 0", state: model.JobSucceeded, exitCode: intPtr(0)},
		{name: "propagates exit 7", state: model.JobFailed, exitCode: intPtr(7), wantErr: true, wantCoded: true, wantCode: 7},
		{name: "propagates exit 143 (SIGTERM)", state: model.JobKilled, exitCode: intPtr(143), wantErr: true, wantCoded: true, wantCode: 143},
		{name: "killed with exit code 0 must not report success", state: model.JobKilled, exitCode: intPtr(0), wantErr: true, wantCoded: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: tc.state, ExitCode: tc.exitCode}

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
					w.WriteHeader(http.StatusCreated)
					_ = json.NewEncoder(w).Encode(job)
				case r.URL.Path == "/v1/jobs/job1/logs":
					w.WriteHeader(http.StatusOK)
				case r.URL.Path == "/v1/jobs/job1":
					_ = json.NewEncoder(w).Encode(job)
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

			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)

			var coded interface{ ExitCode() int }
			if tc.wantCoded {
				require.True(t, errors.As(err, &coded), "expected a coded exit error, got: %v", err)
				require.Equal(t, tc.wantCode, coded.ExitCode())
			} else {
				require.False(t, errors.As(err, &coded), "expected an un-coded error (main falls back to exit 1), got: %v", err)
			}
		})
	}
}
