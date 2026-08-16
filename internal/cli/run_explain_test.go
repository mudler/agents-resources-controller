package cli_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// TestRunExplainPrintsMatchesAndNeverSubmits is the test the brief demands
// explicitly: one that fails if --explain ever submits. The stub controller
// records whether ANY request other than the explain lookup itself lands,
// which is the only way to actually prove nothing was submitted rather than
// merely that the reported job count was zero.
func TestRunExplainPrintsMatchesAndNeverSubmits(t *testing.T) {
	var otherRequest atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/explain" {
			require.Equal(t, "vram>=40G", r.URL.Query().Get("selector"))
			_ = json.NewEncoder(w).Encode(server.ExplainResponse{
				Selector:   "vram>=40G",
				Matching:   []string{"gpubox:gpu0", "gpubox:gpu1"},
				Free:       []string{"gpubox:gpu1"},
				QueueDepth: 3,
			})
			return
		}
		// Any other request — most of all POST /v1/jobs — must never happen.
		otherRequest.Store(true)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	stdout := &syncBuffer{}
	cmd.SetOut(stdout)
	cmd.SetArgs([]string{"--explain", "--select", "vram>=40G"})

	err := cmd.Execute()
	require.NoError(t, err)
	require.False(t, otherRequest.Load(), "rc run --explain must never issue any request beyond the explain lookup")

	out := stdout.String()
	require.Contains(t, out, "gpubox:gpu0")
	require.Contains(t, out, "gpubox:gpu1")

	// The "free" and "queue depth" lines are each asserted on their own
	// line, not via a bare substring: "1" (the free count) is also a
	// substring of the already-printed "gpubox:gpu1  free" row, so a
	// substring check on the whole output would pass even if the
	// free/queue-depth line were deleted entirely.
	var freeLine, queueLine string
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(line, "free:"):
			freeLine = line
		case strings.HasPrefix(line, "queue depth:"):
			queueLine = line
		}
	}
	require.Equal(t, "free: 1/2", freeLine)
	require.Equal(t, "queue depth: 3", queueLine)
}

// TestRunExplainRequiresSelect: --explain answers "what would this selector
// do", so it is meaningless without one, and the CLI must say so before
// touching the network.
func TestRunExplainRequiresSelect(t *testing.T) {
	var anyRequest atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		anyRequest.Store(true)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{"--explain"})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--select")
	require.False(t, anyRequest.Load())
}

// TestRunRejectsDeviceAndSelectTogether pins the design's explicit rule:
// giving both -d and --select is an error the CLI rejects before any
// request, naming both flags so the caller knows exactly what to fix.
func TestRunRejectsDeviceAndSelectTogether(t *testing.T) {
	var anyRequest atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		anyRequest.Store(true)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--select", "vram>=40G", "--", "true"})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--select")
	require.Contains(t, err.Error(), "-d")
	require.False(t, anyRequest.Load(), "the conflicting flags must be rejected before any request is made")
}

// TestRunSelectSubmitsWithSelectorInsteadOfDeviceID proves --select actually
// wires through to the submit request as a selector, not a device_id, and
// that the two are mutually exclusive at the wire level too.
func TestRunSelectSubmitsWithSelectorInsteadOfDeviceID(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobSucceeded, ExitCode: intPtr(0)}
	var gotSelector, gotDeviceID string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			var req server.SubmitRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			gotSelector = req.Selector
			gotDeviceID = req.DeviceID
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
	cmd.SetErr(&syncBuffer{})
	cmd.SetArgs([]string{"--select", "vram>=40G", "--", "true"})

	err := cmd.Execute()
	require.NoError(t, err)
	require.Equal(t, "vram>=40G", gotSelector)
	require.Empty(t, gotDeviceID, "a selector submit must not also carry a device_id")
}
