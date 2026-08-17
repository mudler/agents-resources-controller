package server_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// A job's stdio mode has to survive the whole trip — submit, the jobs table,
// and the assignment the worker polls for — or the relay has nowhere to
// attach: the worker is the side that dials out, so it only ever learns "this
// one is interactive" from the assignment it is handed.
func TestStdioModeReachesTheAssignment(t *testing.T) {
	for _, mode := range []string{model.StdioTTY, model.StdioPipe} {
		t.Run(mode, func(t *testing.T) {
			ts, _, _, _ := newServer(t)
			workerID := registerWorker(t, ts)

			resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
				DeviceID: "gpubox:gpu0", Command: []string{"/bin/sh"},
				Submitter: "agent-a", Stdio: mode,
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusCreated, resp.StatusCode)
			var job model.Job
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&job))
			require.Equal(t, mode, job.Stdio, "the stored job lost its stdio mode")

			resp2 := poll(t, ts, workerID, "2s")
			defer resp2.Body.Close()
			require.Equal(t, http.StatusOK, resp2.StatusCode)
			var pr server.PollResponse
			require.NoError(t, json.NewDecoder(resp2.Body).Decode(&pr))
			require.Len(t, pr.Assignments, 1)
			require.Equal(t, mode, pr.Assignments[0].Stdio,
				"the assignment lost its stdio mode, so the worker would run this job with its stdio in the log store and nothing would ever attach")
		})
	}
}

// An unknown mode is refused rather than treated as the default. Silently
// falling back would give the caller a job whose stdio goes to the log store
// while it sits waiting for a terminal that will never exist.
func TestUnknownStdioModeIsRefused(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"/bin/sh"},
		Submitter: "agent-a", Stdio: "websocket",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body struct{ Message string }
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Contains(t, body.Message, "websocket")
}

// A hold's process is the worker's own sleeper, chosen by the worker and not
// by the submitter — the same rule that already refuses a hold carrying a
// command, cwd or env. Attaching a terminal to it would be attaching to
// something the submitter never asked to run.
func TestAHoldMayNotAskForATerminal(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Submitter: "agent-a",
		Kind: model.LeaseKindHold, MaxRuntimeSeconds: 60, Stdio: model.StdioTTY,
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
