package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// TestServeExitsPromptlyWhenListenFails guards against rc serve hanging
// silently on a bind failure. Before the fix, the reaper goroutine only ever
// learned to stop from cmd.Context() being cancelled by a signal; when
// ListenAndServe fails immediately (e.g. the port is already in use), RunE
// tries to return but blocks forever inside the deferred reaperWG.Wait(),
// since nothing else would ever cancel that context. Under systemd this
// means the unit never exits and Restart= never fires — a bind conflict
// looks like a hang, not a failure.
func TestServeExitsPromptlyWhenListenFails(t *testing.T) {
	// Occupy a real port first so rc serve's own bind fails the same way a
	// genuine port conflict would.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	t.Setenv("RC_TOKENS", "ctok:client")
	dataDir := t.TempDir()

	cmd := cli.NewServeCmd()
	cmd.SetArgs([]string{"--addr", addr, "--data", dataDir})
	cmd.SetContext(context.Background())
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "address already in use")
	case <-time.After(5 * time.Second):
		t.Fatal("rc serve did not return promptly when the listen address was already in use — it hung instead")
	}
}

// TestSchedulerLoopAssignsQueuedJobWhenDeviceFrees is the one piece of
// coverage for rc serve's own scheduler goroutine — as opposed to
// handleSubmit's one-shot ScheduleOnce call, which every other test in this
// package and internal/server exercises plenty. Nothing here ever calls
// ScheduleOnce directly or reaches into cmd/RunE's internals (there is
// nothing exported to reach): job A's device is freed the way a real worker
// frees it — a POST to /v1/jobs/{id}/status reporting it terminal — and the
// test then only polls the public HTTP surface, waiting for the queued job
// to move to assigned. That can only happen if the ticking scheduler
// goroutine inside rc serve actually ran st.ScheduleOnce() on its own,
// unprompted by anything this test called. If a future change breaks or
// removes that goroutine, this is the test that goes red for it.
func TestSchedulerLoopAssignsQueuedJobWhenDeviceFrees(t *testing.T) {
	// Reserve a free port, then release it immediately so rc serve can bind
	// it in turn. This is the same trade-off net/http/httptest itself makes
	// picking a port; the reverse of TestServeExitsPromptlyWhenListenFails
	// above, which deliberately keeps its listener open to force a conflict.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	t.Setenv("RC_TOKENS", "wtok:worker,ctok:client")
	dataDir := t.TempDir()

	cmd := cli.NewServeCmd()
	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--addr", addr, "--data", dataDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("rc serve did not shut down after its context was cancelled")
		}
	})

	baseURL := "http://" + addr
	cl := client.New(baseURL, "ctok")

	require.Eventually(t, func() bool {
		_, err := cl.State(context.Background())
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "rc serve never came up")

	workerID := registerRawWorker(t, baseURL, "wtok", "schedbox", []string{"gpu0"})

	jobA, err := cl.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "schedbox:gpu0", Command: []string{"true"}, Submitter: "agent-a",
	})
	require.NoError(t, err)
	require.Equal(t, model.JobAssigned, jobA.State, "the free device is handed out on submit")

	jobB, err := cl.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "schedbox:gpu0", Command: []string{"true"}, Submitter: "agent-b",
	})
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, jobB.State)

	// Free A's device exactly the way a real worker does — nothing here
	// calls into the store or the scheduler directly.
	reportRawStatus(t, baseURL, "wtok", jobA.ID, workerID, model.JobSucceeded)

	require.Eventually(t, func() bool {
		j, err := cl.Job(context.Background(), jobB.ID)
		return err == nil && j.State == model.JobAssigned
	}, 5*time.Second, 100*time.Millisecond,
		"rc serve's scheduler loop never assigned the queued job once its device freed")
}

func registerRawWorker(t *testing.T, baseURL, token, host string, devices []string) string {
	t.Helper()
	specs := make([]server.DeviceSpec, 0, len(devices))
	for _, name := range devices {
		specs = append(specs, server.DeviceSpec{Name: name})
	}
	payload, err := json.Marshal(server.RegisterRequest{Host: host, Devices: specs})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/workers/register", bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out.WorkerID
}

func reportRawStatus(t *testing.T, baseURL, token, jobID, workerID string, state model.JobState) {
	t.Helper()
	code := 0
	payload, err := json.Marshal(server.StatusRequest{WorkerID: workerID, State: state, ExitCode: &code})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/jobs/"+jobID+"/status", bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
