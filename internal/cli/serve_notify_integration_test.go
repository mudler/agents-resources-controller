package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/cli"
	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// TestJobsStillScheduleWhileTheWebhookHangs is the property the whole
// notifier exists to protect, proven against a real controller rather than
// against notify.Notify in isolation: an operator's webhook endpoint that
// accepts the connection and then never answers must not stop a job being
// submitted, assigned, reported or replaced — and must not stop rc serve
// exiting either.
//
// The webhook here is worse than unreachable: an unreachable host fails
// fast, while this one hangs with the request open, which is exactly the
// failure mode that would wedge a delivery goroutine for its whole timeout
// budget. The test first proves the wiring is live (the endpoint really is
// hit, so --webhook-url is genuinely connected to the fault handler) and
// only then leans on it.
// notifyLatencyBound is what "never blocks" means concretely for a request
// handler that emits an event: queueing is microseconds, so anything near a
// second means the handler is waiting on the webhook. Generous enough to
// survive a loaded CI machine, far below the ~30s a wedged delivery costs.
const notifyLatencyBound = 2 * time.Second

func TestJobsStillScheduleWhileTheWebhookHangs(t *testing.T) {
	hit := make(chan struct{}, 8)
	release := make(chan struct{})
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case hit <- struct{}{}:
		default:
		}
		// Hang until the test is done, or until the controller gives up on
		// this delivery and cancels the request out from under us.
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	// Cleanups run last-registered-first, so release the handler before
	// httptest waits for its outstanding requests to finish.
	t.Cleanup(hook.Close)
	t.Cleanup(func() { close(release) })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	t.Setenv("RC_TOKENS", "wtok:worker,ctok:client")

	cmd := cli.NewServeCmd()
	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--addr", addr, "--data", t.TempDir(), "--webhook-url", hook.URL})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	defer cancel()

	baseURL := "http://" + addr
	cl := client.New(baseURL, "ctok")
	require.Eventually(t, func() bool {
		_, err := cl.State(context.Background())
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "rc serve never came up")

	workerID := registerRawWorker(t, baseURL, "wtok", "hookbox", []string{"gpu0", "gpu1"})

	// Quarantine gpu0 with a verify-sourced fault. That emits an event, and
	// its delivery wedges against the hanging endpoint.
	//
	// The bound is the actual claim being tested. "The request eventually
	// succeeded" would still hold if emitting blocked for the webhook's
	// whole timeout budget — it just would not be non-blocking. An emit
	// that waits on delivery shows up here and nowhere else.
	started := time.Now()
	faultResp := postRawJSON(t, baseURL, "wtok", "/v1/devices/hookbox:gpu0/fault",
		server.FaultRequest{Reason: "verify failed: 10-vram.sh: 512 MiB still allocated"})
	require.Equal(t, http.StatusOK, faultResp)
	require.Less(t, time.Since(started), notifyLatencyBound,
		"the fault handler waited on webhook delivery instead of queueing it")

	select {
	case <-hit:
	case <-time.After(10 * time.Second):
		t.Fatal("the configured webhook was never called; --webhook-url is not wired to the fault handler")
	}

	// From here on the delivery goroutine is stuck inside the webhook. None
	// of this may notice.
	jobA, err := cl.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "hookbox:gpu1", Command: []string{"true"}, Submitter: "agent-a",
	})
	require.NoError(t, err)
	require.Equal(t, model.JobAssigned, jobA.State, "a free device must still be handed out while the webhook hangs")

	jobB, err := cl.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "hookbox:gpu1", Command: []string{"true"}, Submitter: "agent-b",
	})
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, jobB.State)

	// A terminal report that is itself a watchdog trip: it emits a second
	// event behind the wedged one, and must return just as promptly.
	started = time.Now()
	reportRawWatchdogKill(t, baseURL, "wtok", jobA.ID, workerID)
	require.Less(t, time.Since(started), notifyLatencyBound,
		"the status handler waited on webhook delivery instead of queueing it")

	require.Eventually(t, func() bool {
		j, err := cl.Job(context.Background(), jobB.ID)
		return err == nil && j.State == model.JobAssigned
	}, 5*time.Second, 100*time.Millisecond,
		"the queued job was never assigned; a hanging webhook stalled the scheduler")

	// And shutdown stays bounded: closing the notifier must not wait out a
	// wedged endpoint's full retry budget.
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("rc serve did not shut down while the webhook was hanging")
	}
}

// postRawJSON posts body to a live controller and returns the status code.
func postRawJSON(t *testing.T, baseURL, token, path string, body any) int {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

// reportRawWatchdogKill reports a job terminal exactly as a worker whose
// max_runtime watchdog tripped does — the one terminal report that is itself
// an event source.
func reportRawWatchdogKill(t *testing.T, baseURL, token, jobID, workerID string) {
	t.Helper()
	code := 137
	require.Equal(t, http.StatusOK, postRawJSON(t, baseURL, token, "/v1/jobs/"+jobID+"/status",
		server.StatusRequest{
			WorkerID: workerID, State: model.JobKilled, ExitCode: &code,
			Reason: "max_runtime exceeded (4h0m0s)",
		}))
}
