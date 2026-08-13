package server_test

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func registerWorker(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []string{"gpu0"}})
	defer resp.Body.Close()
	var reg server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&reg))
	return reg.WorkerID
}

func TestSubmitAllocatesDeviceAndReturnsJob(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var job model.Job
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&job))
	require.Equal(t, model.JobAssigned, job.State)
	require.Equal(t, "gpubox:gpu0", job.DeviceID)
}

// Stage 1 has no queue: a busy device is refused immediately, not parked.
func TestSubmitOnBusyDeviceReturns409(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	first.Body.Close()

	second := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-b",
	})
	defer second.Body.Close()
	require.Equal(t, http.StatusConflict, second.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(second.Body).Decode(&body))
	require.Equal(t, "no_device_available", body["error"])
}

func TestDevicesViewShowsHolderAndHeartbeatAge(t *testing.T) {
	ts, _, _, c := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	resp.Body.Close()

	c.Advance(90 * time.Second)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/state", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	got, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer got.Body.Close()

	var state server.StateResponse
	require.NoError(t, json.NewDecoder(got.Body).Decode(&state))
	require.Len(t, state.Devices, 1)
	require.Equal(t, "agent-a", state.Devices[0].Holder)
	require.Equal(t, []string{"./bench"}, state.Devices[0].Command)
	require.Equal(t, 90, state.Devices[0].ElapsedSeconds)
	require.Equal(t, 90, state.Devices[0].HeartbeatAgeSeconds)
}

func TestLogStreamDeliversWorkerChunks(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	var job model.Job
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&job))
	resp.Body.Close()

	appended := post(t, ts, "wtok", "/v1/jobs/"+job.ID+"/logs", nil)
	appended.Body.Close()

	// Real chunks arrive as a raw body, not JSON.
	raw, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/jobs/"+job.ID+"/logs",
		strings.NewReader("hello from the box\n"))
	require.NoError(t, err)
	raw.Header.Set("Authorization", "Bearer wtok")
	pushed, err := ts.Client().Do(raw)
	require.NoError(t, err)
	pushed.Body.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/jobs/"+job.ID+"/logs", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	stream, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer stream.Body.Close()

	sc := bufio.NewScanner(stream.Body)
	require.True(t, sc.Scan())
	require.Contains(t, sc.Text(), "hello from the box")
}

// TestStreamLogsForUnknownJobReturns404 guards against Follow silently
// O_CREATE-ing a log file (and pinning a goroutine + fd) for a job ID that
// was never allocated. Requires its own server so the log directory path is
// known and inspectable — newServer does not expose it.
func TestStreamLogsForUnknownJobReturns404(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })

	logsDir := filepath.Join(dir, "logs")
	logs, err := logstore.New(logsDir)
	require.NoError(t, err)

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client", "atok": "admin"},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/jobs/does-not-exist/logs", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "not_found", body["error"])

	// logstore.Read returning empty is not proof nothing was created — an
	// O_CREATE'd empty file reads back as empty too. Check the directory.
	entries, err := os.ReadDir(logsDir)
	require.NoError(t, err)
	require.Empty(t, entries, "no log file should exist for a job that was never allocated")
}

// TestGetJobStoreFailureIsNot404 makes sure an infrastructure failure is
// never reported to the client as "job not found": a live job during a DB
// outage must not look like a vanished job, or a client might resubmit onto
// a device that is still actually busy.
func TestGetJobStoreFailureIsNot404(t *testing.T) {
	ts, st, _, _ := newServer(t)
	require.NoError(t, st.Close())

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/jobs/whatever-id", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "database is closed",
		"the raw driver error must not leak to the client")

	var body map[string]string
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Equal(t, "store_error", body["error"])
}

// TestClearDeviceWithLiveLeaseReturns409 covers the one manual override that
// exists for a stuck GPU: ClearDevice refuses to contradict a live lease, and
// the HTTP layer must report that refusal, not paper over it with a 200.
func TestClearDeviceWithLiveLeaseReturns409(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// The device now has both a live lease and (forced) unhealthy state, the
	// exact scenario an operator would try to clear.
	require.NoError(t, st.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, time.Now()))

	clr := post(t, ts, "atok", "/v1/devices/gpubox:gpu0/clear", nil)
	defer clr.Body.Close()
	require.Equal(t, http.StatusConflict, clr.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(clr.Body).Decode(&body))
	require.Equal(t, "device_not_cleared", body["error"])

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State,
		"the device must still not be schedulable after a refused clear")
}

func TestSubmitRejectsEmptySubmitter(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestSubmitWakesWaitingWorkerLongPoll is the only coverage of poke: without
// it, a worker blocked in the assignments long-poll would only ever see a
// freshly assigned job after the wait window (up to 30s in production)
// expired, not the moment it was submitted.
func TestSubmitWakesWaitingWorkerLongPoll(t *testing.T) {
	ts, _, _, _ := newServer(t)
	workerID := registerWorker(t, ts)

	type pollResult struct {
		resp *http.Response
		err  error
		dur  time.Duration
	}
	pollDone := make(chan pollResult, 1)
	pollStarted := make(chan struct{})

	go func() {
		req, err := http.NewRequest(http.MethodGet,
			ts.URL+"/v1/workers/"+workerID+"/assignments?wait=10s", nil)
		if err != nil {
			pollDone <- pollResult{err: err}
			return
		}
		req.Header.Set("Authorization", "Bearer wtok")
		close(pollStarted)
		start := time.Now()
		resp, err := ts.Client().Do(req)
		pollDone <- pollResult{resp: resp, err: err, dur: time.Since(start)}
	}()

	<-pollStarted
	time.Sleep(50 * time.Millisecond) // let the poll register with the notifier before submit

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	select {
	case r := <-pollDone:
		require.NoError(t, r.err)
		defer r.resp.Body.Close()
		require.Equal(t, http.StatusOK, r.resp.StatusCode)
		require.Less(t, r.dur, 2*time.Second,
			"poke should wake the long-poll well before the 10s wait window elapses")

		var assignments []server.Assignment
		require.NoError(t, json.NewDecoder(r.resp.Body).Decode(&assignments))
		require.Len(t, assignments, 1)
		require.Equal(t, "gpubox:gpu0", assignments[0].DeviceID)
	case <-time.After(5 * time.Second):
		t.Fatal("long-poll did not return after submit poked the worker")
	}
}
