package server_test

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
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
