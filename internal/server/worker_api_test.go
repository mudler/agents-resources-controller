package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func newServer(t *testing.T) (*httptest.Server, *store.Store, *clock.Fake) {
	t.Helper()
	dir := t.TempDir()
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })

	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client", "atok": "admin"},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, st, c
}

func post(t *testing.T, ts *httptest.Server, token, path string, body any) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, &buf)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}

func TestUnauthenticatedRequestIsRejected(t *testing.T) {
	ts, _, _ := newServer(t)
	resp := post(t, ts, "bogus", "/v1/workers/register", server.RegisterRequest{Host: "gpubox"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestClientTokenCannotRegisterWorker(t *testing.T) {
	ts, _, _ := newServer(t)
	resp := post(t, ts, "ctok", "/v1/workers/register", server.RegisterRequest{Host: "gpubox"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestRegisterCreatesDevices(t *testing.T) {
	ts, st, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []string{"gpu0", "gpu1"}})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.WorkerID)

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Len(t, devices, 2)
	require.Equal(t, "gpubox:gpu0", devices[0].ID)
	require.Equal(t, model.DeviceReady, devices[0].State)
}

func TestAssignmentsLongPollReturns204WhenIdle(t *testing.T) {
	ts, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []string{"gpu0"}})
	var reg server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&reg))
	resp.Body.Close()

	req, err := http.NewRequest(http.MethodGet,
		ts.URL+"/v1/workers/"+reg.WorkerID+"/assignments?wait=50ms", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer wtok")

	got, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer got.Body.Close()
	require.Equal(t, http.StatusNoContent, got.StatusCode)
}

func TestWorkerReportingTerminalStatusFreesDevice(t *testing.T) {
	ts, st, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []string{"gpu0"}})
	resp.Body.Close()

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"true"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	code := 0
	sr := post(t, ts, "wtok", "/v1/jobs/"+job.ID+"/status",
		server.StatusRequest{State: model.JobSucceeded, ExitCode: &code})
	defer sr.Body.Close()
	require.Equal(t, http.StatusOK, sr.StatusCode)

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)
}
