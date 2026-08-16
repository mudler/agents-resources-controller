package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func deviceIDsOf(t *testing.T, state server.StateResponse) []string {
	t.Helper()
	out := make([]string, 0, len(state.Devices))
	for _, v := range state.Devices {
		out = append(out, v.Device.ID)
	}
	return out
}

func del(t *testing.T, ts *httptest.Server, token, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, ts.URL+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}

// Retiring is destructive and fleet-wide, so it sits behind the admin token
// exactly as clearing does. A client token must not be able to delete the
// hardware inventory.
func TestRetireDeviceRequiresAdmin(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := del(t, ts, "ctok", "/v1/devices/gpubox:gpu0")
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	require.Contains(t, deviceIDsOf(t, fetchState(t, ts, "ctok")), "gpubox:gpu0",
		"a refused retire must leave the device in place")
}

func TestRetireDeviceRemovesItFromTheFleet(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := del(t, ts, "atok", "/v1/devices/gpubox:gpu0")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.NotContains(t, deviceIDsOf(t, fetchState(t, ts, "ctok")), "gpubox:gpu0")
}

// The response says the thing an operator would otherwise learn by watching
// the row reappear: a worker that still declares this device will recreate
// it on its next registration.
func TestRetireDeviceSaysItMayComeBack(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := del(t, ts, "atok", "/v1/devices/gpubox:gpu0")
	defer resp.Body.Close()
	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Contains(t, body["note"], "reappear")
}

// Retiring hardware out from under a running job would strand it on a device
// the controller no longer believes exists.
func TestRetireDeviceRefusesWhileHeld(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)
	post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"sleep", "60"}, Submitter: "alice",
	}).Body.Close()

	resp := del(t, ts, "atok", "/v1/devices/gpubox:gpu0")
	defer resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.Contains(t, deviceIDsOf(t, fetchState(t, ts, "ctok")), "gpubox:gpu0")
}

func TestRetireUnknownDeviceIs404(t *testing.T) {
	ts, _, _, _ := newServer(t)
	resp := del(t, ts, "atok", "/v1/devices/gpubox:nope")
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// whoami is what lets the dashboard offer admin controls to an admin token
// instead of prompting for a second one on every click.
func TestWhoamiReportsTheCallersRole(t *testing.T) {
	ts, _, _, _ := newServer(t)

	for token, want := range map[string]string{"ctok": "client", "atok": "admin"} {
		resp := get(t, ts, token, "/v1/whoami")
		var body server.WhoamiResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		resp.Body.Close()
		require.Equal(t, want, body.Role)
	}
}

// It reports a role and nothing else: a response that echoed the token back
// would put it somewhere new (a log, a devtools pane) for no gain.
func TestWhoamiNeverEchoesTheToken(t *testing.T) {
	ts, _, _, _ := newServer(t)
	resp := get(t, ts, "atok", "/v1/whoami")
	defer resp.Body.Close()
	var raw map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&raw))
	require.Equal(t, []string{"role"}, keysOf(raw))
}

func TestWhoamiRejectsAnUnknownToken(t *testing.T) {
	ts, _, _, _ := newServer(t)
	resp := get(t, ts, "nope", "/v1/whoami")
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
