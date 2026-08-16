package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

func fetchState(t *testing.T, ts *httptest.Server, token string) server.StateResponse {
	t.Helper()
	resp := get(t, ts, token, "/v1/state")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out server.StateResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func viewOf(t *testing.T, state server.StateResponse, id string) server.DeviceView {
	t.Helper()
	for _, v := range state.Devices {
		if v.Device.ID == id {
			return v
		}
	}
	t.Fatalf("device %s not in state", id)
	return server.DeviceView{}
}

// The dashboard's whole job when a device leaves the pool is to say WHY.
// /v1/state announced `unhealthy` and withheld the reason, so the page could
// offer a "clear" button beside a problem it could not describe, and an
// operator had to leave for `rc describe` to learn what they were clearing.
func TestStateCarriesTheQuarantineReason(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	post(t, ts, "wtok", "/v1/devices/gpubox:gpu0/fault",
		map[string]string{"reason": "verify failed: 50-vram.sh: exited 1: 72G still pinned"}).Body.Close()

	dev := viewOf(t, fetchState(t, ts, "ctok"), "gpubox:gpu0")
	require.Equal(t, model.DeviceUnhealthy, dev.Device.State)
	require.Contains(t, dev.QuarantineReason, "72G still pinned",
		"a device out of the pool must say why, or the page can only announce the problem")
}

// A healthy device carries no reason, so its presence alone is the "something
// is wrong here" signal and no second condition is needed to read it.
func TestStateOmitsTheQuarantineReasonWhenHealthy(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	require.Empty(t, viewOf(t, fetchState(t, ts, "ctok"), "gpubox:gpu0").QuarantineReason)
}

// "Queued" on its own is not useful: queued for nine seconds is normal and
// queued for forty minutes is a problem, and both used to render identically.
// The wait is measured by the CONTROLLER, like every other age on this wire —
// a reader's clock must not be able to make a stuck queue look fresh.
func TestStateCarriesHowLongEachQueuedJobHasWaited(t *testing.T) {
	ts, _, _, clk := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"sleep", "60"}, Submitter: "alice",
	})
	first.Body.Close()

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"echo", "second"}, Submitter: "bob",
	})
	defer resp.Body.Close()
	var queued model.Job
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&queued))
	require.Equal(t, model.JobQueued, queued.State)

	clk.Advance(11 * time.Minute)

	state := fetchState(t, ts, "ctok")
	require.Equal(t, 660, state.QueuedWaitingSeconds[queued.ID],
		"the controller must report the wait it measured on its own clock")
}
