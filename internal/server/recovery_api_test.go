package server_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/notify"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// registerBody is what a worker posts at startup, with whatever it can prove
// about processes left over from an interrupted job.
func registerBody(proof model.RecoveryProof) server.RegisterRequest {
	return server.RegisterRequest{
		Host:     "gpubox",
		Devices:  []server.DeviceSpec{{Name: "gpu0"}},
		Recovery: proof,
	}
}

func stateOf(t *testing.T, st *store.Store, id string) model.DeviceState {
	t.Helper()
	devices, err := st.Devices()
	require.NoError(t, err)
	for _, d := range devices {
		if d.ID == id {
			return d.State
		}
	}
	t.Fatalf("device %s not found", id)
	return ""
}

// The containerised case, end to end over HTTP: a worker restarts while it
// has a job in flight — the pod was rescheduled, the image was rolled — and
// its device must come back to the pool on the worker's own credentials. No
// admin token appears in this test, and that is the point: today the only
// exit from this quarantine is POST /v1/devices/{id}/clear, which a worker
// cannot call.
func TestContainerisedWorkerRestartMidLeaseRecoversWithoutAnAdminToken(t *testing.T) {
	ts, st, _, c := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register", registerBody(model.RecoveryProof{}))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./train"}, Submitter: "agent-a", LeaseTTL: time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, st.MarkRunning(job.ID, c.Now()))

	// The restarted process announces itself, proving its PID namespace went
	// with the container it died in.
	resp = post(t, ts, "wtok", "/v1/workers/register", registerBody(model.RecoveryProof{Isolated: true}))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// The stranded job is still reconciled as lost — recovery is about the
	// device, not about pretending the job survived.
	reloaded, err := st.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)

	require.Equal(t, model.DeviceReady, stateOf(t, st, "gpubox:gpu0"),
		"an isolated worker's device must return to the pool on registration alone")

	_, err = st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-b", LeaseTTL: time.Hour,
	})
	require.NoError(t, err, "the recovered device must be genuinely schedulable")
}

// The dangerous case. A host worker restarting under systemd does NOT kill
// the jobs it started — they are reparented to init and may still be
// training on the card. A worker that cannot prove they are gone says
// nothing, and nothing is exactly what must happen to the device.
func TestHostWorkerThatCannotProveSurvivorsAreGoneDoesNotRecover(t *testing.T) {
	cases := []struct {
		name  string
		proof model.RecoveryProof
	}{
		{"no claim at all", model.RecoveryProof{}},
		{"looked and found a survivor", model.RecoveryProof{SurvivorsChecked: true, SurvivorsFound: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts, st, _, c := newServer(t)

			resp := post(t, ts, "wtok", "/v1/workers/register", registerBody(model.RecoveryProof{}))
			require.Equal(t, http.StatusOK, resp.StatusCode)
			resp.Body.Close()

			job, err := st.Allocate(store.AllocateRequest{
				DeviceID: "gpubox:gpu0", Command: []string{"./train"}, Submitter: "agent-a", LeaseTTL: time.Hour,
			})
			require.NoError(t, err)
			require.NoError(t, st.MarkRunning(job.ID, c.Now()))

			resp = post(t, ts, "wtok", "/v1/workers/register", registerBody(tc.proof))
			require.Equal(t, http.StatusOK, resp.StatusCode)
			resp.Body.Close()

			require.Equal(t, model.DeviceUnhealthy, stateOf(t, st, "gpubox:gpu0"),
				"a device whose previous job may still be running on it must stay out of the pool")
		})
	}
}

// An operator must be able to answer "why is this device back?" without
// reading the controller's logs.
func TestAutoRecoveryEmitsAnEvent(t *testing.T) {
	ts, st, n, sink := newNotifyingServer(t)

	// Both devices go out the way idle devices actually go out: their worker
	// stopped reporting and the sweep quarantined them. No job is involved —
	// an unhealthyAfter of zero is how this test says "the silence has gone
	// on long enough" without needing the clock.
	res, err := st.Sweep(0, 0)
	require.NoError(t, err)
	require.Len(t, res.DevicesUnhealthy, 2)

	resp := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host:     "gpubox",
		Devices:  []server.DeviceSpec{{Name: "gpu0"}, {Name: "gpu1"}},
		Recovery: model.RecoveryProof{SurvivorsChecked: true},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	var got []notify.Event
	for _, e := range flush(t, n, sink) {
		if e.Kind == notify.KindDeviceRecovered {
			got = append(got, e)
		}
	}
	require.Len(t, got, 2, "every device returned to the pool must be announced, by name")
	require.Equal(t, "gpubox:gpu0", got[0].Device)
	require.Contains(t, got[0].Reason, "worker_lost", "the event must say what the device was out for")
	require.Contains(t, got[0].Reason, "no surviving processes", "and what proof brought it back")
	require.Equal(t, "gpubox:gpu1", got[1].Device)
}
