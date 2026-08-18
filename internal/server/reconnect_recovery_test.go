package server_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// What the worker's reconnection is for, from the controller's side.
//
// On 2026-08-18 a lease expired because the controller had restarted, its
// device was quarantined, and it stayed quarantined: autonomous recovery runs
// at registration, only the controller had restarted, and the worker — alive
// and heartbeating the whole time — never registered again. A human cleared
// it. Once the worker registers again on its own, the existing proof path
// does the rest, and no new way of clearing a device had to be invented.

func TestAReRegistrationAfterAControllerRestartRecoversALeaseExpiryQuarantine(t *testing.T) {
	ts, st, _, c := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register", registerBody(model.RecoveryProof{}))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./train"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	require.NoError(t, st.MarkRunning(job.ID, c.Now()))

	// The lease lapses with nobody renewing it and the device goes out of
	// the pool. (The startup grace is what stops a controller restart
	// CAUSING this; it is not what gets the device back afterwards, and a
	// device quarantined for any of the other recoverable causes is in
	// exactly the same position.)
	c.Advance(2 * time.Minute)
	res, err := st.Sweep(30*time.Second, 5*time.Minute, time.Time{})
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired)
	require.Equal(t, model.DeviceUnhealthy, stateOf(t, st, "gpubox:gpu0"))

	// The worker noticed the controller is a different process and came back
	// through registration, carrying what it can prove: it is running
	// nothing, and no process from the interrupted job survives.
	resp = post(t, ts, "wtok", "/v1/workers/register",
		registerBody(model.RecoveryProof{SurvivorsChecked: true}))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	require.Equal(t, model.DeviceReady, stateOf(t, st, "gpubox:gpu0"),
		"a device quarantined by an expiry nobody could renew against must come back on the worker's proof")

	_, err = st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-b", LeaseTTL: time.Hour,
	})
	require.NoError(t, err, "the recovered device must be genuinely schedulable")
}

// And the line that does not move. A self-reported hardware fault is not
// answered by "no processes are running": the probe that reported it tested
// something this proof does not. Nothing about reconnecting more often may
// erode that — a worker that reconnects every time its controller restarts
// would otherwise be a slow way of clearing every fault in the fleet.
func TestAReRegistrationNeverClearsAFault(t *testing.T) {
	ts, st, _, c := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register", registerBody(model.RecoveryProof{}))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	require.NoError(t, st.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, c.Now(),
		"verify failed: 10-vram.sh: 512 MiB still allocated"))

	for _, proof := range []model.RecoveryProof{
		{SurvivorsChecked: true},
		{Isolated: true},
	} {
		resp = post(t, ts, "wtok", "/v1/workers/register", registerBody(proof))
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		require.Equal(t, model.DeviceUnhealthy, stateOf(t, st, "gpubox:gpu0"),
			"a self-reported fault waits for a human, whatever the worker can prove about processes")
	}
}
