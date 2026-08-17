package store_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// isolated is the claim a containerised worker makes: its PID namespace was
// destroyed with the container, so nothing from a previous job exists.
var isolated = model.RecoveryProof{Isolated: true}

// clean is the claim a host worker makes when it looked for processes left
// behind by its previous instance and found none.
var clean = model.RecoveryProof{SurvivorsChecked: true}

// quarantineIdleDevice takes gpubox:gpu0 out of the pool the way the fleet
// actually loses idle devices: its worker simply stops reporting and the
// sweep demotes it with reason worker_lost. No job is involved, which is the
// case the design calls the common one — a box that was doing nothing when
// its worker was restarted.
func quarantineIdleDevice(t *testing.T, s *store.Store, c interface{ Advance(time.Duration) }) {
	t.Helper()
	c.Advance(10 * time.Minute)
	res, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []string{"gpubox:gpu0"}, res.DevicesUnhealthy)
}

func deviceState(t *testing.T, s *store.Store, id string) model.DeviceState {
	t.Helper()
	devices, err := s.Devices()
	require.NoError(t, err)
	for _, d := range devices {
		if d.ID == id {
			return d.State
		}
	}
	t.Fatalf("device %s not found", id)
	return ""
}

// The case that took a box out of the pool while it sat doing nothing: an
// idle device quarantined by the sweep, and a worker that comes back able to
// prove no process from before survived. It must return to the pool without
// an admin token.
func TestAutoRecoverReturnsIdleWorkerLostDeviceToPool(t *testing.T) {
	s, c := newStore(t)
	quarantineIdleDevice(t, s, c)

	recovered, err := s.AutoRecover("w1", isolated)
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	require.Equal(t, "gpubox:gpu0", recovered[0].DeviceID)
	require.Equal(t, "worker_lost", recovered[0].Reason,
		"the event must be able to say what the device was quarantined FOR")

	require.Equal(t, model.DeviceReady, deviceState(t, s, "gpubox:gpu0"))

	// Ready means genuinely schedulable, not merely relabelled.
	_, err = s.Allocate(req("agent-a"))
	require.NoError(t, err)
}

// A host worker that looked for survivors and found none proves exactly what
// isolation proves, so it clears the same quarantines.
func TestAutoRecoverAcceptsACleanSurvivorCheck(t *testing.T) {
	s, c := newStore(t)
	quarantineIdleDevice(t, s, c)

	recovered, err := s.AutoRecover("w1", clean)
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	require.Equal(t, model.DeviceReady, deviceState(t, s, "gpubox:gpu0"))
}

// The case where getting it wrong hands out a GPU with a live training
// process on it: a host worker that cannot prove anything, or that looked and
// found something, must leave the device exactly where it is.
func TestAutoRecoverWithoutProofLeavesTheDeviceQuarantined(t *testing.T) {
	cases := []struct {
		name  string
		proof model.RecoveryProof
	}{
		{"silence is not proof", model.RecoveryProof{}},
		{"survivors found", model.RecoveryProof{SurvivorsChecked: true, SurvivorsFound: true}},
		{"found survivors without claiming to have looked", model.RecoveryProof{SurvivorsFound: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, c := newStore(t)
			quarantineIdleDevice(t, s, c)

			recovered, err := s.AutoRecover("w1", tc.proof)
			require.NoError(t, err)
			require.Empty(t, recovered)
			require.Equal(t, model.DeviceUnhealthy, deviceState(t, s, "gpubox:gpu0"))

			_, err = s.Allocate(req("agent-a"))
			require.ErrorIs(t, err, store.ErrNoDevice)
		})
	}
}

// A self-reported hardware fault is not answered by "no processes are
// running": the probe that reported it tested something this proof does not.
// No claim clears it.
func TestAutoRecoverNeverClearsAFault(t *testing.T) {
	for _, proof := range []model.RecoveryProof{isolated, clean} {
		s, c := newStore(t)
		require.NoError(t, s.SetDeviceState(
			"gpubox:gpu0", model.DeviceUnhealthy, c.Now(), "72G still pinned"))

		recovered, err := s.AutoRecover("w1", proof)
		require.NoError(t, err)
		require.Empty(t, recovered)
		require.Equal(t, model.DeviceUnhealthy, deviceState(t, s, "gpubox:gpu0"))

		reasons, err := s.QuarantineReasons([]string{"gpubox:gpu0"})
		require.NoError(t, err)
		require.Equal(t, "fault", reasons["gpubox:gpu0"], "the fault must survive intact, not be cleared or relabelled")
	}
}

// Nothing may contradict the lease table, the same rule ClearDevice enforces:
// a device with a live lease stays out however good the proof is.
//
// The state this sets up — quarantined for a process-caused reason WITH a
// lease still live — is not reachable through the public API today: every
// path that writes one of those reasons releases the lease in the same
// transaction. That is exactly why the guard is worth pinning rather than
// trusting: it protects against a future lease kind this path does not know
// about, so the only way to exercise it now is to write the row directly.
func TestAutoRecoverSkipsADeviceWithALiveLease(t *testing.T) {
	c := clock.NewFake(time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC))
	dbPath := filepath.Join(t.TempDir(), "rc.db")
	s, err := store.Open(dbPath, c)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1", State: model.DeviceReady}},
	))
	_, err = s.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	raw, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer raw.Close()
	_, err = raw.Exec(
		`UPDATE devices SET state = 'unhealthy', quarantine_reason = 'worker_lost' WHERE id = ?`,
		"gpubox:gpu0")
	require.NoError(t, err)

	recovered, err := s.AutoRecover("w1", isolated)
	require.NoError(t, err)
	require.Empty(t, recovered)
	require.Equal(t, model.DeviceUnhealthy, deviceState(t, s, "gpubox:gpu0"))
}

// A device that quarantines, clears and quarantines again is describing a
// real problem, and silently looping hides it. Three automatic recoveries in
// an hour are allowed; the fourth waits for a human.
func TestAutoRecoverStopsAfterThreeRecoveriesInAnHour(t *testing.T) {
	s, c := newStore(t)

	for i := 1; i <= 3; i++ {
		quarantineIdleDevice(t, s, c)
		recovered, err := s.AutoRecover("w1", isolated)
		require.NoError(t, err)
		require.Len(t, recovered, 1, "recovery %d of 3 must still happen", i)
		require.NoError(t, s.RecordHeartbeat("w1", c.Now(), nil))
		c.Advance(time.Minute)
	}

	quarantineIdleDevice(t, s, c)
	recovered, err := s.AutoRecover("w1", isolated)
	require.NoError(t, err)
	require.Empty(t, recovered, "the fourth recovery inside an hour must not happen")
	require.Equal(t, model.DeviceUnhealthy, deviceState(t, s, "gpubox:gpu0"))
}

// The window slides: a device that flapped three times yesterday is not
// permanently barred from recovering today.
func TestAutoRecoverResumesOnceTheFlapWindowHasPassed(t *testing.T) {
	s, c := newStore(t)

	for range 3 {
		quarantineIdleDevice(t, s, c)
		recovered, err := s.AutoRecover("w1", isolated)
		require.NoError(t, err)
		require.Len(t, recovered, 1)
		require.NoError(t, s.RecordHeartbeat("w1", c.Now(), nil))
	}

	c.Advance(61 * time.Minute)
	quarantineIdleDevice(t, s, c)
	recovered, err := s.AutoRecover("w1", isolated)
	require.NoError(t, err)
	require.Len(t, recovered, 1, "the three earlier recoveries have aged out of the window")
	require.Equal(t, model.DeviceReady, deviceState(t, s, "gpubox:gpu0"))
}

// A worker speaks only for its own devices: its proof says nothing about
// another host's hardware.
func TestAutoRecoverTouchesOnlyTheRegisteringWorkersDevices(t *testing.T) {
	s, c := newStore(t)
	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w2", Host: "orin", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "orin:gpu0", Host: "orin", Name: "gpu0", WorkerID: "w2", State: model.DeviceReady}},
	))

	c.Advance(10 * time.Minute)
	_, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, deviceState(t, s, "gpubox:gpu0"))
	require.Equal(t, model.DeviceUnhealthy, deviceState(t, s, "orin:gpu0"))

	recovered, err := s.AutoRecover("w2", isolated)
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	require.Equal(t, "orin:gpu0", recovered[0].DeviceID)
	require.Equal(t, model.DeviceUnhealthy, deviceState(t, s, "gpubox:gpu0"),
		"another worker's device must be untouched by this worker's proof")
}
