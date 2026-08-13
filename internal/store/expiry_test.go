package store_test

import (
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/stretchr/testify/require"
)

func TestExpiredLeaseIsReleasedAndDeviceQuarantined(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	// Past the lease TTL, with the worker still heartbeating so the
	// worker-loss path is not what fires here.
	c.Advance(20 * time.Minute)
	require.NoError(t, s.RecordHeartbeat("w1", c.Now()))

	res, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired)

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)
	require.Contains(t, reloaded.KillReason, "lease expired")

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Empty(t, leases)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State,
		"an expired lease proves nothing about whether the device is occupied")
}

func TestLiveLeaseIsNotExpired(t *testing.T) {
	s, c := newStore(t)

	_, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	c.Advance(time.Minute)
	require.NoError(t, s.RecordHeartbeat("w1", c.Now()))

	res, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Empty(t, res.LeasesExpired)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceBusy, devices[0].State)
}

// A reboot is the one event that proves the device is clean, so its devices
// go back to ready rather than being quarantined.
func TestRebootReturnsDevicesToReady(t *testing.T) {
	s, c := newStore(t)

	// Establish a known prior boot. Without this the stored boot ID is empty,
	// which is NOT proof of a reboot and must quarantine — see
	// TestMissingBootIDQuarantines.
	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-1", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-2", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)
	require.Contains(t, reloaded.KillReason, "host rebooted")

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)
}

// A worker process restart with the same boot ID proves nothing: an orphan
// may still be holding the GPU.
func TestWorkerRestartWithSameBootIDQuarantines(t *testing.T) {
	s, c := newStore(t)

	// newStore registered w1 with an empty boot ID; register once with one so
	// the restart below is a genuine same-boot case.
	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-1", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-1", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)
	require.Contains(t, reloaded.KillReason, "worker re-registered")

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State)
}

// No boot ID at all is not proof of a reboot.
func TestMissingBootIDQuarantines(t *testing.T) {
	s, c := newStore(t)

	_, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State)
}
