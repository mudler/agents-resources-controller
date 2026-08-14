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

	// Past the lease TTL. No heartbeat is recorded here on purpose: a
	// heartbeating worker renews its job's lease (see
	// TestHeartbeatRenewsALiveJobLease), so the only way for a lease to
	// actually expire is for the worker to have gone silent — that is the
	// scenario this test is pinning. grace/unhealthyAfter are generous so
	// the silence-based demotion and worker-loss reap passes (unrelated
	// mechanisms) don't also fire and confound the assertions below.
	c.Advance(20 * time.Minute)

	res, err := s.Sweep(time.Hour, time.Hour)
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

// Pins the expiry boundary from the "not yet" side: one second short of the
// TTL, the lease must still be live. See TestLeaseExpiresAtTTLBoundary for
// the other side, one second later, where it must not be.
func TestLiveLeaseIsNotExpired(t *testing.T) {
	s, c := newStore(t)

	_, err := s.Allocate(req("agent-a")) // req()'s LeaseTTL is one minute
	require.NoError(t, err)

	c.Advance(59 * time.Second)
	// Generous grace/unhealthyAfter: this test is isolating lease expiry,
	// not the silence-based demotion pass, and no heartbeat is recorded
	// here since that would itself renew the lease under test.
	res, err := s.Sweep(time.Hour, time.Hour)
	require.NoError(t, err)
	require.Empty(t, res.LeasesExpired)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceBusy, devices[0].State)
}

// Pins the expiry boundary from the other side: expires_at is a deadline, so
// at now == expires_at the TTL has fully elapsed and the lease must already
// be treated as expired (Sweep's predicate is <=, not <).
func TestLeaseExpiresAtTTLBoundary(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a")) // req()'s LeaseTTL is one minute
	require.NoError(t, err)

	c.Advance(time.Minute) // exactly the TTL: now == expires_at
	res, err := s.Sweep(time.Hour, time.Hour)
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired)
}

// A worker that keeps heartbeating renews its running job's lease every
// time, so expiry is never a timer on honest, still-reporting work — only a
// backstop once the heartbeats themselves stop (that case is
// TestExpiredLeaseIsReleasedAndDeviceQuarantined above).
func TestHeartbeatRenewsALiveJobLease(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a")) // req()'s LeaseTTL is one minute
	require.NoError(t, err)

	// Advance well past both the original one-minute TTL and the 15-minute
	// renewal window, heartbeating every minute along the way — as a real
	// worker heartbeating every ~10s would, just compressed for the test.
	for i := 0; i < 20; i++ {
		c.Advance(time.Minute)
		require.NoError(t, s.RecordHeartbeat("w1", c.Now()))
	}

	res, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Empty(t, res.LeasesExpired)
	require.Empty(t, res.JobsLost)

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.NotEqual(t, model.JobLost, reloaded.State)

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Len(t, leases, 1, "the lease must still be live")

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

// A reboot proves no process survived, but proves nothing about the
// hardware: a device already quarantined for a self-reported fault must
// stay quarantined through a reboot, not be silently handed back to the
// pool just because the host power-cycled.
func TestRebootDoesNotResurrectHardwareFault(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-1", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	// The worker self-reports a hardware fault while the job is still
	// running on the device.
	require.NoError(t, s.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, c.Now()))

	// The host reboots.
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
	require.Equal(t, model.DeviceUnhealthy, devices[0].State,
		"a reboot proves no process survived, not that the hardware fault is gone")
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

// The path every existing host takes once, the first time it registers
// after being upgraded to a boot-ID-aware worker: the previously stored
// boot ID is empty (never recorded), so a freshly reported non-empty one
// proves nothing about whether a reboot actually happened — an orphaned
// process from before the upgrade may still be pinning the device.
func TestFirstBootIDAfterUpgradeQuarantines(t *testing.T) {
	s, c := newStore(t) // newStore registers w1 with an empty boot ID

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
