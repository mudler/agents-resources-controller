package store_test

import (
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
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

	res, err := s.Sweep(time.Hour, time.Hour, time.Time{})
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
	res, err := s.Sweep(time.Hour, time.Hour, time.Time{})
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
	res, err := s.Sweep(time.Hour, time.Hour, time.Time{})
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired)
}

// A worker that keeps heartbeating AND keeps naming the job it is running
// renews that job's lease every time, so expiry is never a timer on honest,
// still-reporting work — only a backstop once the heartbeats themselves stop
// (that case is TestExpiredLeaseIsReleasedAndDeviceQuarantined above) or the
// worker stops claiming the job (TestHeartbeatDoesNotRenewAnUnclaimedJobLease
// below). This is the healthy path: it must keep working exactly as before.
func TestHeartbeatRenewsALiveJobLease(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a")) // req()'s LeaseTTL is one minute
	require.NoError(t, err)

	// Advance well past both the original one-minute TTL and the 15-minute
	// renewal window, heartbeating every minute along the way — as a real
	// worker heartbeating every ~10s would, just compressed for the test.
	for i := 0; i < 20; i++ {
		c.Advance(time.Minute)
		require.NoError(t, s.RecordHeartbeat("w1", c.Now(), []string{job.ID}))
	}

	res, err := s.Sweep(30*time.Second, 5*time.Minute, time.Time{})
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

// The lost-assignment-response case, and the reason lease renewal is driven
// by what the worker claims rather than by the worker merely being alive.
//
// The controller commits a job to "running" before writing the assignment
// response (see server.handleAssignments — that ordering is deliberate: it is
// what stops a retried poll from starting the same job twice). If that
// response is then lost, the worker has no process for the job and will never
// report on it, but it keeps heartbeating for its other work. Renewing every
// lease the worker owns on that heartbeat pinned the lease forever: the
// device stayed busy with a holder that did not exist, expires_in never
// counted down, and no CLI could recover the hardware.
//
// Here the worker heartbeats without naming the job, exactly as a worker that
// never received it would. The lease must lapse, the sweep must mark the job
// lost and quarantine the device, and an admin clear must then be able to put
// the hardware back in the pool.
func TestHeartbeatDoesNotRenewAnUnclaimedJobLease(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a")) // req()'s LeaseTTL is one minute
	require.NoError(t, err)
	require.NoError(t, s.MarkRunning(job.ID, c.Now()))

	// The worker is alive and heartbeating throughout — it just never claims
	// this job, because it never got the assignment.
	for i := 0; i < 3; i++ {
		c.Advance(10 * time.Second)
		require.NoError(t, s.RecordHeartbeat("w1", c.Now(), nil))
	}

	// Still inside the original TTL: nothing has expired yet, and the worker
	// itself is in perfect contact, so nothing else may demote the device.
	res, err := s.Sweep(30*time.Second, 5*time.Minute, time.Time{})
	require.NoError(t, err)
	require.Empty(t, res.LeasesExpired)
	require.Empty(t, res.DevicesUnhealthy)

	// Past the lease TTL, with the worker still heartbeating and still not
	// claiming the job.
	c.Advance(2 * time.Minute)
	require.NoError(t, s.RecordHeartbeat("w1", c.Now(), nil))

	res, err = s.Sweep(30*time.Second, 5*time.Minute, time.Time{})
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired,
		"a lease nobody claims to be holding must be allowed to expire")

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)
	require.Contains(t, reloaded.KillReason, "lease expired")

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Empty(t, leases, "the stranded lease must be released")

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State,
		"expiry proves the holder stopped reporting, not that the device is idle")

	// And the device is recoverable: with no live lease left, the operator's
	// clear is no longer refused, which is what makes this an actual way out
	// rather than a different kind of stuck.
	cleared, err := s.ClearDevice("gpubox:gpu0")
	require.NoError(t, err)
	require.True(t, cleared)

	next, err := s.Allocate(req("agent-b"))
	require.NoError(t, err)
	require.Equal(t, "gpubox:gpu0", next.DeviceID)
}

// A worker may only renew what is genuinely its own: naming a job assigned to
// a different worker must not push that job's lease forward, or a
// misconfigured (or hostile) worker could keep someone else's device pinned.
func TestHeartbeatOnlyRenewsItsOwnJobs(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w2", Host: "otherbox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "otherbox:gpu0", Host: "otherbox", Name: "gpu0", WorkerID: "w2", State: model.DeviceReady}},
	))

	// w2 claims w1's job on every heartbeat. w1 keeps its own worker row
	// fresh so this test isolates lease renewal from the silence sweep.
	for i := 0; i < 4; i++ {
		c.Advance(20 * time.Second)
		require.NoError(t, s.RecordHeartbeat("w2", c.Now(), []string{job.ID}))
		require.NoError(t, s.RecordHeartbeat("w1", c.Now(), nil))
	}
	c.Advance(2 * time.Minute)
	require.NoError(t, s.RecordHeartbeat("w1", c.Now(), nil))

	res, err := s.Sweep(30*time.Second, 5*time.Minute, time.Time{})
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired,
		"another worker's claim must not renew this job's lease")
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
	require.NoError(t, s.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, c.Now(), ""))

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

// devicesByID returns the fleet keyed by device ID, so a test with more than
// one device is not hostage to the order Devices() happens to return.
func devicesByID(t *testing.T, s *store.Store) map[string]model.Device {
	t.Helper()
	devices, err := s.Devices()
	require.NoError(t, err)
	out := map[string]model.Device{}
	for _, d := range devices {
		out[d.ID] = d
	}
	return out
}

// registerTwoDeviceHost registers a host with a known prior boot ID and two
// devices, which is the shape the reboot-recovery tests below need: a fleet
// where the reaper has quarantined everything, not just the one device that
// happened to be running a job.
func registerTwoDeviceHost(t *testing.T, s *store.Store, at time.Time, bootID string) {
	t.Helper()
	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: bootID, LastHeartbeatAt: at},
		[]model.Device{
			{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"},
			{ID: "gpubox:gpu1", Host: "gpubox", Name: "gpu1", WorkerID: "w1"},
		},
	))
}

// The case boot-ID recovery was silently missing: a host that stays down
// longer than the reaper's unhealthyAfter — a machine rebooted overnight, or
// held down for maintenance, which is the common case rather than the exotic
// one.
//
// By the time it comes back, the sweep has already marked its jobs lost and
// quarantined every one of its devices, so there is no in-flight job left for
// the registration reap to key off and the reboot branch used to match
// nothing at all. The device state that survived was `unhealthy`, and the
// operator was back to clearing by hand after every reboot — the exact habit
// the boot ID exists to remove.
func TestRebootAfterProlongedDowntimeReturnsDevicesToReady(t *testing.T) {
	s, c := newStore(t)
	registerTwoDeviceHost(t, s, c.Now(), "boot-1")

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	// The host goes away for far longer than unhealthyAfter.
	c.Advance(10 * time.Minute)
	res, err := s.Sweep(30*time.Second, 5*time.Minute, time.Time{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"gpubox:gpu0", "gpubox:gpu1"}, res.DevicesUnhealthy)
	require.Equal(t, []string{job.ID}, res.JobsLost)

	// It comes back rebooted.
	c.Advance(time.Minute)
	registerTwoDeviceHost(t, s, c.Now(), "boot-2")

	devices := devicesByID(t, s)
	require.Equal(t, model.DeviceReady, devices["gpubox:gpu0"].State,
		"a proven reboot must return a device quarantined by worker loss to the pool")
	require.Equal(t, model.DeviceReady, devices["gpubox:gpu1"].State,
		"including devices that were never running anything")

	// Ready means genuinely schedulable, not merely relabelled.
	next, err := s.Allocate(req("agent-b"))
	require.NoError(t, err)
	require.Equal(t, "gpubox:gpu0", next.DeviceID)
}

// The constraint the fix above must respect: a reboot proves no process
// survived, but proves nothing about the hardware. A device the host itself
// reported faulty stays quarantined across the same prolonged downtime and
// the same proven reboot — only a human may put that one back.
func TestRebootAfterProlongedDowntimeKeepsFaultQuarantine(t *testing.T) {
	s, c := newStore(t)
	registerTwoDeviceHost(t, s, c.Now(), "boot-1")

	// gpu1 is reported faulty by the host itself; gpu0 is fine.
	require.NoError(t, s.SetDeviceState("gpubox:gpu1", model.DeviceUnhealthy, c.Now(), ""))

	c.Advance(10 * time.Minute)
	_, err := s.Sweep(30*time.Second, 5*time.Minute, time.Time{})
	require.NoError(t, err)

	c.Advance(time.Minute)
	registerTwoDeviceHost(t, s, c.Now(), "boot-2")

	devices := devicesByID(t, s)
	require.Equal(t, model.DeviceReady, devices["gpubox:gpu0"].State,
		"the healthy device is recovered by the reboot")
	require.Equal(t, model.DeviceUnhealthy, devices["gpubox:gpu1"].State,
		"a power cycle does not settle a reported hardware fault")
}

// The same prolonged downtime with no proof of a reboot must change nothing:
// an orphaned process from before may still be pinning the device.
func TestProlongedDowntimeWithoutRebootProofKeepsQuarantine(t *testing.T) {
	for _, tc := range []struct{ name, returningBootID string }{
		{"same boot id", "boot-1"},
		{"empty boot id", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := newStore(t)
			registerTwoDeviceHost(t, s, c.Now(), "boot-1")

			_, err := s.Allocate(req("agent-a"))
			require.NoError(t, err)

			c.Advance(10 * time.Minute)
			_, err = s.Sweep(30*time.Second, 5*time.Minute, time.Time{})
			require.NoError(t, err)

			c.Advance(time.Minute)
			registerTwoDeviceHost(t, s, c.Now(), tc.returningBootID)

			devices := devicesByID(t, s)
			require.Equal(t, model.DeviceUnhealthy, devices["gpubox:gpu0"].State)
			require.Equal(t, model.DeviceUnhealthy, devices["gpubox:gpu1"].State)

			_, err = s.Allocate(req("agent-b"))
			require.ErrorIs(t, err, store.ErrNoDevice)
		})
	}
}

// A device quarantined by lease expiry — the backstop path, where a job's
// holder stopped renewing but the worker itself kept reporting — is also a
// "we could not prove nothing is on it" quarantine, so a proven reboot
// answers it too.
func TestRebootRecoversADeviceQuarantinedByLeaseExpiry(t *testing.T) {
	s, c := newStore(t)
	registerTwoDeviceHost(t, s, c.Now(), "boot-1")

	job, err := s.Allocate(req("agent-a")) // req()'s LeaseTTL is one minute
	require.NoError(t, err)
	require.NoError(t, s.MarkRunning(job.ID, c.Now()))

	// The worker keeps heartbeating but never claims the job: its lease
	// lapses and the device is quarantined (see
	// TestHeartbeatDoesNotRenewAnUnclaimedJobLease).
	c.Advance(2 * time.Minute)
	require.NoError(t, s.RecordHeartbeat("w1", c.Now(), nil))
	res, err := s.Sweep(30*time.Second, 5*time.Minute, time.Time{})
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired)
	require.Equal(t, model.DeviceUnhealthy, devicesByID(t, s)["gpubox:gpu0"].State)

	registerTwoDeviceHost(t, s, c.Now(), "boot-2")

	require.Equal(t, model.DeviceReady, devicesByID(t, s)["gpubox:gpu0"].State,
		"a reboot proves the process behind the expired lease is gone")
}

// A device an operator cleared and a device a reboot recovered must be
// indistinguishable afterwards: if the recovery left the old quarantine
// reason behind, the NEXT quarantine on that device could be mislabelled and
// a future reboot could revive a genuine fault.
func TestRecoveredDeviceQuarantinesFreshlyAfterwards(t *testing.T) {
	s, c := newStore(t)
	registerTwoDeviceHost(t, s, c.Now(), "boot-1")

	c.Advance(10 * time.Minute)
	_, err := s.Sweep(30*time.Second, 5*time.Minute, time.Time{}) // quarantined: worker lost
	require.NoError(t, err)
	registerTwoDeviceHost(t, s, c.Now(), "boot-2") // recovered by the reboot
	require.Equal(t, model.DeviceReady, devicesByID(t, s)["gpubox:gpu0"].State)

	// Now the host reports a real fault on it, and reboots again.
	require.NoError(t, s.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, c.Now(), ""))
	registerTwoDeviceHost(t, s, c.Now(), "boot-3")

	require.Equal(t, model.DeviceUnhealthy, devicesByID(t, s)["gpubox:gpu0"].State,
		"a fault reported after a recovery must still outlive the next reboot")
}
