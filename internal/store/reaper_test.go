package store_test

import (
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func TestSweepMarksSilentWorkerDevicesUnknown(t *testing.T) {
	s, c := newStore(t)

	c.Advance(45 * time.Second)
	res, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []string{"gpubox:gpu0"}, res.DevicesUnknown)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnknown, devices[0].State)
}

func TestHeartbeatRestoresUnknownDeviceToReady(t *testing.T) {
	s, c := newStore(t)

	c.Advance(45 * time.Second)
	_, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)

	require.NoError(t, s.RecordHeartbeat("w1", c.Now(), nil))

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)
}

// A device demoted to unknown while it was BUSY must come back as busy, not
// ready: the job is still running on it. Returning it to the pool would hand
// an occupied GPU to the next claimant.
func TestHeartbeatRestoresLeasedDeviceToBusyNotReady(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	c.Advance(45 * time.Second)
	_, err = s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)

	// The worker is back and still supervising the job, so it names it.
	require.NoError(t, s.RecordHeartbeat("w1", c.Now(), []string{job.ID}))

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceBusy, devices[0].State)

	// And it must not be claimable while that lease is live.
	_, err = s.Allocate(req("agent-b"))
	require.ErrorIs(t, err, store.ErrNoDevice)

	// Releasing the original job still returns it to the pool normally.
	code := 0
	require.NoError(t, s.Release(job.ID, model.JobSucceeded, &code, ""))
	devices, err = s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)
}

// A device whose worker never came back is NOT returned to the pool: we cannot
// prove nothing is still occupying it.
func TestSweepMarksLostWorkerDevicesUnhealthyNotReady(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	c.Advance(10 * time.Minute)
	res, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []string{"gpubox:gpu0"}, res.DevicesUnhealthy)
	require.Equal(t, []string{job.ID}, res.JobsLost)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State)

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)

	// Nothing may be scheduled onto it.
	_, err = s.Allocate(req("agent-b"))
	require.ErrorIs(t, err, store.ErrNoDevice)
}

func TestClearDeviceMakesUnhealthyDeviceSchedulableAgain(t *testing.T) {
	s, c := newStore(t)

	_, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	c.Advance(10 * time.Minute)
	_, err = s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)

	cleared, err := s.ClearDevice("gpubox:gpu0")
	require.NoError(t, err)
	require.True(t, cleared)
	require.NoError(t, s.RecordHeartbeat("w1", c.Now(), nil))

	job, err := s.Allocate(req("agent-b"))
	require.NoError(t, err)
	require.Equal(t, "gpubox:gpu0", job.DeviceID)
}

// ClearDevice is an operator's assertion that nothing is on the device. It
// must not be able to contradict the lease table.
func TestClearDeviceRefusesWhileLeaseIsLive(t *testing.T) {
	s, _ := newStore(t)

	_, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.NoError(t, s.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, time.Now()))

	cleared, err := s.ClearDevice("gpubox:gpu0")
	require.NoError(t, err)
	require.False(t, cleared)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State)

	_, err = s.Allocate(req("agent-b"))
	require.ErrorIs(t, err, store.ErrNoDevice)
}

// TestRegistrationReapsInFlightJobsAndQuarantinesDevices is the critical
// review fix: a worker that registers is a fresh process announcing it has
// no running jobs, so registration must reconcile whatever the previous
// process left in flight rather than trust the device's current state.
// Without this, a worker restart while a job is running/assigned either
// leaves the device stuck busy forever (the state-preserving CASE kept it
// busy) or, once the reaper has already demoted it, falsifies the device as
// ready while an orphaned process from the dead worker may still hold it.
// Neither is acceptable under "never hand out a device we cannot prove is
// free" — the device must come back unhealthy, not ready and not busy, and
// stay that way until an operator clears it.
func TestRegistrationReapsInFlightJobsAndQuarantinesDevices(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.NoError(t, s.MarkRunning(job.ID, c.Now()))

	// The same worker ID re-registers, as a genuinely restarted `rc worker`
	// process would: nothing here has run the job to completion or reported
	// its outcome.
	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1", State: model.DeviceReady}},
	))

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State, "the stranded job must be marked lost, not left running forever")
	require.NotEmpty(t, reloaded.KillReason, "the kill reason must name the cause")

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Empty(t, leases, "the job's lease must be released, not left live and unreachable")

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Len(t, devices, 1)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State,
		"a device backing a reaped in-flight job must be quarantined unhealthy, not ready and not busy — nothing proves an orphaned process isn't still holding it")

	// Quarantined means genuinely unschedulable, not merely mislabeled.
	_, err = s.Allocate(req("agent-b"))
	require.ErrorIs(t, err, store.ErrNoDevice)
}

// A device that was already unhealthy when its worker went silent past
// unhealthyAfter must still have its in-flight job reaped: it must not be
// stranded non-terminal with a lease that never releases.
func TestSweepReapsJobsOnAlreadyUnhealthyDevice(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.NoError(t, s.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, c.Now()))

	c.Advance(10 * time.Minute)
	_, err = s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Empty(t, leases)
}
