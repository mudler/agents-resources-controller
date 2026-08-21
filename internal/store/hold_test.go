package store_test

import (
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// holdReq builds an EnqueueRequest for a hold on gpubox:gpu0 — the device
// newStore always registers. A hold is a job with kind "hold": the command
// here is the same fixed placeholder the server would substitute (see
// server.handleSubmit), never anything the caller chooses — the store layer
// does not enforce that itself (the submit handler does), so tests below
// exercise the store's own responsibilities: occupancy, the ceiling check,
// and carrying kind/reason through.
func holdReq(submitter, reason string, ttl time.Duration) store.EnqueueRequest {
	return store.EnqueueRequest{
		DeviceID:   "gpubox:gpu0",
		Command:    []string{"hold"},
		Submitter:  submitter,
		Kind:       model.LeaseKindHold,
		Reason:     reason,
		MaxRuntime: ttl,
	}
}

// TestHoldOccupiesDeviceAndQueuesAJobBehindIt is the central claim of task
// 8's design: a hold is a job, so it goes through the exact same allocation
// and queue machinery as any other job — including making an ordinary job
// submitted afterward wait behind it.
func TestHoldOccupiesDeviceAndQueuesAJobBehindIt(t *testing.T) {
	s, _ := newStore(t)

	hold, err := s.Enqueue(holdReq("mudler", "manual profiling", 30*time.Minute))
	require.NoError(t, err)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, hold.ID, assigned[0].ID, "the hold itself is what gets assigned the device")
	require.Equal(t, model.JobAssigned, assigned[0].State)

	job, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, job.State, "the device is held, so the job must queue")

	again, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Empty(t, again, "the hold still occupies the device; nothing new should be assigned")

	pos, err := s.QueuePosition(job.ID)
	require.NoError(t, err)
	require.Equal(t, 1, pos, "the job is at the head of the queue behind the hold")
}

// TestHoldTTLAboveDeviceCeilingIsRejected pins the design note that a hold's
// TTL is capped by the device's declared max_runtime exactly as a job's
// MaxRuntime is — rejected outright, never silently clamped down to what the
// device will actually tolerate.
func TestHoldTTLAboveDeviceCeilingIsRejected(t *testing.T) {
	s, _ := newStore(t)
	require.NoError(t, s.SetDeviceMaxRuntime("gpubox:gpu0", time.Hour))

	_, err := s.Enqueue(holdReq("mudler", "too long", 2*time.Hour))
	require.ErrorIs(t, err, store.ErrRuntimeAboveCeiling)
}

// TestKillingAHoldFreesTheDevice exercises rc release's underlying path:
// RequestKill flags the job (exactly as rc kill would), and the terminal
// report a worker would send after honouring that flag — modelled here by
// calling Release directly, precisely as worker.execute does once Run
// returns — frees the device and clears its lease. No new mechanism: this
// is the exact path an ordinary job's kill already takes.
func TestKillingAHoldFreesTheDevice(t *testing.T) {
	s, _ := newStore(t)

	hold, err := s.Enqueue(holdReq("mudler", "manual profiling", 30*time.Minute))
	require.NoError(t, err)
	_, err = s.ScheduleOnce()
	require.NoError(t, err)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Len(t, devices, 1)
	require.Equal(t, model.DeviceBusy, devices[0].State, "the hold must occupy the device once granted")

	outcome, err := s.RequestKill(hold.ID, "released by rc release", false)
	require.NoError(t, err)
	require.Equal(t, store.KillRequested, outcome, "RequestKill must flag a hold exactly like an ordinary running job")

	require.NoError(t, s.Release(hold.ID, model.JobKilled, nil, "released by rc release"))

	devices, err = s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State, "releasing the hold must return the device to the pool")

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Empty(t, leases, "no live lease should remain once the hold is released")
}

// TestHoldAppearsInQueuedAndActiveJobsWithKindAndReason pins the field-level
// claim: QueuedJobs and ActiveJobs must report a hold's kind and reason
// verbatim, and — the guard against a test that would pass even if Kind
// always defaulted to "hold" — an ordinary job enqueued alongside it must
// report kind "job", not "hold".
func TestHoldAppearsInQueuedAndActiveJobsWithKindAndReason(t *testing.T) {
	s, _ := newStore(t)

	hold, err := s.Enqueue(holdReq("mudler", "manual profiling", 30*time.Minute))
	require.NoError(t, err)

	queued, err := s.QueuedJobs()
	require.NoError(t, err)
	require.Len(t, queued, 1)
	require.Equal(t, model.LeaseKindHold, queued[0].Kind, "a queued hold must report kind=hold")
	require.Equal(t, "manual profiling", queued[0].Reason, "a queued hold must carry its reason")

	_, err = s.ScheduleOnce()
	require.NoError(t, err)

	active, err := s.ActiveJobs()
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, hold.ID, active[0].ID)
	require.Equal(t, model.LeaseKindHold, active[0].Kind, "an active hold must report kind=hold")
	require.Equal(t, "manual profiling", active[0].Reason, "an active hold must carry its reason")

	_, err = s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	queued, err = s.QueuedJobs()
	require.NoError(t, err)
	require.Len(t, queued, 1)
	require.Equal(t, model.LeaseKindJob, queued[0].Kind, "an ordinary job must report kind=job, not hold")
	require.Empty(t, queued[0].Reason, "an ordinary job must not carry a hold's reason")
}
