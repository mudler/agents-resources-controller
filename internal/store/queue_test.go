package store_test

import (
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func enq(submitter string, priority int) store.EnqueueRequest {
	return store.EnqueueRequest{
		DeviceID:  "gpubox:gpu0",
		Command:   []string{"./bench"},
		Submitter: submitter,
		Priority:  priority,
	}
}

func TestEnqueueThenScheduleAssignsOneJob(t *testing.T) {
	s, _ := newStore(t)

	first, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, first.State)

	second, err := s.Enqueue(enq("agent-b", 0))
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, second.State)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, first.ID, assigned[0].ID, "FIFO: the first submitter wins")

	// The device is taken, so a second pass assigns nothing.
	again, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Empty(t, again)

	pos, err := s.QueuePosition(second.ID)
	require.NoError(t, err)
	require.Equal(t, 1, pos, "the waiting job is now at the head")
}

func TestHigherPriorityRunsFirst(t *testing.T) {
	s, _ := newStore(t)

	_, err := s.Enqueue(enq("agent-low", 0))
	require.NoError(t, err)
	high, err := s.Enqueue(enq("agent-high", 5))
	require.NoError(t, err)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, high.ID, assigned[0].ID)
}

func TestReleasingADeviceLetsTheNextQueuedJobRun(t *testing.T) {
	s, _ := newStore(t)

	first, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	second, err := s.Enqueue(enq("agent-b", 0))
	require.NoError(t, err)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Equal(t, first.ID, assigned[0].ID)

	code := 0
	require.NoError(t, s.Release(first.ID, model.JobSucceeded, &code, ""))

	assigned, err = s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, second.ID, assigned[0].ID)
}

// The invariant, now with a queue in front of it.
func TestSchedulingNeverProducesTwoLiveLeases(t *testing.T) {
	s, _ := newStore(t)

	for i := 0; i < 20; i++ {
		_, err := s.Enqueue(enq("agent", 0))
		require.NoError(t, err)
	}
	for i := 0; i < 20; i++ {
		if _, err := s.ScheduleOnce(); err != nil {
			t.Fatalf("schedule pass %d: %v", i, err)
		}
		leases, err := s.Leases()
		require.NoError(t, err)
		require.LessOrEqual(t, len(leases), 1, "pass %d produced %d live leases", i, len(leases))
	}
}

func TestCancelQueuedRemovesItFromTheQueue(t *testing.T) {
	s, _ := newStore(t)

	first, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	second, err := s.Enqueue(enq("agent-b", 0))
	require.NoError(t, err)

	ok, err := s.CancelQueued(first.ID, "user cancelled")
	require.NoError(t, err)
	require.True(t, ok)

	reloaded, err := s.Job(first.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobKilled, reloaded.State)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, second.ID, assigned[0].ID)
}

func TestCancelQueuedRefusesARunningJob(t *testing.T) {
	s, _ := newStore(t)

	job, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	_, err = s.ScheduleOnce()
	require.NoError(t, err)

	ok, err := s.CancelQueued(job.ID, "user cancelled")
	require.NoError(t, err)
	require.False(t, ok, "a job that already holds a device is not cancellable this way")
}

func TestQueuedJobOnUnhealthyDeviceIsNotAssigned(t *testing.T) {
	s, c := newStore(t)

	_, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	require.NoError(t, s.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, c.Now()))

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Empty(t, assigned, "an unhealthy device must not be scheduled onto")
}

func TestIdempotentEnqueueReturnsTheSameJob(t *testing.T) {
	s, _ := newStore(t)

	r := enq("agent-a", 0)
	r.IdempotencyKey = "abc123"

	first, err := s.Enqueue(r)
	require.NoError(t, err)
	second, err := s.Enqueue(r)
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)

	queued, err := s.QueuedJobs()
	require.NoError(t, err)
	require.Len(t, queued, 1)
}

func TestEnqueueRejectsRuntimeAboveTheDeviceCeiling(t *testing.T) {
	s, _ := newStore(t)
	require.NoError(t, s.SetDeviceMaxRuntime("gpubox:gpu0", time.Hour))

	r := enq("agent-a", 0)
	r.MaxRuntime = 4 * time.Hour

	_, err := s.Enqueue(r)
	require.ErrorIs(t, err, store.ErrRuntimeAboveCeiling)
	require.Contains(t, err.Error(), "1h0m0s", "the error must name the ceiling")
}
