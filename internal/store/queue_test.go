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

	var ids []string
	for i := 0; i < 20; i++ {
		job, err := s.Enqueue(enq("agent", 0))
		require.NoError(t, err)
		ids = append(ids, job.ID)
	}

	var totalAssigned int
	for i := 0; i < 20; i++ {
		assigned, err := s.ScheduleOnce()
		if err != nil {
			t.Fatalf("schedule pass %d: %v", i, err)
		}
		totalAssigned += len(assigned)
		leases, err := s.Leases()
		require.NoError(t, err)
		require.LessOrEqual(t, len(leases), 1, "pass %d produced %d live leases", i, len(leases))
	}

	// The lease-count assertion above can never trip: the partial unique
	// index makes a second live lease impossible regardless of what
	// ScheduleOnce does, even a no-op. These two assertions are what
	// actually exercise scheduling: exactly one of the twenty queued jobs
	// ever gets the single device, and it is the one FIFO ordering picked.
	require.Equal(t, 1, totalAssigned, "exactly one job should ever be assigned onto the single device")

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Len(t, leases, 1)
	require.Equal(t, ids[0], leases[0].JobID, "FIFO: the first job enqueued is the one holding the device")
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

// raceCancelClock deterministically reproduces "CancelQueued lands between
// ScheduleOnce's queue snapshot and the moment it reaches that job" without
// goroutines or timing luck. assignQueued's very first statement is
// s.clock.Now() — reached right after ScheduleOnce already has its queued
// snapshot, and before assignQueued has touched the database at all. Firing
// the cancel from inside that Now() call lands it in exactly the window an
// HTTP CancelQueued request would race into, single-threaded and every time.
type raceCancelClock struct {
	*clock.Fake
	store *store.Store
	jobID string
	armed bool

	fired     bool
	cancelOK  bool
	cancelErr error
}

func (c *raceCancelClock) Now() time.Time {
	if c.armed && !c.fired {
		c.fired = true // set before calling in: CancelQueued reads the clock too, and must not re-arm.
		c.cancelOK, c.cancelErr = c.store.CancelQueued(c.jobID, "raced cancel")
	}
	return c.Fake.Now()
}

// A job that vanishes between the scheduling pass's snapshot and the moment
// it is reached must not reserve its device for itself: that would idle a
// free device for a whole tick and strand a reservation row nobody deletes.
func TestCancelledHeadJobDoesNotBlockTheQueue(t *testing.T) {
	c := &raceCancelClock{Fake: clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))}

	dbPath := filepath.Join(t.TempDir(), "rc.db")
	s, err := store.Open(dbPath, c)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	c.store = s

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1", State: model.DeviceReady}},
	))

	first, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	second, err := s.Enqueue(enq("agent-b", 0))
	require.NoError(t, err)

	// Arm the trap for the next clock read: assignQueued(first)'s.
	c.jobID = first.ID
	c.armed = true

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.True(t, c.fired, "test is broken: the race clock never fired")
	require.NoError(t, c.cancelErr)
	require.True(t, c.cancelOK, "CancelQueued must have found A still queued at the moment it raced in")

	require.Len(t, assigned, 1, "the cancelled head job must not block the pass from assigning the job behind it")
	require.Equal(t, second.ID, assigned[0].ID, "B must be assigned in this single pass, not the next one")

	raw, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer raw.Close()
	var n int
	require.NoError(t, raw.QueryRow(`SELECT COUNT(*) FROM reservations WHERE job_id = ?`, first.ID).Scan(&n))
	require.Equal(t, 0, n, "no reservation row should remain for the cancelled job")
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
