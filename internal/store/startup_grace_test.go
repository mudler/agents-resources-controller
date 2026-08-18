package store_test

import (
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// These tests are the controller-restart defect of 2026-08-18, in the store.
//
// The controller was restarted to pick up a new image and was down for a few
// seconds. Its first sweep found a lease whose expires_at had passed DURING
// that downtime, expired it, quarantined the device and marked a perfectly
// healthy job lost — the holder had been alive the whole time and had simply
// had nobody to renew against while the controller was gone.
//
// A deadline that lapsed while nothing was listening is not evidence about
// the holder, so Sweep is given an instant before which it must not conclude
// anything destructive. What it must NOT become is an amnesty: a lease whose
// holder is genuinely gone still expires the moment the window closes.

// holdOff is a hold-off instant far enough ahead that a sweep run "now"
// falls inside it. It is deliberately not the caller's own grace/unhealthy
// thresholds: the hold-off is about when the CONTROLLER started, not about
// how long a worker has been quiet.
func holdOff(s *store.Store) time.Time { return s.Now().Add(30 * time.Second) }

func TestLeaseThatLapsedDuringControllerDowntimeSurvivesTheStartupGrace(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.NoError(t, s.MarkRunning(job.ID, c.Now()))

	// The controller was down for 90 seconds; the lease's minute-long TTL
	// lapsed somewhere in the middle of that.
	c.Advance(90 * time.Second)

	res, err := s.Sweep(30*time.Second, 5*time.Minute, holdOff(s))
	require.NoError(t, err)
	require.Empty(t, res.LeasesExpired,
		"a deadline that lapsed while the controller was not running is not evidence the holder is gone")

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Len(t, leases, 1, "the lease must still be live so its holder can renew it")

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobRunning, reloaded.State, "the job was running fine; nothing happened to it")

	require.NotEqual(t, model.DeviceUnhealthy, stateOfDevice(t, s, "gpubox:gpu0"),
		"the device must not be quarantined for a deadline nobody could renew against")
}

// The whole point of the window: the holder gets a chance to renew, and once
// it has, the lease is simply live again and outlives the window.
func TestHolderThatRenewsInsideTheStartupGraceKeepsItsLease(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.NoError(t, s.MarkRunning(job.ID, c.Now()))

	c.Advance(90 * time.Second)
	until := holdOff(s)

	_, err = s.Sweep(30*time.Second, 5*time.Minute, until)
	require.NoError(t, err)

	// The worker's first heartbeat after the controller came back, naming
	// the job it is actually supervising.
	require.NoError(t, s.RecordHeartbeat("w1", c.Now(), []string{job.ID}))

	// The window closes, with the worker heartbeating on its own schedule
	// throughout. Nothing is owed to this lease any more: it is live on its
	// own merits.
	c.Advance(31 * time.Second)
	require.NoError(t, s.RecordHeartbeat("w1", c.Now(), []string{job.ID}))

	res, err := s.Sweep(30*time.Second, 5*time.Minute, until)
	require.NoError(t, err)
	require.Empty(t, res.LeasesExpired)

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobRunning, reloaded.State)
	require.Equal(t, model.DeviceBusy, stateOfDevice(t, s, "gpubox:gpu0"))
}

// The grace must not become an amnesty. A holder that never came back is
// still written off — just after the window instead of during it.
func TestLapsedLeaseWithNoHolderIsExpiredOnceTheStartupGraceCloses(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.NoError(t, s.MarkRunning(job.ID, c.Now()))

	c.Advance(90 * time.Second)
	until := holdOff(s)

	_, err = s.Sweep(30*time.Second, 5*time.Minute, until)
	require.NoError(t, err)

	// Nobody renewed anything. The window closes.
	c.Advance(31 * time.Second)
	res, err := s.Sweep(30*time.Second, 5*time.Minute, until)
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired)

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)
	require.Equal(t, "lease expired", reloaded.KillReason)
	require.Equal(t, model.DeviceUnhealthy, stateOfDevice(t, s, "gpubox:gpu0"))
}

// Worker silence is measured against workers.last_heartbeat_at, which is a
// STORED timestamp: after a restart it dates from before the outage, so the
// first sweep of a controller that was down longer than unhealthyAfter would
// otherwise write off every device in the fleet and lose every job on it —
// the same defect as the lease one, one table over. Demotion to unknown is
// left alone: it is not a quarantine, and the next heartbeat undoes it.
func TestSilentWorkerIsNotWrittenOffInsideTheStartupGrace(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.NoError(t, s.MarkRunning(job.ID, c.Now()))

	// Down for ten minutes — twice unhealthyAfter.
	c.Advance(10 * time.Minute)
	until := holdOff(s)

	res, err := s.Sweep(30*time.Second, 5*time.Minute, until)
	require.NoError(t, err)
	require.Empty(t, res.DevicesUnhealthy,
		"silence spanning an outage is not evidence the worker is gone")
	require.Empty(t, res.JobsLost)
	require.Equal(t, []string{"gpubox:gpu0"}, res.DevicesUnknown,
		"unknown is still right: we genuinely have not heard from it since we started")
	require.Equal(t, model.DeviceUnknown, stateOfDevice(t, s, "gpubox:gpu0"))

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobRunning, reloaded.State)
}

func TestSilentWorkerIsWrittenOffOnceTheStartupGraceCloses(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.NoError(t, s.MarkRunning(job.ID, c.Now()))

	c.Advance(10 * time.Minute)
	until := holdOff(s)

	_, err = s.Sweep(30*time.Second, 5*time.Minute, until)
	require.NoError(t, err)

	c.Advance(31 * time.Second)
	res, err := s.Sweep(30*time.Second, 5*time.Minute, until)
	require.NoError(t, err)
	require.Equal(t, []string{"gpubox:gpu0"}, res.DevicesUnhealthy)
	require.Equal(t, []string{job.ID}, res.JobsLost)
	require.Equal(t, model.DeviceUnhealthy, stateOfDevice(t, s, "gpubox:gpu0"))

	reasons, err := s.QuarantineReasons([]string{"gpubox:gpu0"})
	require.NoError(t, err)
	require.Equal(t, "worker_lost", reasons["gpubox:gpu0"])
}

// A zero hold-off is what every caller that has no startup to wait out
// passes, and it must mean "judge normally" rather than "hold off forever".
func TestZeroHoldOffJudgesImmediately(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	c.Advance(90 * time.Second)
	res, err := s.Sweep(30*time.Second, 5*time.Minute, time.Time{})
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired)
}

// stateOfDevice is the device's current state, by ID.
func stateOfDevice(t *testing.T, s *store.Store, id string) model.DeviceState {
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
