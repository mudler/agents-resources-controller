package store_test

import (
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/stretchr/testify/require"
)

// TestRecentJobsForDeviceOrdersNewestFirstAndLimits backs `rc describe`'s
// job history: it must come back most-recent-first (submission order, not
// insertion accident) and respect the limit the caller passes, since
// describe only ever wants the last handful, not a whole device's lifetime.
func TestRecentJobsForDeviceOrdersNewestFirstAndLimits(t *testing.T) {
	s, c := newStore(t)

	var ids []string
	for i := 0; i < 4; i++ {
		job, err := s.Allocate(req("agent-a"))
		require.NoError(t, err)
		code := 0
		require.NoError(t, s.Release(job.ID, model.JobSucceeded, &code, ""))
		ids = append(ids, job.ID)
		c.Advance(time.Minute) // distinct submitted_at per job
	}

	recent, err := s.RecentJobsForDevice("gpubox:gpu0", 2)
	require.NoError(t, err)
	require.Len(t, recent, 2, "must respect the limit")
	require.Equal(t, ids[3], recent[0].ID, "newest job must come first")
	require.Equal(t, ids[2], recent[1].ID)
}

// TestRecentJobsForDeviceOnUnknownDeviceIsEmpty pins the boring case: a
// device with no job history at all (or one nobody ever heard of) yields an
// empty slice, not an error — `rc describe` on a freshly registered device
// must not fail just because nothing has ever run on it.
func TestRecentJobsForDeviceOnUnknownDeviceIsEmpty(t *testing.T) {
	s, _ := newStore(t)

	recent, err := s.RecentJobsForDevice("gpubox:does-not-exist", 5)
	require.NoError(t, err)
	require.Empty(t, recent)
}
