package store_test

import (
	"testing"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func deviceIDs(t *testing.T, s *store.Store) []string {
	t.Helper()
	devices, err := s.Devices()
	require.NoError(t, err)
	out := make([]string, 0, len(devices))
	for _, d := range devices {
		out = append(out, d.ID)
	}
	return out
}

// Retiring is the operation that was missing: a box is decommissioned, or a
// card is pulled, and until now the only way to get it out of `rc devices`
// was to edit the database by hand.
func TestRetireRemovesTheDeviceAndItsLabels(t *testing.T) {
	s, c := newStore(t)
	registerSecondDevice(t, s)
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vram": "80G"}, c.Now()))

	require.NoError(t, s.RetireDevice("gpubox:gpu0"))

	ids := deviceIDs(t, s)
	require.NotContains(t, ids, "gpubox:gpu0")
	require.Contains(t, ids, "gpubox:gpu1", "retiring one device must not touch its neighbours")

	labels, err := s.AllLabels()
	require.NoError(t, err)
	require.Empty(t, labels["gpubox:gpu0"], "a retired device must not leave labels behind")
}

// A device someone is using is not one you can retire out from under them:
// the lease is the whole guarantee, and deleting the row would strand a
// running job on hardware the controller no longer believes exists.
func TestRetireRefusesWhileSomethingHoldsIt(t *testing.T) {
	s, _ := newStore(t)
	_, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	require.ErrorIs(t, s.RetireDevice("gpubox:gpu0"), store.ErrDeviceBusy)
	require.Contains(t, deviceIDs(t, s), "gpubox:gpu0",
		"the refusal must leave the device exactly as it was")
}

// An unhealthy device is the most common thing anyone wants to retire — the
// card died, so it is already out of the pool and on its way out of the
// fleet. It holds no lease, so it retires.
func TestRetireWorksOnAnUnhealthyDevice(t *testing.T) {
	s, c := newStore(t)
	require.NoError(t, s.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, c.Now(),
		"card fell off the bus"))

	require.NoError(t, s.RetireDevice("gpubox:gpu0"))
	require.Empty(t, deviceIDs(t, s))
}

func TestRetireReportsAnUnknownDevice(t *testing.T) {
	s, _ := newStore(t)
	require.ErrorIs(t, s.RetireDevice("gpubox:nope"), store.ErrDeviceNotFound)
}

// Job history outlives the hardware. "which box ran this, and how did it go"
// must stay answerable after the box is gone — that is exactly when it gets
// asked.
func TestRetireKeepsJobHistory(t *testing.T) {
	s, _ := newStore(t)
	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.NoError(t, s.Release(job.ID, model.JobSucceeded, nil, ""))

	require.NoError(t, s.RetireDevice("gpubox:gpu0"))

	got, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, "gpubox:gpu0", got.DeviceID,
		"the job must still name the device it ran on, even though it is gone")
}
