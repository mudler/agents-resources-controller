package store_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpsertHostDocStoresBodyAndTimestamp(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.UpsertHostDoc("gpubox", "", "# gpubox\nshared rack A1", c.Now()))

	body, at, err := s.HostDoc("gpubox", "")
	require.NoError(t, err)
	require.Equal(t, "# gpubox\nshared rack A1", body)
	require.Equal(t, c.Now(), at)
}

// A second upsert for the same (host, deviceID) replaces the body outright,
// not appends — the row is a live snapshot of the worker's disk, not a log.
func TestUpsertHostDocReplacesOnSecondCall(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.UpsertHostDoc("gpubox", "", "first version", c.Now()))
	c.Advance(0)
	require.NoError(t, s.UpsertHostDoc("gpubox", "", "second version", c.Now()))

	body, _, err := s.HostDoc("gpubox", "")
	require.NoError(t, err)
	require.Equal(t, "second version", body)
}

// The host-wide sheet (deviceID "") and a device's own sheet are independent
// rows: writing one must never clobber or be confused with the other.
func TestHostAndDeviceSheetsAreIndependent(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.UpsertHostDoc("gpubox", "", "host sheet", c.Now()))
	require.NoError(t, s.UpsertHostDoc("gpubox", "gpubox:gpu0", "gpu0 sheet", c.Now()))

	hostBody, _, err := s.HostDoc("gpubox", "")
	require.NoError(t, err)
	require.Equal(t, "host sheet", hostBody)

	deviceBody, _, err := s.HostDoc("gpubox", "gpubox:gpu0")
	require.NoError(t, err)
	require.Equal(t, "gpu0 sheet", deviceBody)
}

// A (host, deviceID) pair that was never written yields an empty body and
// the zero time, not an error — most hosts and most devices never write one.
func TestHostDocMissingIsNotAnError(t *testing.T) {
	s, _ := newStore(t)

	body, at, err := s.HostDoc("gpubox", "gpubox:gpu9")
	require.NoError(t, err)
	require.Empty(t, body)
	require.True(t, at.IsZero())
}
