package store_test

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func newStore(t *testing.T) (*store.Store, *clock.Fake) {
	t.Helper()
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))
	s, err := store.Open(filepath.Join(t.TempDir(), "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1", State: model.DeviceReady}},
	))
	return s, c
}

func req(submitter string) store.AllocateRequest {
	return store.AllocateRequest{
		DeviceID:  "gpubox:gpu0",
		Command:   []string{"./bench"},
		Submitter: submitter,
		LeaseTTL:  time.Minute,
	}
}

// nowOf exposes the fake clock's current time to tests that need to stamp a
// row directly.
func nowOf(s *store.Store) time.Time { return s.Now() }

// registerSecondDevice adds gpubox:gpu1 to the worker newStore created.
func registerSecondDevice(t *testing.T, s *store.Store) {
	t.Helper()
	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-1", LastHeartbeatAt: s.Now()},
		[]model.Device{
			{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"},
			{ID: "gpubox:gpu1", Host: "gpubox", Name: "gpu1", WorkerID: "w1"},
		},
	))
}

func TestAllocateGrantsDeviceOnce(t *testing.T) {
	s, _ := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.Equal(t, model.JobAssigned, job.State)
	require.Equal(t, "gpubox:gpu0", job.DeviceID)

	_, err = s.Allocate(req("agent-b"))
	require.ErrorIs(t, err, store.ErrNoDevice)
}

func TestReleaseReturnsDeviceToPool(t *testing.T) {
	s, _ := newStore(t)

	first, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	code := 0
	require.NoError(t, s.Release(first.ID, model.JobSucceeded, &code, ""))

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)

	second, err := s.Allocate(req("agent-b"))
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID)
}

// The invariant: concurrent claimants, exactly one winner, no torn state.
func TestConcurrentAllocateYieldsExactlyOneWinner(t *testing.T) {
	s, _ := newStore(t)

	const claimants = 32
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		granted int
		denied  int
	)
	start := make(chan struct{})

	for i := 0; i < claimants; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.Allocate(req("agent"))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				granted++
			case errors.Is(err, store.ErrNoDevice):
				denied++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, 1, granted)
	require.Equal(t, claimants-1, denied)

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Len(t, leases, 1)
}

func TestAllocateSkipsUnhealthyDevice(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, c.Now()))

	_, err := s.Allocate(req("agent-a"))
	require.ErrorIs(t, err, store.ErrNoDevice)
}

func TestIdempotentSubmitReturnsSameJob(t *testing.T) {
	s, _ := newStore(t)

	r := req("agent-a")
	r.IdempotencyKey = "abc123"

	first, err := s.Allocate(r)
	require.NoError(t, err)

	second, err := s.Allocate(r)
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Len(t, leases, 1)
}
