package store_test

import (
	"errors"
	"testing"

	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// WorkerHost is what handlePushLabels (internal/server) uses to verify a
// request's claimed host actually matches the worker ID in the URL, so a
// typo'd or stale worker ID can never silently write labels for the wrong
// host — or for no host at all.
func TestWorkerHostReturnsTheRegisteredHost(t *testing.T) {
	s, _ := newStore(t) // registers worker "w1" as host "gpubox"

	host, err := s.WorkerHost("w1")
	require.NoError(t, err)
	require.Equal(t, "gpubox", host)
}

func TestWorkerHostForUnknownIDReturnsErrWorkerNotFound(t *testing.T) {
	s, _ := newStore(t)

	_, err := s.WorkerHost("does-not-exist")
	require.Error(t, err)
	require.True(t, errors.Is(err, store.ErrWorkerNotFound))
}
