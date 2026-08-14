package store_test

import (
	"testing"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func labelled(t *testing.T, s *store.Store, deviceID string, kv map[string]string) {
	t.Helper()
	require.NoError(t, s.ReplaceLabels(deviceID, model.SourceDetected, kv, nowOf(s)))
}

func selectorEnq(submitter, sel string) store.EnqueueRequest {
	return store.EnqueueRequest{
		Selector:  sel,
		Command:   []string{"./bench"},
		Submitter: submitter,
	}
}

func TestEnqueueRejectsBothDeviceAndSelector(t *testing.T) {
	s, _ := newStore(t)
	r := selectorEnq("agent-a", "vendor=nvidia")
	r.DeviceID = "gpubox:gpu0"
	_, err := s.Enqueue(r)
	require.Error(t, err)
}

func TestEnqueueRejectsNeitherDeviceNorSelector(t *testing.T) {
	s, _ := newStore(t)
	r := selectorEnq("agent-a", "")
	_, err := s.Enqueue(r)
	require.Error(t, err)
}

func TestEnqueueRejectsASelectorMatchingNothing(t *testing.T) {
	s, _ := newStore(t)
	labelled(t, s, "gpubox:gpu0", map[string]string{"vendor": "nvidia"})

	_, err := s.Enqueue(selectorEnq("agent-a", "vendor=amd"))
	require.ErrorIs(t, err, store.ErrNoMatchingDevice)
}

func TestSelectorJobIsAssignedToAMatchingDevice(t *testing.T) {
	s, _ := newStore(t)
	labelled(t, s, "gpubox:gpu0", map[string]string{"vendor": "nvidia", "vram": "80G"})

	job, err := s.Enqueue(selectorEnq("agent-a", "vram>=40G"))
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, job.State)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, "gpubox:gpu0", assigned[0].DeviceID)
}

func TestSelectorJobSkipsANonMatchingDevice(t *testing.T) {
	s, _ := newStore(t)
	registerSecondDevice(t, s) // gpubox:gpu1
	labelled(t, s, "gpubox:gpu0", map[string]string{"vram": "24G"})
	labelled(t, s, "gpubox:gpu1", map[string]string{"vram": "80G"})

	_, err := s.Enqueue(selectorEnq("agent-a", "vram>=40G"))
	require.NoError(t, err)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, "gpubox:gpu1", assigned[0].DeviceID,
		"the 24G card must never be chosen for a >=40G selector")
}

// The reservation property: a selector job at the head holds its candidates.
func TestQueuedSelectorJobBlocksAPinnedJobBehindIt(t *testing.T) {
	s, _ := newStore(t)
	labelled(t, s, "gpubox:gpu0", map[string]string{"vram": "80G"})

	// Occupy the device.
	first, err := s.Enqueue(selectorEnq("agent-a", "vram>=40G"))
	require.NoError(t, err)
	_, err = s.ScheduleOnce()
	require.NoError(t, err)

	// Head of queue: another selector job waiting for the same device.
	_, err = s.Enqueue(selectorEnq("agent-b", "vram>=40G"))
	require.NoError(t, err)
	// Behind it: a pinned job for that exact device.
	_, err = s.Enqueue(enq("agent-c", 0))
	require.NoError(t, err)

	code := 0
	require.NoError(t, s.Release(first.ID, model.JobSucceeded, &code, ""))

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, "agent-b", assigned[0].Submitter,
		"the waiting selector job must not be overtaken by a pinned job behind it")
}

func TestMatchingDevicesIsSortedAndFiltered(t *testing.T) {
	s, _ := newStore(t)
	registerSecondDevice(t, s)
	labelled(t, s, "gpubox:gpu0", map[string]string{"vram": "80G"})
	labelled(t, s, "gpubox:gpu1", map[string]string{"vram": "24G"})

	got, err := s.MatchingDevices("vram>=40G")
	require.NoError(t, err)
	require.Equal(t, []string{"gpubox:gpu0"}, got)
}
