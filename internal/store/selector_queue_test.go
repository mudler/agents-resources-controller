package store_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
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

// The reservation property: a selector job at the head that genuinely
// cannot be placed holds its only candidate, so a pinned job behind it
// cannot take that device out from under it.
//
// The original version of this test (see fix-round-1 review, Important 3)
// enqueued the occupying job through Enqueue+ScheduleOnce and released it
// *before* calling ScheduleOnce again, so by the time the pass that matters
// ran, gpu0 was already free and both jobs were assignable. Because
// QueuedJobs always returns jobs in strict FIFO order and a fresh pass never
// carries a reservation over from the last one, the earlier job in the
// queue is *always* tried first regardless of whether the reservation
// mechanism does anything at all — the review confirmed this by stubbing
// out the reservation skip entirely and watching the test still pass. That
// made it a test of FIFO ordering, not of reservations.
//
// This version keeps gpu0 genuinely busy (via the Allocate bypass, which
// never enters the queue) through the pass where agent-b fails to place, so
// the reservation this task adds is exercised for real: the assertion on
// the raw reservations row below is what actually distinguishes "the
// scheduler is holding gpu0 for agent-b" from "the scheduler dropped it and
// FIFO ordering happened to save us anyway." Verified by temporarily
// removing the `s.reserve` call in ScheduleOnce's "nothing free" branch:
// the row-count assertion below goes red (0 rows, not 1) while the final
// placement assertion still happens to pass, exactly confirming that this
// row check — not the eventual winner — is what proves the reservation
// fired. See the fix-round-1 report for the exact command and output.
func TestQueuedSelectorJobBlocksAPinnedJobBehindIt(t *testing.T) {
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))
	dbPath := filepath.Join(t.TempDir(), "rc.db")
	s, err := store.Open(dbPath, c)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1", State: model.DeviceReady}},
	))
	labelled(t, s, "gpubox:gpu0", map[string]string{"vram": "80G"})

	// Occupy the device through the queue-bypassing path, which never
	// enters the queue at all — so gpu0 stays busy through the pass below
	// without a third queued job complicating the picture.
	occupier, err := s.Allocate(req("occupier"))
	require.NoError(t, err)

	// Head of queue: a selector job that can only land on gpu0, which is
	// busy right now.
	head, err := s.Enqueue(selectorEnq("agent-b", "vram>=40G"))
	require.NoError(t, err)
	// Behind it: a pinned job for that exact device.
	_, err = s.Enqueue(enq("agent-c", 0))
	require.NoError(t, err)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Empty(t, assigned, "gpu0 is still busy; nothing should be assigned this pass")

	raw, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer raw.Close()
	var n int
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM reservations WHERE job_id = ? AND device_id = ?`,
		head.ID, "gpubox:gpu0").Scan(&n))
	require.Equal(t, 1, n, "the selector job must reserve its only candidate while it cannot be placed")

	code := 0
	require.NoError(t, s.Release(occupier.ID, model.JobSucceeded, &code, ""))

	assigned, err = s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, "agent-b", assigned[0].Submitter,
		"the waiting selector job must not be overtaken by a pinned job behind it")
}

// The exact interleaving fix-round-1's Important 1 flagged: a selector
// job's reservation must not be all-or-nothing. A(vram>=80G) matches only
// gpu1, which is busy, and reserves it. B(vram>=24G), directly behind A,
// matches BOTH gpu0 (free) and gpu1 (reserved by A) — it must still be
// able to take gpu0, the one candidate nobody ahead of it has claimed.
// Before the fix, any reserved candidate skipped the whole job, so B
// reserved nothing and C — a job pinned to gpu0, behind B — won gpu0 out
// from under it instead.
func TestSelectorJobTakesAnUnreservedCandidateDespiteAnOverlappingReservation(t *testing.T) {
	s, _ := newStore(t)
	registerSecondDevice(t, s) // gpubox:gpu1
	labelled(t, s, "gpubox:gpu0", map[string]string{"vram": "24G"})
	labelled(t, s, "gpubox:gpu1", map[string]string{"vram": "80G"})

	// Occupy gpu1 so A cannot place there and must reserve it instead.
	_, err := s.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu1", Command: []string{"./bench"}, Submitter: "occupier", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	_, err = s.Enqueue(selectorEnq("agent-a", "vram>=80G")) // matches gpu1 only
	require.NoError(t, err)
	b, err := s.Enqueue(selectorEnq("agent-b", "vram>=24G")) // matches gpu0 and gpu1
	require.NoError(t, err)
	_, err = s.Enqueue(enq("agent-c", 0)) // pinned gpu0, behind B
	require.NoError(t, err)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1, "gpu1 stays busy; only gpu0 is available to hand out this pass")
	require.Equal(t, b.ID, assigned[0].ID)
	require.Equal(t, "gpubox:gpu0", assigned[0].DeviceID,
		"B must take the one candidate nobody ahead of it has claimed, not be starved by A's overlapping reservation on gpu1")
}

// fix-round-1's Important 2: a per-device runtime ceiling is a declaration
// by whoever owns the box about what it tolerates, and a pinned submit is
// rejected outright rather than clamped if it asks for more (see
// TestEnqueueRejectsRuntimeAboveTheDeviceCeiling). A selector must not be a
// way around that: a selector job asking for more runtime than every
// matching device's ceiling allows must be refused at submit, exactly like
// the pinned case, naming the ceiling it exceeded.
func TestEnqueueRejectsASelectorAboveEveryMatchingDeviceCeiling(t *testing.T) {
	s, _ := newStore(t)
	require.NoError(t, s.SetDeviceMaxRuntime("gpubox:gpu0", time.Hour))
	labelled(t, s, "gpubox:gpu0", map[string]string{"vram": "80G"})

	r := selectorEnq("agent-a", "vram>=40G")
	r.MaxRuntime = 4 * time.Hour

	_, err := s.Enqueue(r)
	require.ErrorIs(t, err, store.ErrRuntimeAboveCeiling)
	require.Contains(t, err.Error(), "1h0m0s", "the error must name the ceiling exceeded")
}

// The other half of fix-round-1's Important 2: a selector job that lands on
// a device whose ceiling is lower than what it asked for must never
// actually be scheduled there, even if that device only stops qualifying
// after submit (e.g. a broader selector where a second, more permissive
// device also matches). ScheduleOnce must apply the same ceiling filter
// Enqueue does, not just Enqueue.
func TestScheduleOnceNeverPlacesASelectorJobOnADeviceBelowItsCeiling(t *testing.T) {
	s, _ := newStore(t)
	registerSecondDevice(t, s) // gpubox:gpu1
	labelled(t, s, "gpubox:gpu0", map[string]string{"vram": "80G"})
	labelled(t, s, "gpubox:gpu1", map[string]string{"vram": "80G"})
	require.NoError(t, s.SetDeviceMaxRuntime("gpubox:gpu0", 30*time.Minute))

	r := selectorEnq("agent-a", "vram>=40G")
	r.MaxRuntime = time.Hour
	job, err := s.Enqueue(r)
	require.NoError(t, err, "gpu1 alone tolerates the requested hour, so submit must succeed")

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, job.ID, assigned[0].ID)
	require.Equal(t, "gpubox:gpu1", assigned[0].DeviceID,
		"gpu0's 30m ceiling cannot carry a 1h request even though it matches the selector")
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
