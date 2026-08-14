package server_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/logstore"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func TestSubmitQueuesWhenDeviceIsBusy(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	defer first.Body.Close()
	require.Equal(t, http.StatusCreated, first.StatusCode)

	// Give the first job the device.
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	second := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	defer second.Body.Close()
	require.Equal(t, http.StatusCreated, second.StatusCode, "a busy device queues, it does not 409")

	var job model.Job
	require.NoError(t, json.NewDecoder(second.Body).Decode(&job))
	require.Equal(t, model.JobQueued, job.State)
}

func TestSubmitWithNoWaitStillFailsFast(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	first.Body.Close()
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	second := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b", NoWait: true,
	})
	defer second.Body.Close()
	require.Equal(t, http.StatusConflict, second.StatusCode)
}

func TestJobViewReportsQueuePosition(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	a.Body.Close()
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	b := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	var queued model.Job
	require.NoError(t, json.NewDecoder(b.Body).Decode(&queued))
	b.Body.Close()

	view := get(t, ts, "ctok", "/v1/jobs/"+queued.ID)
	defer view.Body.Close()
	var out server.JobView
	require.NoError(t, json.NewDecoder(view.Body).Decode(&out))
	require.Equal(t, 1, out.QueuePosition)
	require.Equal(t, model.JobQueued, out.Job.State)
}

func TestSubmitRejectsRuntimeAboveDeviceCeiling(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)
	require.NoError(t, st.SetDeviceMaxRuntime("gpubox:gpu0", time.Hour))

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
		MaxRuntimeSeconds: 4 * 3600,
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "runtime_above_ceiling", body["error"])
}

func TestKillCancelsAQueuedJob(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	a.Body.Close()
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	b := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	var queued model.Job
	require.NoError(t, json.NewDecoder(b.Body).Decode(&queued))
	b.Body.Close()

	kill := post(t, ts, "ctok", "/v1/jobs/"+queued.ID+"/kill", map[string]string{"submitter": "agent-b"})
	defer kill.Body.Close()
	require.Equal(t, http.StatusOK, kill.StatusCode)

	reloaded, err := st.Job(queued.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobKilled, reloaded.State)
}

func TestKillRefusesSomeoneElsesJob(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	var job model.Job
	require.NoError(t, json.NewDecoder(a.Body).Decode(&job))
	a.Body.Close()

	kill := post(t, ts, "ctok", "/v1/jobs/"+job.ID+"/kill", map[string]string{"submitter": "agent-b"})
	defer kill.Body.Close()
	require.Equal(t, http.StatusForbidden, kill.StatusCode)

	reloaded, err := st.Job(job.ID)
	require.NoError(t, err)
	require.NotEqual(t, model.JobKilled, reloaded.State)
}

// TestQueuedJobIsAssignedWhenTheDeviceFrees is the task's headline
// behaviour: a job that queued behind a busy device must actually get that
// device once it frees, not just sit there forever with a queue position.
func TestQueuedJobIsAssignedWhenTheDeviceFrees(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	var jobA model.Job
	require.NoError(t, json.NewDecoder(a.Body).Decode(&jobA))
	a.Body.Close()
	require.Equal(t, model.JobAssigned, jobA.State, "the free device is handed out on submit")

	b := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	var jobB model.Job
	require.NoError(t, json.NewDecoder(b.Body).Decode(&jobB))
	b.Body.Close()
	require.Equal(t, model.JobQueued, jobB.State)

	code := 0
	require.NoError(t, st.Release(jobA.ID, model.JobSucceeded, &code, ""))

	assigned, err := st.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, jobB.ID, assigned[0].ID, "the queued job must be the one that gets the freed device")

	reloaded, err := st.Job(jobB.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobAssigned, reloaded.State)
	require.Equal(t, "gpubox:gpu0", reloaded.DeviceID)

	pos, err := st.QueuePosition(jobB.ID)
	require.NoError(t, err)
	require.Equal(t, 0, pos, "an assigned job is no longer queued")
}

// TestStateIncludesQueuedJobs proves StateResponse.Queued actually carries
// queued jobs rather than being an always-empty field nobody ever populates.
func TestStateIncludesQueuedJobs(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	a.Body.Close()

	b := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	var queued model.Job
	require.NoError(t, json.NewDecoder(b.Body).Decode(&queued))
	b.Body.Close()
	require.Equal(t, model.JobQueued, queued.State)

	resp := get(t, ts, "ctok", "/v1/state")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var state server.StateResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&state))
	require.Len(t, state.Queued, 1)
	require.Equal(t, queued.ID, state.Queued[0].ID)
}

// TestAdminCanKillAnotherSubmittersJob checks both sides of the ownership
// check on the same job: a client token acting on someone else's job is
// still refused, and an admin token in the exact same position succeeds —
// with a kill reason that names the admin override rather than an empty or
// unrelated submitter string.
func TestAdminCanKillAnotherSubmittersJob(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	a.Body.Close()

	b := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	var queued model.Job
	require.NoError(t, json.NewDecoder(b.Body).Decode(&queued))
	b.Body.Close()
	require.Equal(t, model.JobQueued, queued.State)

	clientKill := post(t, ts, "ctok", "/v1/jobs/"+queued.ID+"/kill", map[string]string{"submitter": "agent-c"})
	defer clientKill.Body.Close()
	require.Equal(t, http.StatusForbidden, clientKill.StatusCode,
		"a client token acting on someone else's job must still be refused")

	adminKill := post(t, ts, "atok", "/v1/jobs/"+queued.ID+"/kill", map[string]string{"submitter": "agent-c"})
	defer adminKill.Body.Close()
	require.Equal(t, http.StatusOK, adminKill.StatusCode, "an admin token overrides ownership")

	reloaded, err := st.Job(queued.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobKilled, reloaded.State)
	require.Contains(t, reloaded.KillReason, "admin",
		"the kill reason must name the admin override, not the mismatched submitter")
}

// TestRequestKillFlagsARunningJob proves RequestKill's actual effect: it has
// no caller yet outside handleKill (the worker side that acts on it arrives
// in a later task), so this is the only coverage that the flag it sets is
// the one TakeKillRequests reads back.
func TestRequestKillFlagsARunningJob(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	var job model.Job
	require.NoError(t, json.NewDecoder(a.Body).Decode(&job))
	a.Body.Close()
	require.Equal(t, model.JobAssigned, job.State)
	require.NotEmpty(t, job.WorkerID)

	kill := post(t, ts, "ctok", "/v1/jobs/"+job.ID+"/kill", map[string]string{"submitter": "agent-a"})
	defer kill.Body.Close()
	require.Equal(t, http.StatusOK, kill.StatusCode)

	flagged, err := st.TakeKillRequests(job.WorkerID)
	require.NoError(t, err)
	require.Contains(t, flagged, job.ID)
}

// TestSubmitRejectsUnknownDevice covers the minor finding that an unknown
// device_id used to surface as a 500 store_error rather than a 400 naming
// the problem — a real regression versus stage 1's behaviour, and untested
// until now.
func TestSubmitRejectsUnknownDevice(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts) // registers gpubox:gpu0 only

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:doesnotexist", Command: []string{"./a"}, Submitter: "agent-a",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "unknown_device", body["error"])
	require.Contains(t, body["message"], "gpubox:doesnotexist")
}

// raceNoWaitClock deterministically reproduces the race in the Important-1
// finding — "CancelQueued lands after the scheduler has already assigned
// the job" — without goroutines or timing luck, the same technique
// internal/store/queue_test.go's TestCancelledHeadJobDoesNotBlockTheQueue
// uses. handleSubmit's NoWait path reads the job (still queued), then calls
// CancelQueued; CancelQueued's very first statement is s.clock.Now(). Firing
// the race from inside that specific Now() call — the 4th clock read during
// B's submit (Enqueue's, assignQueued's, reserve's, then CancelQueued's) —
// lands it in exactly the window a concurrent scheduler tick would race
// into, single-threaded and every time: it releases A's device and runs
// ScheduleOnce itself, so B is already assigned by the time CancelQueued's
// UPDATE ... WHERE state = 'queued' actually executes.
type raceNoWaitClock struct {
	*clock.Fake
	store *store.Store

	armed bool
	skip  int
	fired bool

	releaseJobID string
}

func (c *raceNoWaitClock) Now() time.Time {
	if c.armed && !c.fired {
		if c.skip > 0 {
			c.skip--
		} else {
			c.fired = true // set before calling in: the calls below read the clock too.
			code := 0
			if err := c.store.Release(c.releaseJobID, model.JobSucceeded, &code, ""); err != nil {
				panic("raceNoWaitClock: release: " + err.Error())
			}
			if _, err := c.store.ScheduleOnce(); err != nil {
				panic("raceNoWaitClock: schedule: " + err.Error())
			}
		}
	}
	return c.Fake.Now()
}

// TestNoWaitReturnsTheJobWhenItWinsTheRace drives the exact interleaving
// from Important finding 1: a NoWait submit reads a job as queued, but by
// the time it tries to cancel that job, the device has already freed and
// the scheduler has assigned this exact job to it. The caller asked not to
// wait in a queue — it did not ask to have a job that is now running hidden
// behind a 409, with the GPU held for nobody watching. The fix must answer
// 201 with the assigned job, never a 409 for a job that is live.
func TestNoWaitReturnsTheJobWhenItWinsTheRace(t *testing.T) {
	c := &raceNoWaitClock{Fake: clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))}

	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	c.store = st

	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client", "atok": "admin"},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	var jobA model.Job
	require.NoError(t, json.NewDecoder(a.Body).Decode(&jobA))
	a.Body.Close()
	require.Equal(t, model.JobAssigned, jobA.State)

	// Arm the trap for B's submit: ignore the first 3 clock reads (Enqueue,
	// assignQueued, reserve — all still finding the device busy, since A is
	// only released from inside the trap itself), then fire on the 4th
	// (CancelQueued's), which is the exact race window.
	c.releaseJobID = jobA.ID
	c.skip = 3
	c.armed = true

	b := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b", NoWait: true,
	})
	defer b.Body.Close()
	require.True(t, c.fired, "test is broken: the race clock never fired")

	require.Equal(t, http.StatusCreated, b.StatusCode,
		"a job that won the race must be reported as created, never as a 409 conflict")

	var jobB model.Job
	require.NoError(t, json.NewDecoder(b.Body).Decode(&jobB))
	require.Equal(t, model.JobAssigned, jobB.State,
		"the caller must see the job exactly as it now stands: assigned, not hidden behind an error")
	require.NotEqual(t, model.JobKilled, jobB.State)
}

// TestSubmitRejectsPriorityOutOfRange covers the minor that priority was
// unbounded although the spec bounds it "to a small range so it stays a
// nudge rather than a scheduling language". Out-of-range values are
// rejected, not clamped — a caller who asked for 500 must not be quietly
// given 10 — and the error names the range so the caller can fix it without
// reading the source.
func TestSubmitRejectsPriorityOutOfRange(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	for _, priority := range []int{server.MaxPriority + 1, server.MinPriority - 1, 1000} {
		resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
			DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
			Priority: priority,
		})
		var body map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode, "priority %d", priority)
		require.Equal(t, "priority_out_of_range", body["error"])
		require.Contains(t, body["message"], fmt.Sprintf("%d", server.MinPriority))
		require.Contains(t, body["message"], fmt.Sprintf("%d", server.MaxPriority))
	}

	// Nothing was queued along the way: a rejected submit must not leave a
	// job behind for the scheduler loop to pick up later.
	queued, err := st.QueuedJobs()
	require.NoError(t, err)
	require.Empty(t, queued)

	// The bounds themselves are accepted.
	for _, priority := range []int{server.MinPriority, 0, server.MaxPriority} {
		resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
			DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
			Priority: priority,
		})
		resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode, "priority %d", priority)
	}
}
