package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func newServer(t *testing.T) (*httptest.Server, *store.Store, *logstore.Store, *clock.Fake) {
	t.Helper()
	dir := t.TempDir()
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })

	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client", "atok": "admin"},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, st, logs, c
}

func post(t *testing.T, ts *httptest.Server, token, path string, body any) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, &buf)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}

func get(t *testing.T, ts *httptest.Server, token, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}

// postRaw sends a raw byte body without JSON-encoding it, and lets the
// caller set the Authorization header verbatim (no automatic "Bearer "
// prefixing), so tests can exercise malformed auth headers and oversized
// bodies precisely.
func postRaw(t *testing.T, ts *httptest.Server, authHeader, path string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}

func TestUnauthenticatedRequestIsRejected(t *testing.T) {
	ts, _, _, _ := newServer(t)
	resp := post(t, ts, "bogus", "/v1/workers/register", server.RegisterRequest{Host: "gpubox"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestClientTokenCannotRegisterWorker(t *testing.T) {
	ts, _, _, _ := newServer(t)
	resp := post(t, ts, "ctok", "/v1/workers/register", server.RegisterRequest{Host: "gpubox"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestAuthorizationHeaderRequiresBearerScheme(t *testing.T) {
	ts, _, _, _ := newServer(t)
	// "wtok" is a valid token value but sent without the "Bearer " scheme
	// prefix; TrimPrefix-style matching would silently accept this.
	resp := postRaw(t, ts, "wtok", "/v1/workers/register", nil)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestRegisterCreatesDevices(t *testing.T) {
	ts, st, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}, {Name: "gpu1"}}})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.WorkerID)

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Len(t, devices, 2)
	require.Equal(t, "gpubox:gpu0", devices[0].ID)
	require.Equal(t, model.DeviceReady, devices[0].State)
}

// TestRegisterAppliesDeviceRuntimeCeilings guards the production caller
// SetDeviceMaxRuntime never had before this task: a device declared with
// max_runtime_seconds at registration must actually end up with that
// ceiling recorded, not just accepted and dropped. Devices() does not
// surface the ceiling column, so this proves it indirectly through the
// effect the ceiling has: a submit asking for more runtime than declared is
// rejected, and one asking for less (or none) is accepted.
func TestRegisterAppliesDeviceRuntimeCeilings(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{
			{Name: "gpu0", MaxRuntimeSeconds: 3600},
		}})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	over := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
		MaxRuntimeSeconds: 7200,
	})
	defer over.Body.Close()
	require.Equal(t, http.StatusBadRequest, over.StatusCode,
		"a submit above the declared ceiling must be rejected, proving the ceiling actually landed")
	var body map[string]string
	require.NoError(t, json.NewDecoder(over.Body).Decode(&body))
	require.Equal(t, "runtime_above_ceiling", body["error"])

	under := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
		MaxRuntimeSeconds: 1800,
	})
	defer under.Body.Close()
	require.Equal(t, http.StatusCreated, under.StatusCode,
		"a submit within the declared ceiling must be accepted")
}

// TestRegistrationClearsARemovedCeiling guards the fix for the review
// finding that a ceiling could be set but never removed: worker.yaml is the
// declaration of intent, so a registration that omits max_runtime for a
// device it previously declared one for must actually clear it, not merely
// leave the old value in place forever. An operator who deletes
// `max_runtime: 1h` and restarts the worker must get a device with no limit.
func TestRegistrationClearsARemovedCeiling(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{
			{Name: "gpu0", MaxRuntimeSeconds: 3600},
		}})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The worker restarts with max_runtime dropped from worker.yaml.
	resp2 := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{
			{Name: "gpu0"},
		}})
	resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	submit := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
		MaxRuntimeSeconds: 7200,
	})
	defer submit.Body.Close()
	require.Equal(t, http.StatusCreated, submit.StatusCode,
		"a ceiling removed from worker.yaml and re-registered must not still cap the job")
}

// TestAssignmentsEnvelopeCarriesKillRequests guards the wire change this task
// makes: a kill must reach the worker through the same long-poll response an
// assignment does, not a separate channel, and it alone (with no new
// assignment) must be enough to end the 204 idle path.
func TestAssignmentsEnvelopeCarriesKillRequests(t *testing.T) {
	ts, st, _, _ := newServer(t)
	workerID := registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	var job model.Job
	require.NoError(t, json.NewDecoder(a.Body).Decode(&job))
	a.Body.Close()
	require.Equal(t, model.JobAssigned, job.State)

	// Drain the assignment itself first, exactly as a worker's first poll
	// would, so the job is "running" and only the kill is left to observe.
	req, err := http.NewRequest(http.MethodGet,
		ts.URL+"/v1/workers/"+workerID+"/assignments?wait=50ms", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer wtok")
	firstResp, err := ts.Client().Do(req)
	require.NoError(t, err)
	firstResp.Body.Close()

	flagged, err := st.RequestKill(job.ID)
	require.NoError(t, err)
	require.True(t, flagged)

	req2, err := http.NewRequest(http.MethodGet,
		ts.URL+"/v1/workers/"+workerID+"/assignments?wait=2s", nil)
	require.NoError(t, err)
	req2.Header.Set("Authorization", "Bearer wtok")
	resp2, err := ts.Client().Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode,
		"a kill with no new assignment must still end the long-poll with 200, not 204")

	var poll server.PollResponse
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&poll))
	require.Empty(t, poll.Assignments)
	require.Contains(t, poll.Kills, job.ID)
}

// poll drains one long-poll for a worker and returns the response, so tests
// that care about what a poll answers don't each rebuild the request.
func poll(t *testing.T, ts *httptest.Server, workerID, wait string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet,
		ts.URL+"/v1/workers/"+workerID+"/assignments?wait="+wait, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer wtok")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}

// A kill flag nobody can action must not turn the long poll into a hot loop.
//
// handleAssignments ends its wait the instant there is a kill to report,
// which is right for a kill the worker can act on. But a flag on a job no
// worker is actually running — the lost-assignment case — is never cleared by
// anyone, so re-offering it on every poll made each poll return in
// milliseconds and the worker re-poll at its 250ms floor for as long as the
// flag stood: thousands of requests a minute from one idle worker.
//
// After the first delivery the flag goes quiet for killRedeliverInterval, so
// the very next poll must block for its full window and answer 204.
func TestDeliveredKillDoesNotHotLoopTheLongPoll(t *testing.T) {
	ts, st, _, _ := newServer(t)
	workerID := registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	var job model.Job
	require.NoError(t, json.NewDecoder(a.Body).Decode(&job))
	a.Body.Close()

	poll(t, ts, workerID, "50ms").Body.Close() // drain the assignment

	flagged, err := st.RequestKill(job.ID)
	require.NoError(t, err)
	require.True(t, flagged)

	first := poll(t, ts, workerID, "2s")
	defer first.Body.Close()
	require.Equal(t, http.StatusOK, first.StatusCode)
	var delivered server.PollResponse
	require.NoError(t, json.NewDecoder(first.Body).Decode(&delivered))
	require.Contains(t, delivered.Kills, job.ID)

	// The worker did nothing with it (it has no process for this job). The
	// next poll must wait rather than answering instantly with the same flag.
	started := time.Now()
	second := poll(t, ts, workerID, "300ms")
	defer second.Body.Close()
	require.Equal(t, http.StatusNoContent, second.StatusCode,
		"an already-delivered kill must not end the next long poll immediately")
	require.GreaterOrEqual(t, time.Since(started), 250*time.Millisecond,
		"the poll must actually have waited out its window, not spun")
}

// A kill IS re-delivered once the quiet interval has passed: the response
// carrying it can be lost like any other, so re-delivery has to exist — it
// just must not be free-running.
func TestKillIsRedeliveredAfterTheQuietInterval(t *testing.T) {
	ts, st, _, c := newServer(t)
	workerID := registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	var job model.Job
	require.NoError(t, json.NewDecoder(a.Body).Decode(&job))
	a.Body.Close()

	poll(t, ts, workerID, "50ms").Body.Close()

	_, err := st.RequestKill(job.ID)
	require.NoError(t, err)
	poll(t, ts, workerID, "2s").Body.Close() // first delivery

	c.Advance(31 * time.Second)

	again := poll(t, ts, workerID, "2s")
	defer again.Body.Close()
	require.Equal(t, http.StatusOK, again.StatusCode)
	var redelivered server.PollResponse
	require.NoError(t, json.NewDecoder(again.Body).Decode(&redelivered))
	require.Contains(t, redelivered.Kills, job.ID,
		"a kill whose delivery may have been lost must be offered again")
}

// The wire half of the lease-renewal fix: the controller renews the lease of
// a job the worker names in its heartbeat, and does not renew one it doesn't.
// The store-level reasoning is in store.RecordHeartbeat and
// TestHeartbeatDoesNotRenewAnUnclaimedJobLease; this pins the HTTP contract
// the worker actually speaks.
func TestHeartbeatRenewsOnlyTheJobsTheWorkerNames(t *testing.T) {
	for _, tc := range []struct {
		name      string
		claim     func(jobID string) any
		wantState model.JobState
	}{
		{
			name:      "named",
			claim:     func(jobID string) any { return server.HeartbeatRequest{RunningJobIDs: []string{jobID}} },
			wantState: model.JobRunning,
		},
		{
			// Exactly what a worker whose assignment response was lost sends:
			// it is alive and heartbeating, it just has no such job.
			name:      "unnamed",
			claim:     func(string) any { return server.HeartbeatRequest{} },
			wantState: model.JobLost,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, st, _, c := newServer(t)
			workerID := registerWorker(t, ts)

			a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
				DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
			})
			var job model.Job
			require.NoError(t, json.NewDecoder(a.Body).Decode(&job))
			a.Body.Close()
			poll(t, ts, workerID, "50ms").Body.Close() // the job is now running

			// Well past the job's 5-minute default lease TTL, heartbeating
			// throughout so the worker itself is never the silent one.
			for i := 0; i < 8; i++ {
				c.Advance(time.Minute)
				hb := post(t, ts, "wtok", "/v1/workers/"+workerID+"/heartbeat", tc.claim(job.ID))
				require.Equal(t, http.StatusOK, hb.StatusCode)
				hb.Body.Close()
			}

			_, err := st.Sweep(30*time.Second, 5*time.Minute)
			require.NoError(t, err)

			reloaded, err := st.Job(job.ID)
			require.NoError(t, err)
			require.Equal(t, tc.wantState, reloaded.State)
		})
	}
}

// A heartbeat with no body at all stays valid: it means "I am running
// nothing", which is what a worker with an empty job table has always sent.
func TestBareHeartbeatIsAccepted(t *testing.T) {
	ts, _, _, _ := newServer(t)
	workerID := registerWorker(t, ts)

	resp := postRaw(t, ts, "Bearer wtok", "/v1/workers/"+workerID+"/heartbeat", nil)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAssignmentsLongPollReturns204WhenIdle(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}})
	var reg server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&reg))
	resp.Body.Close()

	req, err := http.NewRequest(http.MethodGet,
		ts.URL+"/v1/workers/"+reg.WorkerID+"/assignments?wait=50ms", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer wtok")

	got, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer got.Body.Close()
	require.Equal(t, http.StatusNoContent, got.StatusCode)
}

func TestWorkerReportingTerminalStatusFreesDevice(t *testing.T) {
	ts, st, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}})
	var reg server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&reg))
	resp.Body.Close()

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"true"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	code := 0
	sr := post(t, ts, "wtok", "/v1/jobs/"+job.ID+"/status",
		server.StatusRequest{WorkerID: reg.WorkerID, State: model.JobSucceeded, ExitCode: &code})
	defer sr.Body.Close()
	require.Equal(t, http.StatusOK, sr.StatusCode)

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)
}

func TestWorkerCannotReportStatusForAnotherWorkersJob(t *testing.T) {
	ts, st, _, _ := newServer(t)

	resp1 := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}})
	var reg1 server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp1.Body).Decode(&reg1))
	resp1.Body.Close()

	resp2 := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox2", Devices: []server.DeviceSpec{{Name: "gpu0"}}})
	var reg2 server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&reg2))
	resp2.Body.Close()
	require.NotEqual(t, reg1.WorkerID, reg2.WorkerID)

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"true"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	// Worker 2 reports status for worker 1's job.
	sr := post(t, ts, "wtok", "/v1/jobs/"+job.ID+"/status",
		server.StatusRequest{WorkerID: reg2.WorkerID, State: model.JobFailed, Reason: "not mine"})
	defer sr.Body.Close()
	require.Equal(t, http.StatusForbidden, sr.StatusCode)

	// The device must still be busy and the job must still be non-terminal:
	// the GPU was not handed away to a second job while the first is still
	// running on it.
	devices, err := st.Devices()
	require.NoError(t, err)
	var gpu0 *model.Device
	for i := range devices {
		if devices[i].ID == "gpubox:gpu0" {
			gpu0 = &devices[i]
		}
	}
	require.NotNil(t, gpu0)
	require.Equal(t, model.DeviceBusy, gpu0.State)

	got, err := st.Job(job.ID)
	require.NoError(t, err)
	require.False(t, got.State.Terminal())
}

// TestAssignmentIsHandedOutOnlyOnce guards against a worker's poll loop
// re-running a job: handing out an assignment must be a state transition
// (assigned -> running), not just a query, or the same job stays "assigned"
// and gets returned — and re-executed — on every subsequent poll until the
// worker's own "running" report happens to land.
func TestAssignmentIsHandedOutOnlyOnce(t *testing.T) {
	ts, st, _, _ := newServer(t)
	workerID := registerWorker(t, ts)

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"true"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	poll := func() []server.Assignment {
		req, err := http.NewRequest(http.MethodGet,
			ts.URL+"/v1/workers/"+workerID+"/assignments?wait=50ms", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer wtok")
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent {
			return nil
		}
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var out server.PollResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		return out.Assignments
	}

	first := poll()
	require.Len(t, first, 1)
	require.Equal(t, job.ID, first[0].JobID)

	// The job must not still be "assigned" after being handed out once: a
	// second poll, arriving before any status report from the worker, must
	// not see it again.
	second := poll()
	require.Empty(t, second, "a job must not be handed out twice before the worker has reported anything")

	got, err := st.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobRunning, got.State)
}

// TestReRegistrationCannotLeaveADeviceReadyWithALiveLease guards the fix for
// the critical review finding: a worker that restarts mid-job — same
// host-derived worker ID, brand-new process — must not falsify its device
// as ready (or leave it busy forever) while the controller has no proof an
// orphaned process from the dead worker isn't still holding it.
// Registration must reconcile: reap the in-flight job, release its lease,
// and quarantine the device unhealthy.
func TestReRegistrationCannotLeaveADeviceReadyWithALiveLease(t *testing.T) {
	ts, st, _, c := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"sleep", "30"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	require.NoError(t, st.MarkRunning(job.ID, c.Now()))

	leasesBefore, err := st.Leases()
	require.NoError(t, err)
	require.Len(t, leasesBefore, 1, "sanity: the job's lease must actually be live before the restart")

	// The worker "restarts": a fresh process registers under the same
	// host-derived worker ID while the controller still believes the job is
	// running on it.
	resp2 := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}})
	defer resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Len(t, devices, 1)
	require.NotEqual(t, model.DeviceReady, devices[0].State,
		"a re-registering worker must never leave a device ready while its lease was live")
	require.Equal(t, model.DeviceUnhealthy, devices[0].State,
		"the device must be quarantined, not merely left non-ready")

	leasesAfter, err := st.Leases()
	require.NoError(t, err)
	require.Empty(t, leasesAfter, "the stranded lease must be released once the job is reaped, not left live and unreachable")

	reloaded, err := st.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)
}

func TestJobStatusForUnknownJobReturns404(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}})
	var reg server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&reg))
	resp.Body.Close()

	sr := post(t, ts, "wtok", "/v1/jobs/does-not-exist/status",
		server.StatusRequest{WorkerID: reg.WorkerID, State: model.JobFailed})
	defer sr.Body.Close()
	require.Equal(t, http.StatusNotFound, sr.StatusCode)
}

// TestDeviceFaultQuarantinesUnhealthy pins the new route's contract: a
// worker reporting a fault (an acquire hook that failed) must land the
// device in the same unhealthy state SetDeviceState already produces for a
// self-reported fault.
func TestDeviceFaultQuarantinesUnhealthy(t *testing.T) {
	ts, _, _, _ := newServer(t)

	post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}}).Body.Close()

	resp := post(t, ts, "wtok", "/v1/devices/gpubox:gpu0/fault", map[string]string{"reason": "on_acquire failed: exit 1"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	dr := get(t, ts, "ctok", "/v1/devices")
	defer dr.Body.Close()
	var views []server.DeviceView
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&views))
	require.Len(t, views, 1)
	require.Equal(t, model.DeviceUnhealthy, views[0].Device.State)
}

// TestDeviceFaultOutlivesAReboot pins the reason it must be "fault" and not
// some other quarantine cause: only "fault" is NOT in rebootClearableReasons
// (internal/store/reaper.go), so a subsequent reboot (a changed boot ID at
// re-registration) must leave the device unhealthy — unlike worker_lost,
// lease_expired, or registration, all of which a proven reboot does clear.
func TestDeviceFaultOutlivesAReboot(t *testing.T) {
	ts, _, _, _ := newServer(t)

	post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", BootID: "boot-1", Devices: []server.DeviceSpec{{Name: "gpu0"}}}).Body.Close()

	post(t, ts, "wtok", "/v1/devices/gpubox:gpu0/fault", map[string]string{"reason": "on_acquire failed"}).Body.Close()

	// The host reboots: a changed boot ID is the one thing that proves
	// nothing survived, and normally clears a quarantine.
	post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", BootID: "boot-2", Devices: []server.DeviceSpec{{Name: "gpu0"}}}).Body.Close()

	dr := get(t, ts, "ctok", "/v1/devices")
	defer dr.Body.Close()
	var views []server.DeviceView
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&views))
	require.Len(t, views, 1)
	require.Equal(t, model.DeviceUnhealthy, views[0].Device.State,
		"a fault must outlive a reboot: a reboot proves no process survived, not that the hardware is sound")
}

// TestDeviceFaultOnUnknownDeviceIs404 guards the fix for a route that
// answered 200 having changed nothing: SetDeviceState's UPDATE simply
// matches no row for an ID this controller has never seen, which SQL does
// not consider an error. The worker then logs a successful quarantine for a
// device that is still perfectly schedulable — the worst combination there
// is, since nobody goes looking for a device everyone believes was taken
// out of the pool.
func TestDeviceFaultOnUnknownDeviceIs404(t *testing.T) {
	ts, _, _, _ := newServer(t)

	post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}}).Body.Close()

	resp := post(t, ts, "wtok", "/v1/devices/gpubox:typo0/fault", map[string]string{"reason": "on_acquire failed"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode,
		"a fault report that matched no device must never read as success")

	// The real device is untouched: nothing was quarantined by accident.
	dr := get(t, ts, "ctok", "/v1/devices")
	defer dr.Body.Close()
	var views []server.DeviceView
	require.NoError(t, json.NewDecoder(dr.Body).Decode(&views))
	require.Len(t, views, 1)
	require.Equal(t, model.DeviceReady, views[0].Device.State)
}

// TestUnknownTokenCannotReportDeviceFault pins the other half of the fault
// route's auth: an unrecognised (or missing) token is refused outright,
// before the handler can quarantine anything.
func TestUnknownTokenCannotReportDeviceFault(t *testing.T) {
	ts, _, _, _ := newServer(t)
	resp := post(t, ts, "nope", "/v1/devices/gpubox:gpu0/fault", map[string]string{"reason": "x"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestAdminTokenMayReportDeviceFault pins the deliberate exception in the
// role model: admin outranks every role, on worker routes included, so an
// operator standing in for a host (or a verify probe run by hand) can
// quarantine a device without a worker token. This is the documented rule
// in require(), not an oversight — a test says so, so a future reader does
// not "fix" it into a 403.
func TestAdminTokenMayReportDeviceFault(t *testing.T) {
	ts, _, _, _ := newServer(t)

	post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}}).Body.Close()

	resp := post(t, ts, "atok", "/v1/devices/gpubox:gpu0/fault", map[string]string{"reason": "operator standing in"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestClientTokenCannotReportDeviceFault mirrors
// TestClientTokenCannotRegisterWorker: the fault route is worker-token only,
// the same as every other route a device host itself calls.
func TestClientTokenCannotReportDeviceFault(t *testing.T) {
	ts, _, _, _ := newServer(t)
	resp := post(t, ts, "ctok", "/v1/devices/gpubox:gpu0/fault", map[string]string{"reason": "x"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestOversizedLogChunkIsRejected(t *testing.T) {
	ts, _, logs, _ := newServer(t)

	oversized := bytes.Repeat([]byte("x"), (1<<20)+1)
	resp := postRaw(t, ts, "Bearer wtok", "/v1/jobs/job1/logs", oversized)
	defer resp.Body.Close()
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)

	got, err := logs.Read("job1")
	require.NoError(t, err)
	require.Empty(t, got)
}
