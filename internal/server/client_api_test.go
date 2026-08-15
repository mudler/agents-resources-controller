package server_test

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/logstore"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func registerWorker(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}}})
	defer resp.Body.Close()
	var reg server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&reg))
	return reg.WorkerID
}

func TestSubmitAllocatesDeviceAndReturnsJob(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var job model.Job
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&job))
	require.Equal(t, model.JobAssigned, job.State)
	require.Equal(t, "gpubox:gpu0", job.DeviceID)
}

// Stage 2 queues a busy device by default (see TestSubmitQueuesWhenDeviceIsBusy
// in queue_api_test.go); this test now covers what it was actually testing —
// a caller that opts out of the queue with NoWait still gets an immediate
// 409, not a parked job.
func TestSubmitOnBusyDeviceReturns409(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	first.Body.Close()
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	second := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-b", NoWait: true,
	})
	defer second.Body.Close()
	require.Equal(t, http.StatusConflict, second.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(second.Body).Decode(&body))
	require.Equal(t, "no_device_available", body["error"])
}

func TestDevicesViewShowsHolderAndHeartbeatAge(t *testing.T) {
	ts, _, _, c := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	resp.Body.Close()

	c.Advance(90 * time.Second)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/state", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	got, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer got.Body.Close()

	var state server.StateResponse
	require.NoError(t, json.NewDecoder(got.Body).Decode(&state))
	require.Len(t, state.Devices, 1)
	require.Equal(t, "agent-a", state.Devices[0].Holder)
	require.Equal(t, []string{"./bench"}, state.Devices[0].Command)
	require.Equal(t, 90, state.Devices[0].ElapsedSeconds)
	require.Equal(t, 90, state.Devices[0].HeartbeatAgeSeconds)
}

// TestDevicesViewComputesOldestLabelAgeFromControllerClock is the
// dashboard's half of the clock-skew fix: DeviceView.OldestLabelAgeSeconds
// must be computed by the controller against ITS OWN clock, the same way
// HeartbeatAgeSeconds already is — internal/server/dashboard/index.html
// used to compute this one age itself, in the browser, via
// Date.parse(label.updated_at) against Date.now(), and its own comment
// documented the resulting exposure to a skewed operator's-machine clock.
// This advances the fake clock a known amount past a detected label, then
// pushes a fresh declared one, and asserts the OLDER label's age wins —
// not the newer one just pushed, and not an average of the two.
func TestDevicesViewComputesOldestLabelAgeFromControllerClock(t *testing.T) {
	ts, _, _, c := newServer(t)

	reg := post(t, ts, "wtok", "/v1/workers/register", server.RegisterRequest{
		Host: "gpubox", Devices: []server.DeviceSpec{{Name: "gpu0"}},
		Labels: map[string]map[string]string{"gpu0": {"vram": "80G"}},
	})
	var regResp server.RegisterResponse
	require.NoError(t, json.NewDecoder(reg.Body).Decode(&regResp))
	reg.Body.Close()

	c.Advance(2 * time.Hour) // vram/detected is now 2h old

	post(t, ts, "wtok", "/v1/workers/"+regResp.WorkerID+"/labels", server.LabelsPushRequest{
		Host: "gpubox", Devices: []string{"gpu0"},
		DeclaredLabels: map[string]map[string]string{"gpu0": {"owner": "team-a"}},
	}).Body.Close() // owner/declared is fresh: 0s old

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/state", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	got, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer got.Body.Close()

	var state server.StateResponse
	require.NoError(t, json.NewDecoder(got.Body).Decode(&state))
	require.Len(t, state.Devices, 1)
	require.NotNil(t, state.Devices[0].OldestLabelAgeSeconds)
	require.Equal(t, int(2*time.Hour/time.Second), *state.Devices[0].OldestLabelAgeSeconds,
		"the OLDEST label's age must win, not the freshest one just pushed")
}

// TestDevicesViewOldestLabelAgeIsNilWithNoLabels: a device with no labels
// at all has nothing to date, so OldestLabelAgeSeconds must be nil, not a
// meaningless 0 that would read as "confirmed just now".
func TestDevicesViewOldestLabelAgeIsNilWithNoLabels(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts) // no labels

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/state", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	got, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer got.Body.Close()

	var state server.StateResponse
	require.NoError(t, json.NewDecoder(got.Body).Decode(&state))
	require.Len(t, state.Devices, 1)
	require.Nil(t, state.Devices[0].OldestLabelAgeSeconds, "a device with no labels has nothing to date")
}

// TestDevicesViewOldestLabelAgeIsNotClampedForFutureStampedLabel matches
// this task's "never clamp a future timestamp to 0" rule (see formatAge,
// internal/cli/describe.go) for the dashboard's own age field: a label
// stamped ahead of the controller's clock must report a NEGATIVE age, not
// be floored to 0 and read as perfectly fresh.
func TestDevicesViewOldestLabelAgeIsNotClampedForFutureStampedLabel(t *testing.T) {
	ts, st, _, c := newServer(t)
	registerWorker(t, ts)

	future := c.Now().Add(10 * time.Minute)
	require.NoError(t, st.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vram": "80G"}, future))

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/state", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	got, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer got.Body.Close()

	var state server.StateResponse
	require.NoError(t, json.NewDecoder(got.Body).Decode(&state))
	require.Len(t, state.Devices, 1)
	require.NotNil(t, state.Devices[0].OldestLabelAgeSeconds)
	require.Equal(t, -10*60, *state.Devices[0].OldestLabelAgeSeconds)
}

func TestLogStreamDeliversWorkerChunks(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	var job model.Job
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&job))
	resp.Body.Close()

	appended := post(t, ts, "wtok", "/v1/jobs/"+job.ID+"/logs", nil)
	appended.Body.Close()

	// Real chunks arrive as a raw body, not JSON.
	raw, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/jobs/"+job.ID+"/logs",
		strings.NewReader("hello from the box\n"))
	require.NoError(t, err)
	raw.Header.Set("Authorization", "Bearer wtok")
	pushed, err := ts.Client().Do(raw)
	require.NoError(t, err)
	pushed.Body.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/jobs/"+job.ID+"/logs", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	stream, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer stream.Body.Close()

	sc := bufio.NewScanner(stream.Body)
	require.True(t, sc.Scan())
	require.Contains(t, sc.Text(), "hello from the box")
}

// TestStreamLogsForUnknownJobReturns404 guards against Follow silently
// O_CREATE-ing a log file (and pinning a goroutine + fd) for a job ID that
// was never allocated. Requires its own server so the log directory path is
// known and inspectable — newServer does not expose it.
func TestStreamLogsForUnknownJobReturns404(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })

	logsDir := filepath.Join(dir, "logs")
	logs, err := logstore.New(logsDir)
	require.NoError(t, err)

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client", "atok": "admin"},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/jobs/does-not-exist/logs", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "not_found", body["error"])

	// logstore.Read returning empty is not proof nothing was created — an
	// O_CREATE'd empty file reads back as empty too. Check the directory.
	entries, err := os.ReadDir(logsDir)
	require.NoError(t, err)
	require.Empty(t, entries, "no log file should exist for a job that was never allocated")
}

// TestGetJobStoreFailureIsNot404 makes sure an infrastructure failure is
// never reported to the client as "job not found": a live job during a DB
// outage must not look like a vanished job, or a client might resubmit onto
// a device that is still actually busy.
func TestGetJobStoreFailureIsNot404(t *testing.T) {
	ts, st, _, _ := newServer(t)
	require.NoError(t, st.Close())

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/jobs/whatever-id", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "database is closed",
		"the raw driver error must not leak to the client")

	var body map[string]string
	require.NoError(t, json.Unmarshal(raw, &body))
	require.Equal(t, "store_error", body["error"])
}

// TestClearDeviceWithLiveLeaseReturns409 covers the one manual override that
// exists for a stuck GPU: ClearDevice refuses to contradict a live lease, and
// the HTTP layer must report that refusal, not paper over it with a 200.
func TestClearDeviceWithLiveLeaseReturns409(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// The device now has both a live lease and (forced) unhealthy state, the
	// exact scenario an operator would try to clear.
	require.NoError(t, st.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, time.Now()))

	clr := post(t, ts, "atok", "/v1/devices/gpubox:gpu0/clear", nil)
	defer clr.Body.Close()
	require.Equal(t, http.StatusConflict, clr.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(clr.Body).Decode(&body))
	require.Equal(t, "device_not_cleared", body["error"])

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State,
		"the device must still not be schedulable after a refused clear")
}

func TestSubmitRejectsEmptySubmitter(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestSubmitRejectsBothDeviceIDAndSelector pins the HTTP layer's half of the
// exactly-one-of rule: store.Enqueue already rejects this combination, but
// nothing previously proved handleSubmit's own pre-check (which runs before
// Enqueue is ever reached) actually rejects it too, at this layer, with a
// 400.
func TestSubmitRejectsBothDeviceIDAndSelector(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Selector: "vram>=40G",
		Command: []string{"./bench"}, Submitter: "agent-a",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestSubmitWithMalformedSelectorReturns400NotStoreError guards the review
// finding: store.Enqueue wraps a selector parse failure as "selector: %w",
// which matched none of handleSubmit's errors.Is branches and fell through
// to a 500 store_error — telling the agent the CONTROLLER is broken for a
// typo in ITS OWN selector, and hiding which term was wrong. A malformed
// selector must read exactly like /v1/explain's identical case: 400
// bad_selector, with the real reason in the message.
func TestSubmitWithMalformedSelectorReturns400NotStoreError(t *testing.T) {
	ts, _, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		Selector: "not-a-valid-term", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode,
		"a malformed selector must be the caller's mistake (400), not reported as a controller failure (500)")

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "bad_selector", body["error"])
	require.NotEqual(t, "store_error", body["error"])
}

// TestSubmitWakesWaitingWorkerLongPoll is the only coverage of poke: without
// it, a worker blocked in the assignments long-poll would only ever see a
// freshly assigned job after the wait window (up to 30s in production)
// expired, not the moment it was submitted.
func TestSubmitWakesWaitingWorkerLongPoll(t *testing.T) {
	ts, _, _, _ := newServer(t)
	workerID := registerWorker(t, ts)

	type pollResult struct {
		resp *http.Response
		err  error
		dur  time.Duration
	}
	pollDone := make(chan pollResult, 1)
	pollStarted := make(chan struct{})

	go func() {
		req, err := http.NewRequest(http.MethodGet,
			ts.URL+"/v1/workers/"+workerID+"/assignments?wait=10s", nil)
		if err != nil {
			pollDone <- pollResult{err: err}
			return
		}
		req.Header.Set("Authorization", "Bearer wtok")
		close(pollStarted)
		start := time.Now()
		resp, err := ts.Client().Do(req)
		pollDone <- pollResult{resp: resp, err: err, dur: time.Since(start)}
	}()

	<-pollStarted
	time.Sleep(50 * time.Millisecond) // let the poll register with the notifier before submit

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	select {
	case r := <-pollDone:
		require.NoError(t, r.err)
		defer r.resp.Body.Close()
		require.Equal(t, http.StatusOK, r.resp.StatusCode)
		require.Less(t, r.dur, 2*time.Second,
			"poke should wake the long-poll well before the 10s wait window elapses")

		var poll server.PollResponse
		require.NoError(t, json.NewDecoder(r.resp.Body).Decode(&poll))
		require.Len(t, poll.Assignments, 1)
		require.Equal(t, "gpubox:gpu0", poll.Assignments[0].DeviceID)
	case <-time.After(5 * time.Second):
		t.Fatal("long-poll did not return after submit poked the worker")
	}
}
