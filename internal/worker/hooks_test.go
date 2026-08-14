package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

// writeHookScript writes an executable shell script that appends a line to
// logPath every time it runs, naming the event and job it was invoked for.
func writeHookScript(t *testing.T, logPath string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "hook.sh")
	body := fmt.Sprintf("#!/bin/sh\necho \"$RC_EVENT $RC_JOB_ID\" >> %s\n", logPath)
	require.NoError(t, os.WriteFile(p, []byte(body), 0o700))
	return p
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	var lines []string
	for _, l := range splitNonEmpty(string(b)) {
		lines = append(lines, l)
	}
	return lines
}

func splitNonEmpty(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == '\n' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

// TestHooksBackToBackJobsThroughRealExecuteProduceOneAcquireOneRelease
// drives the property the release linger exists for through the actual
// worker.Start()/execute() pipeline (not just the hook state machine in
// isolation): two jobs assigned back-to-back, well inside the linger
// window, must produce exactly one acquire and one release for the device
// they share, no matter how many jobs ran in between.
func TestHooksBackToBackJobsThroughRealExecuteProduceOneAcquireOneRelease(t *testing.T) {
	dir := t.TempDir()
	hookLog := filepath.Join(dir, "hooks.log")
	hook := writeHookScript(t, hookLog)

	var (
		mu         sync.Mutex
		job1Served bool
		job1Done   bool
		job2Served bool
		jobsRun    int
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/workers/register":
			json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

		case r.URL.Path == "/v1/workers/w1/assignments":
			mu.Lock()
			// job1 goes out on the first poll. job2 is only handed out once
			// job1 has actually reached a terminal state — a real
			// controller would never hand out a second assignment for a
			// still-busy device — so the two are genuinely back-to-back
			// (job2 starts right as job1 ends) rather than racing each
			// other through the worker's own goroutines.
			var jobID string
			switch {
			case !job1Served:
				jobID = "job1"
				job1Served = true
			case job1Done && !job2Served:
				jobID = "job2"
				job2Served = true
			}
			mu.Unlock()
			if jobID != "" {
				json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
					"job_id":    jobID,
					"device_id": "gpubox:gpu0",
					"command":   []string{"true"},
				}}})
				return
			}
			w.WriteHeader(http.StatusNoContent)

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs", r.URL.Path == "/v1/jobs/job2/logs":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status", r.URL.Path == "/v1/jobs/job2/status":
			var body struct{ State string }
			json.NewDecoder(r.Body).Decode(&body)
			if body.State == "succeeded" {
				mu.Lock()
				jobsRun++
				if r.URL.Path == "/v1/jobs/job1/status" {
					job1Done = true
				}
				mu.Unlock()
			}
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL: ts.URL,
		Host:          "gpubox",
		Devices: []worker.DeviceConfig{{
			Name: "gpu0", OnAcquire: hook, OnRelease: hook,
			HookTimeout: 5 * time.Second, ReleaseLinger: 500 * time.Millisecond,
		}},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          20 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return jobsRun == 2
	}, 10*time.Second, 20*time.Millisecond, "both jobs must have run back-to-back")

	// Give the linger (500ms) time to actually elapse and fire the release,
	// and confirm nothing extra shows up afterward either.
	time.Sleep(1200 * time.Millisecond)

	lines := readLines(t, hookLog)
	// The leading "release " is Start's own startup reconciliation pass —
	// it runs this device's release hook once, unconditionally, before the
	// first poll (see TestStartupRunsReleaseHooksBeforeFirstPoll); it has
	// no job ID, hence the trailing space. What follows it is the actual
	// property under test: exactly one acquire (from the first job) and
	// one release (naming the last job before the device went idle).
	require.Equal(t, []string{"release ", "acquire job1", "release job2"}, lines)
}

// TestHooksJobAfterLingerElapsedGetsItsOwnAcquire is the other half: once
// the linger genuinely elapses with nothing else arriving, a later job gets
// a fresh acquire — proving the collapsing above is linger-bounded, not
// permanent.
func TestHooksJobAfterLingerElapsedGetsItsOwnAcquire(t *testing.T) {
	dir := t.TempDir()
	hookLog := filepath.Join(dir, "hooks.log")
	hook := writeHookScript(t, hookLog)

	var (
		mu     sync.Mutex
		polls  int
		served = map[string]bool{}
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/workers/register":
			json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

		case r.URL.Path == "/v1/workers/w1/assignments":
			mu.Lock()
			polls++
			var jobID string
			if !served["job1"] {
				jobID = "job1"
				served["job1"] = true
			} else if polls > 20 && !served["job2"] {
				// Only hand out job2 well after enough empty polls have
				// gone by for the short linger to have elapsed.
				jobID = "job2"
				served["job2"] = true
			}
			mu.Unlock()
			if jobID != "" {
				json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
					"job_id":    jobID,
					"device_id": "gpubox:gpu0",
					"command":   []string{"true"},
				}}})
				return
			}
			w.WriteHeader(http.StatusNoContent)

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs", r.URL.Path == "/v1/jobs/job2/logs":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status", r.URL.Path == "/v1/jobs/job2/status":
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL: ts.URL,
		Host:          "gpubox",
		Devices: []worker.DeviceConfig{{
			Name: "gpu0", OnAcquire: hook, OnRelease: hook,
			HookTimeout: 5 * time.Second, ReleaseLinger: 100 * time.Millisecond,
		}},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          10 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return served["job2"]
	}, 10*time.Second, 20*time.Millisecond, "job2 must eventually be served")

	require.Eventually(t, func() bool {
		lines := readLines(t, hookLog)
		return len(lines) == 5
	}, 5*time.Second, 20*time.Millisecond)

	lines := readLines(t, hookLog)
	// Leading "release " is the startup reconciliation pass, as in
	// TestHooksBackToBackJobsThroughRealExecuteProduceOneAcquireOneRelease.
	require.Equal(t, []string{"release ", "acquire job1", "release job1", "acquire job2", "release job2"}, lines)
}

// TestAcquireHookFailureQuarantinesDeviceAndSkipsJob covers the acquire
// failure contract end to end: the job is reported failed with the hook's
// output as the reason, the controller is asked to fault the device, and
// the job's own command never actually runs — proven by an artifact the
// command would have created never showing up.
func TestAcquireHookFailureQuarantinesDeviceAndSkipsJob(t *testing.T) {
	dir := t.TempDir()
	artifact := filepath.Join(dir, "job-ran")
	failScript := filepath.Join(dir, "fail.sh")
	require.NoError(t, os.WriteFile(failScript, []byte("#!/bin/sh\necho boom-output 1>&2\nexit 1\n"), 0o700))

	var (
		mu          sync.Mutex
		served      bool
		statuses    []map[string]any
		faultCalls  []map[string]any
		faultDevice string
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/workers/register":
			json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

		case r.URL.Path == "/v1/workers/w1/assignments":
			mu.Lock()
			first := !served
			served = true
			mu.Unlock()
			if !first {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"sh", "-c", "touch " + artifact},
			}}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			statuses = append(statuses, body)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/devices/gpubox:gpu0/fault":
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			faultCalls = append(faultCalls, body)
			faultDevice = "gpubox:gpu0"
			mu.Unlock()
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL: ts.URL,
		Host:          "gpubox",
		Devices: []worker.DeviceConfig{{
			Name: "gpu0", OnAcquire: failScript,
			HookTimeout: 5 * time.Second, ReleaseLinger: 100 * time.Millisecond,
		}},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          20 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(faultCalls) == 1
	}, 8*time.Second, 20*time.Millisecond, "a failed acquire hook must report the device as faulted")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "gpubox:gpu0", faultDevice)
	require.Contains(t, fmt.Sprint(faultCalls[0]["reason"]), "boom-output",
		"the fault reason should surface the hook's own output")

	require.Len(t, statuses, 1, "the job must go straight to a single terminal report, never a running one")
	require.Equal(t, "failed", statuses[0]["state"])
	require.Contains(t, fmt.Sprint(statuses[0]["reason"]), "boom-output",
		"the operator must see why from the job's own failure reason")

	_, err := os.Stat(artifact)
	require.True(t, os.IsNotExist(err), "the job's own command must never run when its acquire hook fails")
}

// TestStartupRunsReleaseHooksBeforeFirstPoll covers the crash-safety
// contract: a fresh worker process runs every declared device's release
// hook once, before it ever polls for work. The assignments handler records
// whether the release marker already existed the very first time it was
// asked for work — proving ordering, not just eventual occurrence.
func TestStartupRunsReleaseHooksBeforeFirstPoll(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "released")
	release := filepath.Join(dir, "release.sh")
	require.NoError(t, os.WriteFile(release, []byte("#!/bin/sh\ntouch "+marker+"\n"), 0o700))

	existedAtFirstPoll := make(chan bool, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/workers/register":
			json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

		case r.URL.Path == "/v1/workers/w1/assignments":
			select {
			case existedAtFirstPoll <- fileExists(marker):
			default:
			}
			w.WriteHeader(http.StatusNoContent)

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL: ts.URL,
		Host:          "gpubox",
		Devices: []worker.DeviceConfig{{
			Name: "gpu0", OnRelease: release,
			HookTimeout: 5 * time.Second, ReleaseLinger: time.Second,
		}},
		HeartbeatInterval: time.Second,
		PollWait:          10 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	select {
	case existed := <-existedAtFirstPoll:
		require.True(t, existed, "the release hook must have already run by the time the worker makes its first poll")
	case <-time.After(8 * time.Second):
		t.Fatal("worker never polled")
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
