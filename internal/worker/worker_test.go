package worker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestWorkerRegistersRunsAssignmentAndReportsResult(t *testing.T) {
	var (
		mu        sync.Mutex
		logs      []byte
		states    []string
		exitCodes []int
		served    bool
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
			json.NewEncoder(w).Encode([]map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"sh", "-c", "echo hello; exit 7"},
			}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs":
			buf := make([]byte, 4096)
			n, _ := r.Body.Read(buf)
			mu.Lock()
			logs = append(logs, buf[:n]...)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body struct {
				State    string `json:"state"`
				ExitCode *int   `json:"exit_code"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			states = append(states, body.State)
			if body.ExitCode != nil {
				exitCodes = append(exitCodes, *body.ExitCode)
			}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Token:             "wtok",
		Host:              "gpubox",
		Devices:           []string{"gpu0"},
		HeartbeatInterval: 50 * time.Millisecond,
		PollWait:          100 * time.Millisecond,
	})

	go func() { _ = wk.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(exitCodes) == 1 && string(logs) != ""
	}, 10*time.Second, 50*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, string(logs), "hello")
	require.Equal(t, []int{7}, exitCodes)
	require.Equal(t, []string{"running", "failed"}, states)
}

// TestAssignmentIsDeliveredOnlyOnce simulates a controller that keeps
// re-delivering the same assignment on every poll — e.g. because it never
// transitions the job out of "assigned" until the worker's own "running"
// report happens to land, and that report was lost or delayed. Regardless of
// what the controller does, this worker process must never run the same job
// ID a second time. The marker file records one line per execution.
func TestAssignmentIsDeliveredOnlyOnce(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/workers/register":
			json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

		case r.URL.Path == "/v1/workers/w1/assignments":
			// Always hands out job1 again, never advancing it — the worst
			// case for a worker that relies solely on the controller to stop
			// re-delivering completed work.
			json.NewEncoder(w).Encode([]map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"sh", "-c", "echo run >> " + marker},
			}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case strings.HasPrefix(r.URL.Path, "/v1/jobs/") && strings.HasSuffix(r.URL.Path, "/logs"):
			w.WriteHeader(http.StatusOK)

		case strings.HasPrefix(r.URL.Path, "/v1/jobs/") && strings.HasSuffix(r.URL.Path, "/status"):
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Host:              "gpubox",
		Devices:           []string{"gpu0"},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          20 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	// Give the hot-spinning poll loop plenty of chances to re-deliver job1.
	time.Sleep(1500 * time.Millisecond)

	b, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(b), "run"),
		"job1 must be executed exactly once even though the controller keeps re-delivering it")
}

// TestShutdownWaitsForRunningJobAndReportsTerminalState asserts that
// cancelling Start's context does not abandon a job that is still running:
// Start must not return until the job has actually finished, and the
// terminal status report for it must have reached the controller.
func TestShutdownWaitsForRunningJobAndReportsTerminalState(t *testing.T) {
	var (
		mu     sync.Mutex
		served bool
		states []string
		wids   []string
	)
	runningSeen := make(chan struct{})

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
			json.NewEncoder(w).Encode([]map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"sleep", "5"},
			}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body struct {
				State    string `json:"state"`
				WorkerID string `json:"worker_id"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			states = append(states, body.State)
			wids = append(wids, body.WorkerID)
			mu.Unlock()
			if body.State == "running" {
				close(runningSeen)
			}
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	defer cancelShutdown()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Host:              "gpubox",
		Devices:           []string{"gpu0"},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          50 * time.Millisecond,
	})

	startReturned := make(chan struct{})
	go func() {
		_ = wk.Start(shutdownCtx)
		close(startReturned)
	}()

	select {
	case <-runningSeen:
	case <-time.After(10 * time.Second):
		t.Fatal("job never reported running")
	}

	cancelShutdown()

	select {
	case <-startReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after shutdown even though the job should die quickly under SIGTERM")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"running", "killed"}, states,
		"Start must wait for the terminal report to land before returning")
	require.Equal(t, []string{"w1", "w1"}, wids)
}

// TestTerminalReportIsRetriedOnServerError asserts that a terminal status
// report is retried after a transient server error: it is the call that
// frees the device's lease, so silently giving up after one failed attempt
// strands the device until the reaper notices.
func TestTerminalReportIsRetriedOnServerError(t *testing.T) {
	var (
		mu       sync.Mutex
		served   bool
		attempts int
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
			json.NewEncoder(w).Encode([]map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"true"},
			}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body struct {
				State string `json:"state"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			if body.State != "running" {
				mu.Lock()
				attempts++
				n := attempts
				mu.Unlock()
				if n == 1 {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
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
		ControllerURL:     ts.URL,
		Host:              "gpubox",
		Devices:           []string{"gpu0"},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          50 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts >= 2
	}, 10*time.Second, 50*time.Millisecond, "terminal report must be retried after a 500")
}

// TestStatusReportsIncludeWorkerID guards the ownership invariant the
// controller enforces (handleJobStatus rejects a status report with a
// missing or mismatched worker_id): both the "running" report and the
// terminal report must carry this worker's ID.
func TestStatusReportsIncludeWorkerID(t *testing.T) {
	var (
		mu        sync.Mutex
		served    bool
		workerIDs []string
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
			json.NewEncoder(w).Encode([]map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"true"},
			}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body struct {
				WorkerID string `json:"worker_id"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			workerIDs = append(workerIDs, body.WorkerID)
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
		ControllerURL:     ts.URL,
		Host:              "gpubox",
		Devices:           []string{"gpu0"},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          50 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(workerIDs) == 2
	}, 10*time.Second, 50*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"w1", "w1"}, workerIDs)
}
