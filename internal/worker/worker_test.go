package worker_test

import (
	"bytes"
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

	"github.com/mudler/agents-resources-controller/internal/worker"
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
			json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"sh", "-c", "echo hello; exit 7"},
			}}})

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
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
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
			json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"sh", "-c", "echo run >> " + marker},
			}}})

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
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
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

// TestKilledJobIsNotStartedWhenDeliveredWithItsAssignment guards against a
// job whose kill was requested between ScheduleOnce assigning it and the
// worker's poll landing: both the assignment and the kill for the same job
// ID can arrive in the very same poll response. A job the controller has
// already been told to kill must never be spawned — starting it anyway
// allocates VRAM on a device someone may already be waiting for, and the
// user who typed `rc kill` has every reason to believe nothing ran. The
// marker file records an execution the same way TestAssignmentIsDeliveredOnlyOnce
// does; here it must never exist at all.
func TestKilledJobIsNotStartedWhenDeliveredWithItsAssignment(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")

	var (
		mu     sync.Mutex
		served bool
		states []string
		wids   []string
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
			// job1 arrives as a fresh assignment AND as an already-flagged
			// kill in the same response — exactly what the controller sends
			// when `rc kill` lands in the window between assignment and poll.
			json.NewEncoder(w).Encode(map[string]any{
				"assignments": []map[string]any{{
					"job_id":    "job1",
					"device_id": "gpubox:gpu0",
					"command":   []string{"sh", "-c", "echo run >> " + marker},
				}},
				"kills": []string{"job1"},
			})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
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
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          50 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(states) == 1
	}, 5*time.Second, 50*time.Millisecond,
		"a job killed before it ever started must still get a terminal report, not be left sitting assigned")

	// Give the bug every reasonable chance to manifest before asserting it
	// never does: the command must never actually execute.
	time.Sleep(300 * time.Millisecond)
	_, err := os.Stat(marker)
	require.True(t, os.IsNotExist(err),
		"the command must never run once its own delivery also carries a kill for the same job ID")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"killed"}, states)
	require.Equal(t, []string{"w1"}, wids)
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
			json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"sleep", "5"},
			}}})

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
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
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

// TestWorkerKillsJobWhenControllerRequestsIt proves the other end of the wire
// change this task makes: a job ID arriving in the poll envelope's "kills"
// must actually reach the running process, not just get acknowledged. The
// job here ignores nothing special — an ordinary "sleep 30" that would run
// long past the test's timeout on its own — so a "killed" terminal report
// only shows up if the kill was actually delivered through Run's existing
// SIGTERM path via the running map, not merely logged.
func TestWorkerKillsJobWhenControllerRequestsIt(t *testing.T) {
	var (
		mu           sync.Mutex
		served       bool
		sendKill     bool
		states       []string
		exitReported bool
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
			shouldKill := sendKill && !exitReported
			mu.Unlock()
			if first {
				json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
					"job_id":    "job1",
					"device_id": "gpubox:gpu0",
					"command":   []string{"sleep", "30"},
				}}})
				return
			}
			if shouldKill {
				json.NewEncoder(w).Encode(map[string]any{"kills": []string{"job1"}})
				return
			}
			w.WriteHeader(http.StatusNoContent)

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body struct {
				State string `json:"state"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			states = append(states, body.State)
			if body.State == "running" {
				sendKill = true
				close(runningSeen)
			}
			if body.State != "running" {
				exitReported = true
			}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Host:              "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          50 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	select {
	case <-runningSeen:
	case <-time.After(10 * time.Second):
		t.Fatal("job never reported running")
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(states) == 2
	}, 15*time.Second, 50*time.Millisecond,
		"a kill delivered through the poll envelope must terminate the running job")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"running", "killed"}, states,
		"the job that was still sleeping must be reported killed, not left to finish or reported some other way")
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
			json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"true"},
			}}})

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
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
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

// TestCUDAVisibleDevicesIsSetFromTheDeviceName guards the fix for the
// review finding that CUDA_VISIBLE_DEVICES was never set at all: on a
// multi-GPU host a job leased gpu1 would see every GPU, defeating the whole
// point of leasing a specific device. The convention is the trailing
// integer of the device NAME (the part after "host:"); a device with no
// trailing integer must leave the variable unset rather than guess wrong,
// and an operator-supplied value already present in the job's env must
// never be overridden. Each case's job command prints exactly one line
// naming what it sees, so the assertion never depends on parsing a full
// environment dump.
func TestCUDAVisibleDevicesIsSetFromTheDeviceName(t *testing.T) {
	const unsetMarker = "__UNSET__"

	tests := []struct {
		name     string
		deviceID string
		jobEnv   map[string]string
		want     string
	}{
		{
			name:     "trailing index sets CUDA_VISIBLE_DEVICES",
			deviceID: "gpubox:gpu1",
			want:     "1",
		},
		{
			name:     "no trailing index leaves it unset",
			deviceID: "gpubox:mydevice",
			want:     unsetMarker,
		},
		{
			name:     "operator-supplied value is preserved, not overridden",
			deviceID: "gpubox:gpu1",
			jobEnv:   map[string]string{"CUDA_VISIBLE_DEVICES": "0,1"},
			want:     "0,1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				mu     sync.Mutex
				logs   []byte
				served bool
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
					out := map[string]any{
						"job_id":    "job1",
						"device_id": tc.deviceID,
						"command":   []string{"sh", "-c", "echo CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-" + unsetMarker + "}"},
					}
					if len(tc.jobEnv) > 0 {
						out["env"] = tc.jobEnv
					}
					json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{out}})

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
				Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
				HeartbeatInterval: 500 * time.Millisecond,
				PollWait:          50 * time.Millisecond,
			})
			go func() { _ = wk.Start(ctx) }()

			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return bytes.Contains(logs, []byte("CUDA_VISIBLE_DEVICES="))
			}, 10*time.Second, 50*time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			require.Contains(t, string(logs), "CUDA_VISIBLE_DEVICES="+tc.want)
		})
	}
}

// TestPollLoopFloorsDelayAgainstMisbehavingController guards the fix for the
// review finding that a non-conforming controller (or a proxy in front of
// it) which answers the long-poll instantly, instead of blocking for
// roughly PollWait, can turn a single worker's poll loop into thousands of
// requests per second — busy-looping the controller and stalling scheduling
// fleet-wide, not just for this one worker. A conforming controller with a
// real wait= parameter is unaffected: this floor only bites when polls
// return instantly.
func TestPollLoopFloorsDelayAgainstMisbehavingController(t *testing.T) {
	var (
		mu    sync.Mutex
		polls int
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/workers/register":
			json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

		case r.URL.Path == "/v1/workers/w1/assignments":
			mu.Lock()
			polls++
			mu.Unlock()
			// Misbehaving: ignores wait= and answers empty-handed instantly,
			// the way a non-conforming controller or an interfering proxy
			// might.
			json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	const window = 1200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Host:              "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: time.Second,
		PollWait:          10 * time.Millisecond, // irrelevant: the fake controller ignores it anyway
	})
	go func() { _ = wk.Start(ctx) }()

	<-ctx.Done()
	time.Sleep(100 * time.Millisecond) // let any poll already in flight land

	mu.Lock()
	n := polls
	mu.Unlock()

	// With no floor, this fake controller's near-zero response latency lets
	// pollLoop spin at network-round-trip speed — hundreds to thousands of
	// requests in ~1.2s on a local httptest server. With the 250ms floor,
	// ~1.2s / 250ms caps it at roughly 5, plus a little slack for scheduling
	// jitter and the request already in flight when the window closed.
	require.Less(t, n, 20,
		"a misbehaving controller that never blocks the long-poll must not be hammered thousands of times per second by a single worker; got %d polls in %s", n, window)
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
			json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"true"},
			}}})

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
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
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
