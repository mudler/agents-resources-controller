package worker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

// TestHoldRunsItsOwnSleeperNotTheStoredCommand pins task 8's security
// property at the one layer that actually spawns a process: a hold's
// assignment carries a command that would fail INSTANTLY if the worker ever
// executed it (a nonexistent binary). If execute ever looked at a.Command for
// a hold, the job would report "failed" within milliseconds. Instead it must
// run to its watchdog deadline and be killed by max_runtime — proving the
// stored command was never touched, only the worker's own sleeper was.
func TestHoldRunsItsOwnSleeperNotTheStoredCommand(t *testing.T) {
	var (
		mu      sync.Mutex
		served  bool
		states  []string
		reasons []string
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
				"job_id":              "job1",
				"device_id":           "gpubox:gpu0",
				"kind":                "hold",
				"command":             []string{"/no/such/binary-rc-hold-test"},
				"max_runtime_seconds": 1,
			}}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body struct {
				State  string `json:"state"`
				Reason string `json:"reason"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			states = append(states, body.State)
			reasons = append(reasons, body.Reason)
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
		return len(states) >= 2
	}, 10*time.Second, 50*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "running", states[0], "the hold must still report running like any other job")
	final := states[len(states)-1]
	require.Equal(t, "killed", final,
		"a hold must end via the watchdog, not an exec failure — if it reports failed, the stored command was used")
	require.Contains(t, reasons[len(reasons)-1], "max_runtime exceeded",
		"the terminal reason must name the watchdog, confirming the sleeper (not the stored command) ran")
}
