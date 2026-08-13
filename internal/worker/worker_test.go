package worker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
