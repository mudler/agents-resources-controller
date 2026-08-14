package client_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/stretchr/testify/require"
)

// WaitScheduled reports each new queue position, then returns once the job
// leaves the queue.
func TestWaitScheduledReportsPositionsThenReturns(t *testing.T) {
	var polls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := polls.Add(1)
		state, pos := "queued", 3
		switch {
		case n >= 3:
			state, pos = "assigned", 0
		case n == 2:
			pos = 1
		}
		json.NewEncoder(w).Encode(map[string]any{
			"job":            map[string]any{"id": "job1", "state": state},
			"queue_position": pos,
		})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	var seen []int
	job, err := c.WaitScheduled(context.Background(), "job1", func(p int) { seen = append(seen, p) })
	require.NoError(t, err)
	require.Equal(t, "assigned", string(job.State))
	require.Equal(t, []int{3, 1}, seen, "each distinct position is reported once")
}

func TestKillSendsSubmitterAndSurfacesForbidden(t *testing.T) {
	var gotBody map[string]string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "not_job_owner", "message": "only the submitter may kill this job",
		})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	err := c.Kill(context.Background(), "job1", "agent-a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not_job_owner")
	require.Equal(t, "agent-a", gotBody["submitter"])
}
