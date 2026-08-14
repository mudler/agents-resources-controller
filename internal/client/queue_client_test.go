package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

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

// TestWaitScheduledRetriesTransientErrorsThenSucceeds reproduces the
// Critical review finding: a brief controller blip (a couple of failed
// polls — connection refused, a 500 during a restart) must be absorbed by
// retrying, not treated as "give up". Before the fix, WaitScheduled
// returned on the very first JobView error with no retry at all, which
// made rc run kill a job that was actually just fine.
func TestWaitScheduledRetriesTransientErrorsThenSucceeds(t *testing.T) {
	var n atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) <= 2 {
			// Simulate a controller restart: the first two polls fail.
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"job":            map[string]any{"id": "job1", "state": "assigned"},
			"queue_position": 0,
		})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	job, err := c.WaitScheduled(context.Background(), "job1", nil)
	require.NoError(t, err, "a couple of transient poll failures must not surface as an error")
	require.Equal(t, "assigned", string(job.State))
	require.GreaterOrEqual(t, n.Load(), int32(3), "expected WaitScheduled to retry past the failures")
}

// TestWaitScheduledGivesUpAfterTooManyConsecutiveFailures checks the other
// side of the retry: WaitScheduled must still bound how long it tolerates a
// genuinely wedged controller, and the error it gives up with must NOT be
// context.DeadlineExceeded — rc run uses that specific sentinel to decide
// whether killing the still-queued job is safe, and a "poll failures"
// giveup is not the same thing as a real --timeout expiry.
func TestWaitScheduledGivesUpAfterTooManyConsecutiveFailures(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	start := time.Now()
	job, err := c.WaitScheduled(context.Background(), "job1", nil)
	require.Error(t, err)
	require.Nil(t, job)
	require.False(t, errors.Is(err, context.DeadlineExceeded),
		"a poll-failure giveup must not look like a --timeout expiry: got %v", err)
	require.Less(t, time.Since(start), 30*time.Second, "giveup must be bounded, not instant nor unbounded")
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

// TestKillSurfacesNotCancellableAsSentinel lets callers distinguish "the
// job raced onto a different state" (409 not_cancellable) from an ordinary
// failure via errors.Is, instead of string-matching the message — needed
// by rc run to decide whether to print the Stage 1 "STILL RUNNING" warning.
func TestKillSurfacesNotCancellableAsSentinel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "not_cancellable", "message": "job already started",
		})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	err := c.Kill(context.Background(), "job1", "agent-a")
	require.Error(t, err)
	require.True(t, errors.Is(err, client.ErrNotCancellable), "expected ErrNotCancellable, got: %v", err)
	require.Contains(t, err.Error(), "job already started")
}

// TestSubmitRejectsSubSecondMaxRuntime mirrors the worker's own device-config
// validation (internal/worker/config.go): the wire format is whole seconds,
// so a positive-but-sub-second MaxRuntime would silently truncate to 0 —
// "no ceiling" — at the controller. Reject it here, where the value the
// caller actually asked for can still be named.
func TestSubmitRejectsSubSecondMaxRuntime(t *testing.T) {
	c := client.New("http://unused.invalid", "ctok")
	_, err := c.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "gpubox:gpu0", Command: []string{"true"}, Submitter: "agent-a",
		MaxRuntime: 500 * time.Millisecond,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "second")
}
