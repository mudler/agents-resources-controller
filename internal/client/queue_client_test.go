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

	"github.com/mudler/resource-controller/internal/client"
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
// run of instant-failing polls (connection refused, a 500), staying well
// clear of both "instant giveup" and "unbounded". The retry budget here is
// tuned to comfortably survive a several-second controller restart — see
// the wall-clock accounting on the constants above WaitScheduled.
func TestWaitScheduledGivesUpAfterTooManyConsecutiveFailures(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	start := time.Now()
	job, err := c.WaitScheduled(context.Background(), "job1", nil)
	elapsed := time.Since(start)
	require.Error(t, err)
	require.Nil(t, job)
	require.GreaterOrEqual(t, elapsed, 3*time.Second,
		"the retry budget looked too thin for a several-second controller restart: gave up after only %s", elapsed)
	require.Less(t, elapsed, 30*time.Second, "giveup must be bounded, not unbounded")
}

// TestWaitScheduledGivesUpOnHungControllerWithinBudget reproduces the
// scenario the Critical review finding actually lived in: not an
// instantly-failing poll (500, connection refused — see the test above)
// but a HUNG one, where the controller accepts the connection and never
// answers, exactly what a restart or a wedge looks like from the outside.
// Each poll times out against its own per-request scheduledPollTimeout,
// and the giveup error legitimately unwraps to context.DeadlineExceeded
// here — with no caller deadline in sight at all. That is exactly why a
// caller (rc run) deciding whether to kill must check its OWN context's
// Err() directly rather than unwrapping this error; see
// TestRunNeverKillsOnHungControllerWithoutTimeout in internal/cli for the
// caller-side proof. This test only pins that WaitScheduled's own giveup
// stays bounded to a reasonable window rather than the 60+ seconds
// WaitTerminal's shared, far more generous pollTimeout would have produced
// here.
func TestWaitScheduledGivesUpOnHungControllerWithinBudget(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // never respond — simulate a wedged controller
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	start := time.Now()
	job, err := c.WaitScheduled(context.Background(), "job1", nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Nil(t, job)
	require.Less(t, elapsed, 40*time.Second, "a hung controller must be given up on well under a minute")
	// Document the trap this test exists to name: the giveup error DOES
	// legitimately unwrap to context.DeadlineExceeded here, even though no
	// caller ever asked for a bound — proving why run.go must not decide
	// by unwrapping this error (see the WaitScheduled doc comment).
	require.True(t, errors.Is(err, context.DeadlineExceeded),
		"expected the hung-controller giveup to unwrap to context.DeadlineExceeded — "+
			"if this ever stops being true, the caller-side waitCtx.Err() check in "+
			"cli.NewRunCmd may no longer be the operative defense")
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

// TestSubmitRejectsSubSecondIdleTimeout closes the asymmetry flagged in
// review: IdleTimeoutSeconds shares the exact same whole-seconds wire
// format and truncation risk as MaxRuntimeSeconds above, so a declared
// sub-second idle timeout must not be allowed to silently evaporate into
// "no idle limit" either.
func TestSubmitRejectsSubSecondIdleTimeout(t *testing.T) {
	c := client.New("http://unused.invalid", "ctok")
	_, err := c.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "gpubox:gpu0", Command: []string{"true"}, Submitter: "agent-a",
		IdleTimeout: 500 * time.Millisecond,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "second")
}
