package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/cli"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// syncBuffer is a concurrency-safe io.Writer so tests can capture rc's
// stderr output (via cmd.SetErr) without racing the goroutines cobra and
// net/http spin up around it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestRunQueuedShowsPositionsThenSucceeds drives a job that starts out
// queued, reports two distinct positions, then transitions to succeeded —
// `rc run` must surface the positions on stderr and still make it to a
// clean exit.
func TestRunQueuedShowsPositionsThenSucceeds(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}
	var polls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			n := polls.Add(1)
			view := server.JobView{Job: job, QueuePosition: 3}
			switch {
			case n >= 3:
				view.Job.State = model.JobSucceeded
				view.QueuePosition = 0
			case n == 2:
				view.QueuePosition = 1
			}
			_ = json.NewEncoder(w).Encode(view)
		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	stderr := &syncBuffer{}
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--", "true"})

	err := cmd.Execute()
	require.NoError(t, err)
	require.GreaterOrEqual(t, polls.Load(), int32(3), "expected polling to continue until the job left the queue")

	out := stderr.String()
	require.Contains(t, out, "rc: queued at position 3 for gpubox:gpu0")
	require.Contains(t, out, "rc: queued at position 1 for gpubox:gpu0")
}

// TestRunCtrlCWhileQueuedCancelsOutright verifies the distinction that
// matters most here: interrupting a job that is still QUEUED (no lease, no
// process) cancels it outright via rc kill, rather than merely detaching.
// The GET handler flips the job to "killed" once the kill lands, so this
// also pins the honest post-cancel message (Minor: run.go must say what
// actually happened, not unconditionally claim "cancelled").
func TestRunCtrlCWhileQueuedCancelsOutright(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}
	var killCalled atomic.Bool
	var killSubmitter atomic.Value

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			view := server.JobView{Job: job, QueuePosition: 5}
			if killCalled.Load() {
				view.Job.State = model.JobKilled
				view.QueuePosition = 0
			}
			_ = json.NewEncoder(w).Encode(view)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/job1/kill":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			killSubmitter.Store(body["submitter"])
			killCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	stderr := &syncBuffer{}
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--", "true"})

	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	go func() {
		<-time.After(50 * time.Millisecond)
		cancel()
	}()

	err := cmd.Execute()
	require.Error(t, err)

	var coded interface{ ExitCode() int }
	require.True(t, errors.As(err, &coded), "expected a coded exit error, got: %v", err)
	require.Equal(t, 130, coded.ExitCode())

	require.True(t, killCalled.Load(), "expected rc kill to be requested for the still-queued job")
	require.Equal(t, "tester", killSubmitter.Load())
	require.Contains(t, stderr.String(), "rc: cancelled queued job job1")
}

// TestRunUsesDefaultSubmitterForKillWhenAsUnset covers the exact case a
// real review bug lived in: with --as left unset (the default path every
// agent takes), the kill request must carry the resolved default identity,
// not an empty string — an empty submitter would not match job.Submitter
// server-side and the kill would be rejected as 403 not_job_owner.
func TestRunUsesDefaultSubmitterForKillWhenAsUnset(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}
	var killSubmitter atomic.Value

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			_ = json.NewEncoder(w).Encode(server.JobView{Job: job, QueuePosition: 5})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/job1/kill":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			killSubmitter.Store(body["submitter"])
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetErr(&syncBuffer{})
	// Deliberately no --as: exercises defaultSubmitter().
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--", "true"})

	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	go func() {
		<-time.After(50 * time.Millisecond)
		cancel()
	}()

	_ = cmd.Execute()

	submitter, _ := killSubmitter.Load().(string)
	require.NotEmpty(t, submitter, "kill must carry the resolved default submitter, not an empty string")
}

// TestRunCtrlCJobStartedJustBeforeCancel checks the other successful-kill
// outcome: the kill call itself succeeded (200), but by the time rc run
// re-checks, the job had actually started running in the window before the
// cancel landed — the controller flagged it for the worker instead of
// cancelling it outright (see server.handleKill / store.RequestKill). The
// message must say so, not claim "cancelled queued job".
func TestRunCtrlCJobStartedJustBeforeCancel(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}
	var killCalled atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			view := server.JobView{Job: job}
			if killCalled.Load() {
				// It started running right as the kill request landed;
				// RequestKill flagged it (async) rather than cancelling.
				view.Job.State = model.JobRunning
			} else {
				view.Job.State = model.JobQueued
				view.QueuePosition = 2
			}
			_ = json.NewEncoder(w).Encode(view)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/job1/kill":
			killCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	stderr := &syncBuffer{}
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--", "true"})

	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	go func() {
		<-time.After(50 * time.Millisecond)
		cancel()
	}()

	err := cmd.Execute()
	require.Error(t, err)
	out := stderr.String()
	require.Contains(t, out, "started running on gpubox:gpu0")
	require.NotContains(t, out, "rc: cancelled queued job", "must not claim a plain cancel when the job had actually started")
}

// TestRunCtrlCUncancellableStillRunning covers the Important-2 finding: the
// controller can refuse the kill outright (409 not_cancellable) if the job
// raced onto "running" between rc run's last poll and the kill attempt.
// That must not be reported as a bland "could not cancel" — the job is
// now actually consuming a device with nobody watching it, so rc run must
// print Stage 1's honest "STILL RUNNING" warning instead.
func TestRunCtrlCUncancellableStillRunning(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			view := server.JobView{Job: job}
			view.Job.State = model.JobRunning
			_ = json.NewEncoder(w).Encode(view)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/job1/kill":
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "not_cancellable", "message": "job already started",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	stderr := &syncBuffer{}
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--", "true"})

	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	go func() {
		<-time.After(50 * time.Millisecond)
		cancel()
	}()

	err := cmd.Execute()
	require.Error(t, err)

	var coded interface{ ExitCode() int }
	require.True(t, errors.As(err, &coded))
	require.Equal(t, 130, coded.ExitCode())

	out := stderr.String()
	require.Contains(t, out, "job1")
	require.Contains(t, out, "STILL RUNNING")
	require.Contains(t, out, "gpubox:gpu0")
	require.NotContains(t, out, "could not cancel queued job job1:", "must not fall back to the bland message when it knows the job is running")
}

// TestRunGivesUpAfterTimeoutCancelsQueuedJob checks the --timeout branch:
// distinct from Ctrl-C, but it must also cancel the still-queued job rather
// than leaving it stranded in the queue. The underlying error must be a
// genuine context.DeadlineExceeded — that is exactly the signal run.go
// uses to decide killing is safe (see TestRunTransientPollErrorsDoNotKillJob
// for the case where it is NOT safe).
func TestRunGivesUpAfterTimeoutCancelsQueuedJob(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}
	var killCalled atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			view := server.JobView{Job: job, QueuePosition: 5}
			if killCalled.Load() {
				view.Job.State = model.JobKilled
				view.QueuePosition = 0
			}
			_ = json.NewEncoder(w).Encode(view)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/job1/kill":
			killCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	stderr := &syncBuffer{}
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--timeout", "200ms", "--", "true"})

	err := cmd.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "gave up waiting")
	require.True(t, errors.Is(err, context.DeadlineExceeded), "expected the timeout error to unwrap to context.DeadlineExceeded, got: %v", err)
	require.True(t, killCalled.Load(), "expected rc kill to be requested after giving up")
	require.Contains(t, stderr.String(), "rc: cancelled queued job job1")
}

// TestRunTransientPollErrorsDoNotKillJob is the regression test for the
// Critical review finding: a run of poll failures while waiting to be
// scheduled (a wedged connection, a controller mid-restart) must NOT be
// treated the same as a genuine --timeout expiry. Before the fix,
// WaitScheduled gave up on the very first poll error and run.go's default
// branch killed unconditionally — which, if the job had actually been
// scheduled moments earlier, would terminate a job that was running just
// fine. After the fix: no --timeout is set, so there is no
// context.DeadlineExceeded to justify a kill, and rc run must leave the
// job alone.
func TestRunTransientPollErrorsDoNotKillJob(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}
	var killCalled atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			// Every poll after submit fails, simulating a wedged/erroring
			// controller. WaitScheduled must retry for a while, then give
			// up with an error that is NOT context.DeadlineExceeded.
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/job1/kill":
			killCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetErr(&syncBuffer{})
	// Deliberately no --timeout: the caller asked to wait indefinitely, so
	// a controller hiccup must cost patience, not the job.
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--", "true"})

	err := cmd.Execute()
	require.Error(t, err)

	var coded interface{ ExitCode() int }
	require.False(t, errors.As(err, &coded), "an error rc run doesn't understand must not carry a job exit code")
	require.False(t, killCalled.Load(), "a run of poll failures must never be treated as license to kill the job")
}

// TestRunNeverKillsOnHungControllerWithoutTimeout is the regression test
// for the second round of the Critical finding: checking
// errors.Is(err, context.DeadlineExceeded) is NOT a reliable way to detect
// "the caller's own --timeout elapsed", because WaitScheduled bounds each
// individual poll with its own per-request timeout — so a controller that
// accepts connections but never answers (a genuine wedge, not an
// instantly-failing one) produces exactly that same wrapped error, with no
// caller deadline in sight at all. With no --timeout flag, rc run must
// never take the "genuine timeout" branch here; it may only kill when the
// caller actually asked for a bound and that bound elapsed.
//
// This must run against a real hang (never respond), not an instant
// failure (see TestRunTransientPollErrorsDoNotKillJob) — the earlier test
// cannot reach this bug, since a 500 returned immediately never exercises
// WaitScheduled's per-request context timeout at all.
func TestRunNeverKillsOnHungControllerWithoutTimeout(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobQueued}
	var killCalled atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/job1":
			// Accept the connection, never answer it — a genuine wedge,
			// not an instantly-failing error. Each poll times out against
			// its own per-request context, which is exactly the trap:
			// that per-request timeout also produces
			// context.DeadlineExceeded, indistinguishable from a real
			// --timeout expiry if the caller inspects the error's wrapped
			// content instead of its own context.
			<-r.Context().Done()
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/job1/kill":
			killCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetErr(&syncBuffer{})
	// Deliberately no --timeout: the caller asked to wait indefinitely.
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--", "true"})

	err := cmd.Execute()
	require.Error(t, err)

	var coded interface{ ExitCode() int }
	require.False(t, errors.As(err, &coded), "an error rc run doesn't understand must not carry a job exit code")
	require.Contains(t, err.Error(), "job1")
	require.Contains(t, err.Error(), "lost track",
		"a hung controller with no --timeout must be reported as lost track, not as a timeout giveup")
	require.NotContains(t, err.Error(), "gave up waiting",
		"must not be reported using the --timeout giveup wording when the caller never asked for a bound")
	require.False(t, killCalled.Load(), "a hung controller must never be treated as license to kill the job when no --timeout was given")
}

// TestRunDetachesFromRunningJobOnCtrlC pins Stage 1's behaviour, which this
// task must not regress: Ctrl-C on a job that is already RUNNING (a lease
// exists, a process is on the device) only detaches — it must never kill.
// This is the mirror image of TestRunCtrlCWhileQueuedCancelsOutright and,
// before this fix round, was not covered by any test that actually
// exercised the interrupt path end to end.
func TestRunDetachesFromRunningJobOnCtrlC(t *testing.T) {
	job := model.Job{ID: "job1", DeviceID: "gpubox:gpu0", State: model.JobRunning}
	logsReached := make(chan struct{}, 1)
	var killCalled atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case logsReached <- struct{}{}:
			default:
			}
			// Block until the client disconnects (simulating Ctrl-C),
			// exactly like a job that is still producing output.
			<-r.Context().Done()
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/job1/kill":
			killCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewRunCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	stderr := &syncBuffer{}
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"-d", "gpubox:gpu0", "--as", "tester", "--", "true"})

	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	go func() {
		<-logsReached
		cancel()
	}()

	err := cmd.Execute()
	require.Error(t, err)

	var coded interface{ ExitCode() int }
	require.True(t, errors.As(err, &coded), "expected a coded exit error, got: %v", err)
	require.Equal(t, 130, coded.ExitCode())

	out := stderr.String()
	require.Contains(t, out, "STILL RUNNING")
	require.Contains(t, out, "rc ps")
	require.False(t, killCalled.Load(), "Ctrl-C on a RUNNING job must only detach — it must never kill the job")
}
