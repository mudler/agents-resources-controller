package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestReportTerminalWithRetryBoundsEachAttempt guards the fix for the
// review finding that shutdownGrace's comment was false: nothing capped an
// individual terminal-report HTTP call, so a blackholed controller (TCP
// accepted, nothing ever comes back) could let a single attempt run for w's
// full 2-minute http.Client timeout — and five of those blow through the
// 45s shutdown grace many times over, silently abandoning jobs that
// shutdownGrace's own comment claimed they wouldn't be. Each attempt must
// have its own bounded timeout so the retry loop's total worst case is
// actually knowable and actually fits.
//
// terminalReportAttemptTimeout is shrunk here (it is a var precisely so a
// test can do this) so the test proves the bound is enforced without
// waiting minutes on a real blackholed connection.
func TestReportTerminalWithRetryBoundsEachAttempt(t *testing.T) {
	orig := terminalReportAttemptTimeout
	terminalReportAttemptTimeout = 50 * time.Millisecond
	t.Cleanup(func() { terminalReportAttemptTimeout = orig })

	// blackholeOver stands in for the network never delivering anything back
	// — the handler blocks on it, not on the request's own context, since
	// real blackholing gives the server side no disconnect signal either.
	// Closing it (deferred below, so it runs before ts.Close in LIFO order)
	// releases the stuck handlers once the test's real assertions are done,
	// so ts.Close doesn't itself hang waiting on them.
	blackholeOver := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blackholeOver
	}))
	defer ts.Close()
	defer close(blackholeOver)

	w := New(Config{ControllerURL: ts.URL, Token: "t"})
	w.workerID = "w1"

	start := time.Now()
	done := make(chan struct{})
	go func() {
		w.reportTerminalWithRetry(context.Background(), "job1", map[string]any{"state": "succeeded"})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reportTerminalWithRetry did not return within 5s; a per-attempt timeout is not being enforced")
	}
	elapsed := time.Since(start)

	// Worst case with the shrunk timeout: terminalReportAttempts (5) *
	// terminalReportAttemptTimeout (50ms) plus the retry backoff
	// (200+400+800+1600ms = 3s) ≈ 3.25s. The real assertion is that this
	// terminates at all in bounded time — a blackholed controller under the
	// unfixed code would have hung for terminalReportAttempts * the
	// client's full 2-minute Timeout instead.
	require.Less(t, elapsed, 4*time.Second,
		"each terminal-report attempt must be capped by its own timeout, not left to the client's multi-minute default")
}

// TestReportFaultRetriesUntilTheControllerAccepts guards the fix for a fault
// report that was fire-and-forget: the acquire hook has just said the GPU is
// not free, and this call is the only thing standing between that device and
// the next job the controller schedules onto it. One dropped attempt used to
// leave it schedulable. It carries the terminal report's weight, so it gets
// the terminal report's retry budget.
func TestReportFaultRetriesUntilTheControllerAccepts(t *testing.T) {
	var (
		mu       sync.Mutex
		attempts int
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable) // controller restarting
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	w := New(Config{ControllerURL: ts.URL, Token: "t"})
	w.reportFault(context.Background(), "gpubox:gpu0", "acquire hook failed: exited 1")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 3, attempts,
		"a transient failure must be retried until the controller actually quarantines the device")
}

// TestReportFaultDoesNotRetryA404 is the other half: an unknown device (or a
// rejected token) answers the same way five times over, so retrying only
// delays the log line that tells the operator the device was never actually
// quarantined.
func TestReportFaultDoesNotRetryA404(t *testing.T) {
	var (
		mu       sync.Mutex
		attempts int
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	w := New(Config{ControllerURL: ts.URL, Token: "t"})
	w.reportFault(context.Background(), "gpubox:nosuch", "acquire hook failed")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, attempts, "a 4xx will never become a 2xx; retrying it buys nothing")
}
