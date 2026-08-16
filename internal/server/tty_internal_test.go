package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTTYHandoffTimeoutFreesAHalfWithNoPeer pins the only bound a blocked
// pipe write can have. A worker that dials out for a session the operator's
// terminal never joins blocks in io.Pipe.Write, which no request context can
// preempt — and the two POST halves get no context cancellation at all while
// their request body is unread, because net/http only starts the background
// read that notices a dropped connection once the body hits EOF. Without
// ttyHandoffTimeout that handler, its connection and its session slot are
// pinned for the life of the process, which is exactly the leak that stays
// invisible until a controller has been up for a week.
//
// It also builds its server with no store at all, a blunter form of the
// promise the relay makes: not one of these code paths may reach SQLite.
func TestTTYHandoffTimeoutFreesAHalfWithNoPeer(t *testing.T) {
	original := ttyHandoffTimeout
	ttyHandoffTimeout = 100 * time.Millisecond
	t.Cleanup(func() { ttyHandoffTimeout = original })

	srv := New(Config{Tokens: map[string]string{"wtok": "worker"}})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		ts.URL+"/v1/jobs/job-lonely/tty/out", pr)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer wtok")

	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Output nobody is there to take.
	_, err = pw.Write([]byte("into the void"))
	require.NoError(t, err)

	// The handler gives up, so the response ends instead of hanging forever.
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, resp.Body)
		done <- err
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a half with no peer never gave up: its goroutine and connection are pinned")
	}

	// And the session it created is gone from the registry, not left behind
	// as a closed corpse a reconnecting worker would attach to.
	require.Eventually(t, func() bool {
		srv.tty.mu.Lock()
		defer srv.tty.mu.Unlock()
		return len(srv.tty.sessions) == 0
	}, 5*time.Second, 10*time.Millisecond, "the abandoned session stayed in the registry")
}
