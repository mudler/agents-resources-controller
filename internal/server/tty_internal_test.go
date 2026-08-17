package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// newTTYRelay builds a server with one allocated job, which a session needs
// because the relay validates the job when a half connects.
func newTTYRelay(t *testing.T) (*httptest.Server, *Server, string) {
	t.Helper()
	dir := t.TempDir()
	c := clock.NewFake(time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC))

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	require.NoError(t, st.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1", State: model.DeviceReady}},
	))
	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"/bin/sh"}, Submitter: "agent-a",
	})
	require.NoError(t, err)

	srv := New(Config{Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client"}})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, srv, job.ID
}

// openStream starts a request and returns its response plus, for a POST, the
// writer feeding its body. The handler flushes its 200 before relaying, so
// this returns while the body is still open.
func openStream(t *testing.T, ts *httptest.Server, method, token, path string) (*http.Response, io.WriteCloser) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var body io.Reader
	var w io.WriteCloser
	if method == http.MethodPost {
		pr, pw := io.Pipe()
		body, w = pr, pw
	}
	req, err := http.NewRequestWithContext(ctx, method, ts.URL+path, body)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return resp, w
}

func sessionCount(srv *Server) int {
	srv.tty.mu.Lock()
	defer srv.tty.mu.Unlock()
	return len(srv.tty.sessions)
}

// TestTTYHandoffTimeoutFreesAHalfWithNoPeer pins the only bound a blocked
// pipe write can have. A worker that dials out for a session nothing else
// ever joins blocks in io.Pipe.Write, which no request context can preempt:
// net/http reports a dropped connection to a handler blocked in a *body
// read*, and a handler blocked in the pipe write is not in one — it only
// starts the background read that would notice one once the body hits EOF.
// Without this bound that handler, its connection and its session slot are
// pinned for the life of the process, which is exactly the leak that stays
// invisible until a controller has been up for a week.
func TestTTYHandoffTimeoutFreesAHalfWithNoPeer(t *testing.T) {
	original := ttyHandoffTimeout
	ttyHandoffTimeout = 100 * time.Millisecond
	t.Cleanup(func() { ttyHandoffTimeout = original })

	ts, srv, job := newTTYRelay(t)
	resp, body := openStream(t, ts, http.MethodPost, "wtok", "/v1/jobs/"+job+"/tty/out")

	// Output nobody is there to take, from the only half attached.
	_, err := body.Write([]byte("into the void"))
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
	require.Eventually(t, func() bool { return sessionCount(srv) == 0 },
		5*time.Second, 10*time.Millisecond, "the abandoned session stayed in the registry")
}

// TestTTYTypeAheadSurvivesALongQueueWait is the counterpart, and the case
// that matters far more often. An operator attaches both halves the moment
// they submit; the job then waits for a GPU, which on a controller whose
// whole purpose is leasing *exclusive* GPUs is routinely longer than any
// timeout worth having. Their opening resize frame — or anything they type —
// blocks in the pipe write for that whole wait.
//
// A handoff bound that fired on that write would close the input pipe, and
// the operator would get a terminal that prints perfectly and silently
// swallows every keystroke for the rest of the job: no error, nothing in the
// logs, and every reconnect landing on the same dead pipe. So the bound only
// ends a half that is the *only* thing attached to its session.
//
// This test deliberately outlasts several handoff windows before the worker
// arrives, because a version of it that finishes in milliseconds never arms
// the timer and would pass against the bug.
func TestTTYTypeAheadSurvivesALongQueueWait(t *testing.T) {
	original := ttyHandoffTimeout
	ttyHandoffTimeout = 100 * time.Millisecond
	t.Cleanup(func() { ttyHandoffTimeout = original })

	ts, _, job := newTTYRelay(t)

	// The operator attaches and types while the job is still queued.
	openStream(t, ts, http.MethodGet, "ctok", "/v1/jobs/"+job+"/tty/out")
	_, keys := openStream(t, ts, http.MethodPost, "ctok", "/v1/jobs/"+job+"/tty/in")
	require.NoError(t, WriteTTYFrame(keys, TTYResize(48, 180)))

	// The wait for a device. Several handoff windows pass with the write
	// blocked, so the guard fires repeatedly and must re-arm every time.
	time.Sleep(5 * ttyHandoffTimeout)

	// The box finally picks the job up.
	workerIn, _ := openStream(t, ts, http.MethodGet, "wtok", "/v1/jobs/"+job+"/tty/in")

	var want []byte
	buf := &sliceWriter{}
	require.NoError(t, WriteTTYFrame(buf, TTYResize(48, 180)))
	want = buf.b

	got := make([]byte, len(want))
	read := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(workerIn.Body, got)
		read <- err
	}()
	select {
	case err := <-read:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the terminal went one-way: nothing typed during the queue wait ever reached the box")
	}
	require.Equal(t, string(want), string(got))
}

type sliceWriter struct{ b []byte }

func (s *sliceWriter) Write(p []byte) (int, error) { s.b = append(s.b, p...); return len(p), nil }

// TestTTYGivesUpOnAWedgedReader pins the per-write deadline, the same one the
// log stream has (see TestStreamLogsGivesUpOnAWedgedReader). An operator's
// terminal that stops reading — a suspended laptop, a blackholed route —
// fills the socket buffer, and w.Write then blocks with no deadline forever:
// the request context cannot preempt a write already in flight, so the
// handler never returns and its goroutine, its connection and its session
// live for the life of the process.
//
// The test connects with a raw socket precisely so it can refuse to read,
// which no http.Client-based test can do. With the deadline in place the
// server drops the connection, which the test observes as an immediate end
// once it finally starts reading; without it the server is still feeding this
// socket and the read runs until the test's own bound instead.
func TestTTYGivesUpOnAWedgedReader(t *testing.T) {
	original := ttyWriteTimeout
	ttyWriteTimeout = 200 * time.Millisecond
	t.Cleanup(func() { ttyWriteTimeout = original })

	ts, srv, job := newTTYRelay(t)

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_, err = fmt.Fprintf(conn,
		"GET /v1/jobs/%s/tty/out HTTP/1.1\r\nHost: rc\r\nAuthorization: Bearer ctok\r\n\r\n",
		job)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sessionCount(srv) == 1 },
		5*time.Second, 10*time.Millisecond, "the wedged terminal never attached")

	// The box prints far more than any socket buffer can hold — loopback
	// buffers alone swallow several megabytes — so the handler is certainly
	// blocked in a write while this test does not read.
	_, body := openStream(t, ts, http.MethodPost, "wtok", "/v1/jobs/"+job+"/tty/out")
	go func() {
		chunk := make([]byte, 256*1024)
		for range 256 {
			if _, err := body.Write(chunk); err != nil {
				return
			}
		}
	}()

	time.Sleep(5 * ttyWriteTimeout) // well past the shrunken write deadline

	// Now read. The server has already abandoned this connection, so the
	// stream ends at once. Without the deadline it is still feeding this
	// socket and this read runs until its own bound.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err = io.Copy(io.Discard, conn)
	if err != nil {
		var netErr net.Error
		require.False(t, errors.As(err, &netErr) && netErr.Timeout(),
			"the relay never gave up on a terminal that stopped reading")
		require.NoError(t, err)
	}
}

// A session exists only while somebody is on it. Left in the map, every job
// ID a terminal was ever opened for would occupy memory until the controller
// restarted — which for a long-lived controller is the same shape of leak as
// a goroutine, only quieter.
func TestTTYRegistryDropsASessionNobodyIsAttachedTo(t *testing.T) {
	reg := newTTYRegistry()

	sess, _, err := reg.acquire("job", slotClientOut)
	require.NoError(t, err)
	same, _, err := reg.acquire("job", slotWorkerOut)
	require.NoError(t, err)
	require.Same(t, sess, same, "both halves of one job must land on one session")

	reg.release(sess, slotClientOut)
	require.Len(t, reg.sessions, 1, "a session with a half still on it must stay")

	reg.release(sess, slotWorkerOut)
	require.Empty(t, reg.sessions, "a session nobody is attached to must be dropped")

	select {
	case <-sess.done:
	default:
		t.Fatal("the dropped session was not closed, so its pipes still pin whoever is on them")
	}
}

// A half that is slow to unwind must not delete the replacement session a
// reconnecting peer has already registered. Without the identity check the
// two ends of one terminal end up on two different sessions, each silent to
// the other, and nothing reports it.
func TestTTYRegistryKeepsTheReplacementSession(t *testing.T) {
	reg := newTTYRegistry()

	old, _, err := reg.acquire("job", slotClientOut)
	require.NoError(t, err)
	reg.discard(old) // the output half ended, so the session is over

	fresh, _, err := reg.acquire("job", slotClientOut) // the peer reconnects
	require.NoError(t, err)
	require.NotSame(t, old, fresh)

	reg.release(old, slotClientOut) // the old half finally unwinds

	require.Same(t, fresh, reg.sessions["job"],
		"a stale half deleted the session its replacement had already registered")
}
