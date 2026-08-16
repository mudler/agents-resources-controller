package server

import (
	"errors"
	"fmt"
	"io"
	"net"
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

// TestStreamLogsGivesUpOnAWedgedReader pins the per-write deadline on the log
// stream. A client that opens the stream and then stops reading — a suspended
// laptop, a blackholed route — fills the socket buffer, and w.Write blocks
// with no deadline forever: r.Context().Done() cannot preempt a write already
// in flight, so the handler never returns, logstore's deferred unsubscribe
// never runs, and the subscriber, its goroutine and its fd leak for the life
// of the process. `rc attach` makes long-lived log streams routine, so this
// is an ordinary case.
//
// The test connects with a raw socket precisely so it can refuse to read,
// which no http.Client-based test can do. With the deadline in place the
// server drops the connection, which the test observes as a clean EOF after
// it finally starts reading; without it the stream stays open and the read
// blocks until the test's own deadline.
func TestStreamLogsGivesUpOnAWedgedReader(t *testing.T) {
	original := logWriteTimeout
	logWriteTimeout = 200 * time.Millisecond
	t.Cleanup(func() { logWriteTimeout = original })

	dir := t.TempDir()
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))
	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	require.NoError(t, st.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1", State: model.DeviceReady}},
	))
	// A job that never reaches a terminal state, so the stream only ever ends
	// because the handler gives up on the reader.
	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	require.NoError(t, err)

	srv := New(Config{Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"ctok": "client"}})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	_, err = fmt.Fprintf(conn,
		"GET /v1/jobs/%s/logs HTTP/1.1\r\nHost: rc\r\nAuthorization: Bearer ctok\r\n\r\n",
		job.ID)
	require.NoError(t, err)

	// Far more than any socket buffer will hold, so the handler is certainly
	// blocked in a write while this test does not read.
	chunk := make([]byte, 256*1024)
	for i := range chunk {
		chunk[i] = 'x'
	}
	for i := 0; i < 32; i++ {
		require.NoError(t, logs.Append(job.ID, chunk))
	}

	time.Sleep(500 * time.Millisecond) // well past the shrunken write deadline

	// Now read. The server has already abandoned this connection, so whatever
	// it managed to buffer arrives and then the stream ends. Without the
	// deadline the server would still be feeding this connection and the read
	// would run until this bound instead.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
	_, err = io.Copy(io.Discard, conn)
	if err != nil {
		var netErr net.Error
		require.False(t, errors.As(err, &netErr) && netErr.Timeout(),
			"the server never gave up on a reader that stopped reading")
		require.NoError(t, err)
	}
}
