package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// ttyDeadline bounds every read a TTY test makes. It is never reached by a
// passing test — it exists so a relay that drops a byte fails with the
// message below instead of hanging until the package timeout kills every
// other test's stack with it.
const ttyDeadline = 5 * time.Second

// newTTYServer is newServer with a worker registered and a job factory. The
// relay validates the job once, when a half connects — exactly as the log
// stream does — so a session needs a real job to open against.
func newTTYServer(t *testing.T) (*httptest.Server, *store.Store, func() string) {
	t.Helper()
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	newJob := func() string {
		t.Helper()
		resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
			DeviceID: "gpubox:gpu0", Command: []string{"/bin/sh"}, Submitter: "agent-a",
		})
		defer resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		var job model.Job
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&job))
		require.NotEmpty(t, job.ID)
		return job.ID
	}
	return ts, st, newJob
}

// ttyStream is one half of a session as the test drives it: the body it
// writes to (POST halves), the body it reads from (GET halves), and the
// cancel that aborts the connection the way a killed client would.
type ttyStream struct {
	w      io.WriteCloser
	r      io.ReadCloser
	resp   *http.Response
	cancel context.CancelFunc
}

// abort drops the connection without letting the request finish, which is
// what a client that is killed, suspended or unplugged looks like from the
// controller's side.
func (s *ttyStream) abort() {
	s.cancel()
	if s.r != nil {
		_ = s.r.Close()
	}
	if s.w != nil {
		_ = s.w.Close()
	}
}

// openTTYPost opens one of the two POST halves. The handler flushes its 200
// before it starts relaying, so Do returns while the request body is still
// open and the test can keep feeding it — exactly how the real worker and
// client stream.
func openTTYPost(t *testing.T, ts *httptest.Server, token, path string) *ttyStream {
	t.Helper()
	s := openTTYRequest(t, ts, http.MethodPost, token, path)
	require.Equal(t, http.StatusOK, s.resp.StatusCode)
	return s
}

func openTTYGet(t *testing.T, ts *httptest.Server, token, path string) *ttyStream {
	t.Helper()
	s := openTTYRequest(t, ts, http.MethodGet, token, path)
	require.Equal(t, http.StatusOK, s.resp.StatusCode)
	return s
}

func openTTYRequest(t *testing.T, ts *httptest.Server, method, token, path string) *ttyStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	var body io.Reader
	s := &ttyStream{cancel: cancel}
	if method == http.MethodPost {
		pr, pw := io.Pipe()
		body, s.w = pr, pw
	}
	req, err := http.NewRequestWithContext(ctx, method, ts.URL+path, body)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	s.resp, s.r = resp, resp.Body
	t.Cleanup(s.abort)
	return s
}

// readTTY reads exactly n bytes, failing rather than hanging.
func readTTY(t *testing.T, r io.Reader, n int) string {
	t.Helper()
	buf := make([]byte, n)
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(r, buf)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err, "reading %d bytes off a tty stream", n)
	case <-time.After(ttyDeadline):
		t.Fatalf("timed out waiting for %d bytes; got %q", n, buf)
	}
	return string(buf)
}

// drainTTY reads to EOF, failing rather than hanging. Used where the point of
// the test is that the stream *ends*.
func drainTTY(t *testing.T, r io.Reader) string {
	t.Helper()
	type result struct {
		b   []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		b, err := io.ReadAll(r)
		done <- result{b, err}
	}()
	select {
	case got := <-done:
		require.NoError(t, got.err)
		return string(got.b)
	case <-time.After(ttyDeadline):
		t.Fatal("stream never ended")
	}
	return ""
}

// requireStreamEnds asserts a stream finishes, without insisting on how: a
// half the relay has finished with may end as a clean EOF or as a dropped
// connection, and both mean the handler let go.
func requireStreamEnds(t *testing.T, r io.Reader, msg string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, r)
	}()
	select {
	case <-done:
	case <-time.After(ttyDeadline):
		t.Fatal(msg)
	}
}

func writeTTY(t *testing.T, w io.Writer, s string) {
	t.Helper()
	_, err := io.WriteString(w, s)
	require.NoError(t, err)
}

// ttySession opens all four halves of one job's terminal.
type ttySession struct {
	clientOut, clientIn, workerOut, workerIn *ttyStream
}

func openTTYSession(t *testing.T, ts *httptest.Server, jobID string) *ttySession {
	t.Helper()
	return &ttySession{
		clientOut: openTTYGet(t, ts, "ctok", "/v1/jobs/"+jobID+"/tty/out"),
		workerOut: openTTYPost(t, ts, "wtok", "/v1/jobs/"+jobID+"/tty/out"),
		clientIn:  openTTYPost(t, ts, "ctok", "/v1/jobs/"+jobID+"/tty/in"),
		workerIn:  openTTYGet(t, ts, "wtok", "/v1/jobs/"+jobID+"/tty/in"),
	}
}

func (s *ttySession) abort() {
	s.clientOut.abort()
	s.workerOut.abort()
	s.clientIn.abort()
	s.workerIn.abort()
}

// A terminal is only a terminal if bytes travel both ways: what the PTY
// prints reaches the operator, and what the operator types reaches the PTY.
// The two directions are separate code paths — raw bytes one way, framed
// bytes the other — so both are asserted.
func TestTTYRelaysBytesInBothDirections(t *testing.T) {
	ts, _, newJob := newTTYServer(t)
	s := openTTYSession(t, ts, newJob())

	const banner = "root@gpubox:~# "
	writeTTY(t, s.workerOut.w, banner)
	require.Equal(t, banner, readTTY(t, s.clientOut.r, len(banner)))

	var keys bytes.Buffer
	require.NoError(t, server.WriteTTYFrame(&keys, server.TTYData([]byte("nvidia-smi\r"))))
	writeTTY(t, s.clientIn.w, keys.String())
	require.Equal(t, keys.String(), readTTY(t, s.workerIn.r, keys.Len()))
}

// Sessions are keyed by job. Keyed by worker — the plausible wrong choice,
// since it is the worker that dials in — one operator's keystrokes would land
// in another operator's shell on the same box, and their output in the wrong
// terminal.
func TestTTYSessionsAreIsolatedPerJob(t *testing.T) {
	ts, _, newJob := newTTYServer(t)

	a := openTTYSession(t, ts, newJob())
	b := openTTYSession(t, ts, newJob())

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); writeTTY(t, a.workerOut.w, "AAAAAAAA") }()
	go func() { defer wg.Done(); writeTTY(t, b.workerOut.w, "BBBBBBBB") }()
	wg.Wait()

	require.Equal(t, "AAAAAAAA", readTTY(t, a.clientOut.r, 8))
	require.Equal(t, "BBBBBBBB", readTTY(t, b.clientOut.r, 8))

	wg.Add(2)
	go func() { defer wg.Done(); writeTTY(t, a.clientIn.w, "aaaaaaaa") }()
	go func() { defer wg.Done(); writeTTY(t, b.clientIn.w, "bbbbbbbb") }()
	wg.Wait()

	require.Equal(t, "aaaaaaaa", readTTY(t, a.workerIn.r, 8))
	require.Equal(t, "bbbbbbbb", readTTY(t, b.workerIn.r, 8))
}

// The operator's terminal and the worker's dial-out genuinely race: the
// client attaches as soon as it has submitted, the worker attaches when the
// scheduler hands it the job. Neither order may drop a byte — in particular
// the half that arrives first must be made to wait, not answered into a void.
func TestTTYEitherHalfMayConnectFirst(t *testing.T) {
	t.Run("client first", func(t *testing.T) {
		ts, _, newJob := newTTYServer(t)
		job := newJob()

		clientOut := openTTYGet(t, ts, "ctok", "/v1/jobs/"+job+"/tty/out")
		clientIn := openTTYPost(t, ts, "ctok", "/v1/jobs/"+job+"/tty/in")
		// Typed before the box is even listening: the bytes must be held,
		// not dropped.
		writeTTY(t, clientIn.w, "typed-ahead")

		workerOut := openTTYPost(t, ts, "wtok", "/v1/jobs/"+job+"/tty/out")
		workerIn := openTTYGet(t, ts, "wtok", "/v1/jobs/"+job+"/tty/in")

		require.Equal(t, "typed-ahead", readTTY(t, workerIn.r, len("typed-ahead")))
		writeTTY(t, workerOut.w, "late-banner")
		require.Equal(t, "late-banner", readTTY(t, clientOut.r, len("late-banner")))
	})

	t.Run("worker first", func(t *testing.T) {
		ts, _, newJob := newTTYServer(t)
		job := newJob()

		workerOut := openTTYPost(t, ts, "wtok", "/v1/jobs/"+job+"/tty/out")
		workerIn := openTTYGet(t, ts, "wtok", "/v1/jobs/"+job+"/tty/in")
		// Printed before anyone is watching. A relay that answered the
		// worker into a void here would eat the first screen of output,
		// which is exactly the screen with the prompt on it.
		writeTTY(t, workerOut.w, "early-banner")

		clientOut := openTTYGet(t, ts, "ctok", "/v1/jobs/"+job+"/tty/out")
		clientIn := openTTYPost(t, ts, "ctok", "/v1/jobs/"+job+"/tty/in")

		require.Equal(t, "early-banner", readTTY(t, clientOut.r, len("early-banner")))
		writeTTY(t, clientIn.w, "late-keys")
		require.Equal(t, "late-keys", readTTY(t, workerIn.r, len("late-keys")))
	})
}

// A killed job must not leave an operator sitting in a terminal that will
// never say anything again: when the worker's output half ends, the client's
// ends too.
func TestTTYWorkerHalfClosingEndsTheClientStream(t *testing.T) {
	ts, _, newJob := newTTYServer(t)
	s := openTTYSession(t, ts, newJob())

	writeTTY(t, s.workerOut.w, "goodbye")
	require.NoError(t, s.workerOut.w.Close())

	// Whatever was in flight arrives, and then the stream ends rather than
	// hanging open.
	require.Equal(t, "goodbye", drainTTY(t, s.clientOut.r))

	// The whole session goes with it, not just the direction that ended. The
	// worker's keystroke half ends on its own; the operator's ends the next
	// time it touches the relay, rather than swallowing keys into a session
	// with nothing on the far end.
	requireStreamEnds(t, s.workerIn.r, "the worker's input half outlived the session")
	_, _ = io.WriteString(s.clientIn.w, "x")
	requireStreamEnds(t, s.clientIn.r, "the operator's input half outlived the session")
}

// A dropped keystroke connection is indistinguishable from a deliberate stdin
// EOF, and both close the input pipe. If that pipe were never rebuilt, one
// blip would leave the operator with a terminal that prints perfectly and
// silently swallows every key for the rest of the job — no error, no way to
// tell. Reconnecting must restore typing.
func TestTTYInputHalfWorksAgainAfterAReconnect(t *testing.T) {
	ts, _, newJob := newTTYServer(t)
	job := newJob()
	s := openTTYSession(t, ts, job)

	writeTTY(t, s.clientIn.w, "before")
	require.Equal(t, "before", readTTY(t, s.workerIn.r, len("before")))

	// The operator's keystroke stream drops. The worker's half sees the end
	// of input, as it should.
	s.clientIn.abort()
	requireStreamEnds(t, s.workerIn.r, "the worker's input half did not see the input end")

	// Both ends dial their input stream again.
	clientIn := openTTYPost(t, ts, "ctok", "/v1/jobs/"+job+"/tty/in")
	workerIn := openTTYGet(t, ts, "wtok", "/v1/jobs/"+job+"/tty/in")
	writeTTY(t, clientIn.w, "after")
	require.Equal(t, "after", readTTY(t, workerIn.r, len("after")))

	// And the output direction was never disturbed by any of it.
	writeTTY(t, s.workerOut.w, "still printing")
	require.Equal(t, "still printing", readTTY(t, s.clientOut.r, len("still printing")))
}

// The worker halves are worker routes. A client token that could open them
// would let any submitter write into somebody else's terminal — forged
// output on the operator's screen — and read their keystrokes, passwords
// included.
func TestTTYRolesAreEnforced(t *testing.T) {
	ts, _, newJob := newTTYServer(t)
	job := newJob()

	for _, tc := range []struct {
		name, method, path, token string
		want                      int
	}{
		{"client token cannot write output", http.MethodPost, "/tty/out", "ctok", http.StatusForbidden},
		{"client token cannot read keystrokes", http.MethodGet, "/tty/in", "ctok", http.StatusForbidden},
		{"worker token cannot watch a terminal", http.MethodGet, "/tty/out", "wtok", http.StatusForbidden},
		{"worker token cannot type", http.MethodPost, "/tty/in", "wtok", http.StatusForbidden},
		{"an unknown token gets nothing", http.MethodGet, "/tty/out", "nope", http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, ts.URL+"/v1/jobs/"+job+tc.path, strings.NewReader(""))
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+tc.token)
			resp, err := ts.Client().Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, tc.want, resp.StatusCode)
		})
	}

	// The refusal has to be real, not just a status code: nothing the client
	// token sent may show up on the terminal.
	t.Run("the rejected write reaches nobody", func(t *testing.T) {
		other := newJob()
		clientOut := openTTYGet(t, ts, "ctok", "/v1/jobs/"+other+"/tty/out")

		req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/jobs/"+other+"/tty/out",
			strings.NewReader("INJECTED"))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer ctok")
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusForbidden, resp.StatusCode)

		workerOut := openTTYPost(t, ts, "wtok", "/v1/jobs/"+other+"/tty/out")
		writeTTY(t, workerOut.w, "REAL")
		require.Equal(t, "REAL", readTTY(t, clientOut.r, 4))
	})
}

// A session is opened against a job that exists, checked once at connect —
// the same guard handleStreamLogs has, and for the same reason: without it
// any token can pin a goroutine, an fd and a registry entry per request
// against an ID that was never allocated.
func TestTTYSessionForAnUnknownJobIsRefused(t *testing.T) {
	ts, _, _ := newTTYServer(t)

	for _, tc := range []struct{ method, path, token string }{
		{http.MethodGet, "/tty/out", "ctok"},
		{http.MethodPost, "/tty/in", "ctok"},
		{http.MethodPost, "/tty/out", "wtok"},
		{http.MethodGet, "/tty/in", "wtok"},
	} {
		req, err := http.NewRequest(tc.method, ts.URL+"/v1/jobs/no-such-job"+tc.path, strings.NewReader(""))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+tc.token)
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusNotFound, resp.StatusCode, "%s %s", tc.method, tc.path)
	}
}

// Nothing on the relay path may touch the store. SQLite runs at
// MaxOpenConns(1) and the scheduler holds that connection while it allocates;
// a keystroke that had to queue behind it would make typing stutter every
// time somebody submitted a job. Proven by taking the database away from a
// session that is already up — the terminal must not notice.
func TestTTYRelayNeverTouchesTheStore(t *testing.T) {
	ts, st, newJob := newTTYServer(t)
	s := openTTYSession(t, ts, newJob())

	require.NoError(t, st.Close())

	// First prove the database really is gone, so the rest of this test
	// cannot pass by accident on a store that still works.
	dead := get(t, ts, "ctok", "/v1/jobs/whatever")
	defer dead.Body.Close()
	require.Equal(t, http.StatusInternalServerError, dead.StatusCode,
		"the store still answers, so this test would prove nothing")

	writeTTY(t, s.workerOut.w, "output with no database")
	require.Equal(t, "output with no database", readTTY(t, s.clientOut.r, len("output with no database")))
	writeTTY(t, s.clientIn.w, "keys with no database")
	require.Equal(t, "keys with no database", readTTY(t, s.workerIn.r, len("keys with no database")))
}

// Two connections on one half would split the byte stream between them and
// hand each a corrupt terminal, so the second is refused rather than
// silently interleaved.
func TestTTYSecondConnectionOnAHalfIsRefused(t *testing.T) {
	ts, _, newJob := newTTYServer(t)
	job := newJob()
	first := openTTYGet(t, ts, "ctok", "/v1/jobs/"+job+"/tty/out")

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/jobs/"+job+"/tty/out", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode)

	// The intruder must not have disturbed the session that was already up.
	workerOut := openTTYPost(t, ts, "wtok", "/v1/jobs/"+job+"/tty/out")
	writeTTY(t, workerOut.w, "still here")
	require.Equal(t, "still here", readTTY(t, first.r, len("still here")))
}

// Four streams per session means four handler goroutines, four watchers and
// two connections a side. A leak here is invisible until the controller has
// been up for a week, so the cycles below include the ugly cases: a client
// that vanishes mid-stream while the worker is still writing, and halves left
// attached to a session nothing will ever arrive on.
func TestTTYStreamsDoNotLeakGoroutines(t *testing.T) {
	ts, _, newJob := newTTYServer(t)

	cycle := func() {
		s := openTTYSession(t, ts, newJob())
		writeTTY(t, s.workerOut.w, "hello")
		require.Equal(t, "hello", readTTY(t, s.clientOut.r, 5))
		writeTTY(t, s.clientIn.w, "keys!")
		require.Equal(t, "keys!", readTTY(t, s.workerIn.r, 5))
		s.abort()
	}

	// A client that dies mid-stream while the worker is still printing into
	// the terminal that just disappeared.
	abruptCycle := func() {
		s := openTTYSession(t, ts, newJob())
		writeTTY(t, s.workerOut.w, "hello")
		require.Equal(t, "hello", readTTY(t, s.clientOut.r, 5))
		s.clientOut.abort()
		// Writes fail once the relay tears the session down, which is the
		// point; until then they are absorbed.
		for range 64 {
			if _, err := io.WriteString(s.workerOut.w, strings.Repeat("x", 1024)); err != nil {
				break
			}
		}
		s.abort()
	}

	// An operator who gives up before the job ever starts: two halves
	// attached, both idle, both dropped. Nothing will ever arrive on them to
	// fail on, so these only unwind if the relay watches the request context.
	lonelyCycle := func() {
		job, otherJob := newJob(), newJob()
		clientOut := openTTYGet(t, ts, "ctok", "/v1/jobs/"+job+"/tty/out")
		clientIn := openTTYPost(t, ts, "ctok", "/v1/jobs/"+job+"/tty/in")
		workerIn := openTTYGet(t, ts, "wtok", "/v1/jobs/"+otherJob+"/tty/in")
		clientOut.abort()
		clientIn.abort()
		workerIn.abort()
	}

	// Warm up first: the first request builds transports, connections and
	// their goroutines, and counting those as a leak would make this test
	// noise.
	cycle()
	abruptCycle()
	lonelyCycle()
	ts.Client().CloseIdleConnections()
	settleGoroutines(t)
	before := runtime.NumGoroutine()

	const cycles = 25
	for range cycles {
		cycle()
		abruptCycle()
		lonelyCycle()
	}

	ts.Client().CloseIdleConnections()
	settleGoroutines(t)
	after := runtime.NumGoroutine()
	t.Logf("goroutines: %d before, %d after %d cycles", before, after, 3*cycles)

	// 75 cycles hold 250 stream handlers and as many watchers, so anything
	// that fails to return is hundreds of goroutines, nowhere near this
	// tolerance.
	require.LessOrEqualf(t, after, before+8,
		"goroutines grew from %d to %d across %d connect/disconnect cycles", before, after, 3*cycles)
}

// settleGoroutines waits for the goroutines a finished request leaves briefly
// behind — the server's connection reader, the transport's writer — rather
// than sampling the count at an arbitrary instant.
func settleGoroutines(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(ttyDeadline)
	last := runtime.NumGoroutine()
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n >= last {
			return
		}
		last = n
	}
}

// The frame format the two ends agree on. The controller never decodes it —
// the relay is byte-opaque — but both ends encode against this, so the wire
// form is pinned here rather than reinvented twice.
func TestTTYFrameWireFormat(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, server.WriteTTYFrame(&buf, server.TTYData([]byte("ls"))))
	require.Equal(t, `{"t":"d","b":"bHM="}`+"\n", buf.String())

	buf.Reset()
	require.NoError(t, server.WriteTTYFrame(&buf, server.TTYResize(48, 180)))
	require.Equal(t, `{"t":"r","rows":48,"cols":180}`+"\n", buf.String())
}

func TestTTYFramesRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	// Ctrl-C, a newline and a byte that is not valid UTF-8: a terminal
	// carries arbitrary bytes, and base64 is why newline-delimited framing
	// can carry them at all.
	payload := []byte{0x03, '\n', 0xff, 'a'}
	require.NoError(t, server.WriteTTYFrame(&buf, server.TTYData(payload)))
	require.NoError(t, server.WriteTTYFrame(&buf, server.TTYResize(24, 80)))
	require.Equal(t, 2, bytes.Count(buf.Bytes(), []byte("\n")),
		"a frame must not contain a raw newline, or the framing splits it in half")

	r := server.NewTTYFrameReader(&buf)

	first, err := r.Next()
	require.NoError(t, err)
	require.Equal(t, server.TTYFrameData, first.T)
	require.Equal(t, payload, first.B)

	second, err := r.Next()
	require.NoError(t, err)
	require.Equal(t, server.TTYFrameResize, second.T)
	require.Equal(t, uint16(24), second.Rows)
	require.Equal(t, uint16(80), second.Cols)

	_, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
}

// The relay copies bytes and respects no message boundary, so a frame will
// arrive split across reads. A reader that assumed one read per frame would
// work in tests and corrupt every large paste in production.
func TestTTYFrameReaderReassemblesSplitFrames(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, server.WriteTTYFrame(&buf, server.TTYData([]byte("a-long-line-of-keystrokes"))))
	whole := buf.Bytes()

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		_, _ = pw.Write(whole[:7])
		_, _ = pw.Write(whole[7:])
	}()

	f, err := server.NewTTYFrameReader(pr).Next()
	require.NoError(t, err)
	require.Equal(t, []byte("a-long-line-of-keystrokes"), f.B)
}

// An unknown frame type is skipped, not fatal: the format is meant to grow,
// and a worker built before a frame type existed must not kill somebody's
// terminal over it. Malformed JSON is different — the framing itself is
// lost — and must be reported.
func TestTTYFrameReaderSkipsUnknownTypesAndRejectsGarbage(t *testing.T) {
	stream := strings.NewReader(
		`{"t":"x","future":true}` + "\n" +
			`{"t":"d","b":"aGk="}` + "\n")
	f, err := server.NewTTYFrameReader(stream).Next()
	require.NoError(t, err)
	require.Equal(t, []byte("hi"), f.B)

	_, err = server.NewTTYFrameReader(strings.NewReader("not json at all\n")).Next()
	require.Error(t, err)
	var syntax *json.SyntaxError
	require.ErrorAs(t, err, &syntax)
}
