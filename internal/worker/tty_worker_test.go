package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

// relayHarness is a controller that serves only what an attached job needs:
// registration, one assignment, heartbeats, status, logs, and the two
// worker-side halves of the relay. It hands the test the OTHER end of each
// half, so the assertions are "the box's terminal reached the operator" and
// "what the operator typed reached the box" rather than "some bytes were
// written to a socket".
type relayHarness struct {
	ts *httptest.Server

	// fromBox carries everything the worker POSTs up as the job's output.
	fromBox *io.PipeReader
	// toBox is what the test sends down as the job's input.
	toBox *io.PipeWriter

	// release unblocks both relay handlers at teardown.
	release func()

	mu      sync.Mutex
	logs    []byte
	states  []string
	reasons []string

	// ttyRequests counts every request that touched a relay route, which is
	// how the non-attached case proves it never dialled one.
	ttyRequests atomic.Int64
}

func newRelayHarness(t *testing.T, a map[string]any) *relayHarness {
	t.Helper()
	h := &relayHarness{}

	boxOutR, boxOutW := io.Pipe()
	boxInR, boxInW := io.Pipe()
	h.fromBox, h.toBox = boxOutR, boxInW
	// Both ends of both pipes, closed by run's cleanup below — which must
	// happen BEFORE httptest.Server.Close, since Close waits for every
	// handler to return and these two are parked on exactly these pipes.
	h.release = func() { boxOutR.Close(); boxOutW.Close(); boxInR.Close(); boxInW.Close() }

	var served atomic.Bool
	h.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/workers/register":
			_ = json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

		case r.URL.Path == "/v1/workers/w1/assignments":
			if served.Swap(true) {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{a}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs":
			b, _ := io.ReadAll(r.Body)
			h.mu.Lock()
			h.logs = append(h.logs, b...)
			h.mu.Unlock()
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body struct {
				State  string `json:"state"`
				Reason string `json:"reason"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			h.mu.Lock()
			h.states = append(h.states, body.State)
			h.reasons = append(h.reasons, body.Reason)
			h.mu.Unlock()
			w.WriteHeader(http.StatusOK)

		// The two worker-side halves of the relay, spliced straight to the
		// test's own pipes. This is deliberately not the real registry: the
		// point here is what the WORKER does, and a fake far end makes a
		// dropped byte this package's fault rather than the server's.
		case r.URL.Path == "/v1/jobs/job1/tty/out" && r.Method == http.MethodPost:
			h.ttyRequests.Add(1)
			// The same thing the real relay does, and for the same reason:
			// without it net/http drains an unread request body before it
			// will write a response header, and this body only ends when
			// the terminal does.
			_ = http.NewResponseController(w).EnableFullDuplex()
			w.WriteHeader(http.StatusOK)
			http.NewResponseController(w).Flush()
			_, _ = io.Copy(boxOutW, r.Body)
			boxOutW.Close()

		case r.URL.Path == "/v1/jobs/job1/tty/in" && r.Method == http.MethodGet:
			h.ttyRequests.Add(1)
			w.WriteHeader(http.StatusOK)
			rc := http.NewResponseController(w)
			rc.Flush()
			buf := make([]byte, 4096)
			for {
				n, err := boxInR.Read(buf)
				if n > 0 {
					if _, werr := w.Write(buf[:n]); werr != nil {
						return
					}
					rc.Flush()
				}
				if err != nil {
					return
				}
			}

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(h.ts.Close)
	return h
}

func (h *relayHarness) run(t *testing.T, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)

	wk := worker.New(worker.Config{
		ControllerURL:     h.ts.URL,
		Token:             "wtok",
		Host:              "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: 50 * time.Millisecond,
		PollWait:          100 * time.Millisecond,
	})
	done := make(chan error, 1)
	go func() { done <- wk.Start(ctx) }()
	t.Cleanup(func() {
		h.release()
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop")
		}
	})
}

func (h *relayHarness) state(i int) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if i >= len(h.states) {
		return ""
	}
	return h.states[i]
}

func (h *relayHarness) log() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return string(h.logs)
}

// send pushes one frame down to the box, failing rather than hanging. The
// bound is not decoration: the pipe backing toBox is unbuffered, so a worker
// that never opens its input stream would leave the test parked in a Write
// forever and the failure would arrive as a package-wide timeout with no
// message on it.
func (h *relayHarness) send(t *testing.T, f server.TTYFrame) {
	t.Helper()
	h.sendRaw(t, func() error { return server.WriteTTYFrame(h.toBox, f) })
}

func (h *relayHarness) sendRaw(t *testing.T, write func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- write() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("nothing ever took what we sent: the worker never opened its input stream")
	}
}

// readUntil reads from r until want appears, failing rather than hanging.
func readUntil(t *testing.T, r io.Reader, want string) string {
	t.Helper()
	type res struct {
		s   string
		err error
	}
	out := make(chan res, 1)
	go func() {
		var got strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				got.Write(buf[:n])
				if strings.Contains(got.String(), want) {
					out <- res{got.String(), nil}
					return
				}
			}
			if err != nil {
				out <- res{got.String(), err}
				return
			}
		}
	}()
	select {
	case got := <-out:
		require.NoErrorf(t, got.err, "stream ended before %q appeared; got: %q", want, got.s)
		return got.s
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for %q", want)
		return ""
	}
}

// The interactive loop, through the worker: the box's terminal output is
// POSTed up, what the operator types is GOT down and reaches the process, and
// a resize frame actually changes the window the process sees. All three
// through the framing both ends agree on (server.TTYFrame).
func TestWorkerRunsAnInteractiveJobThroughTheRelay(t *testing.T) {
	h := newRelayHarness(t, map[string]any{
		"job_id":    "job1",
		"device_id": "gpubox:gpu0",
		"command":   []string{"/bin/sh"},
		"stdio":     model.StdioTTY,
	})
	h.run(t, 60*time.Second)

	h.send(t, server.TTYData([]byte("echo hello-from-the-relay\n")))
	readUntil(t, h.fromBox, "hello-from-the-relay")

	// A shell asks the terminal how big it is; getting this wrong is why
	// remote shells wrap at 80 columns forever.
	h.send(t, server.TTYResize(48, 180))
	h.send(t, server.TTYData([]byte("stty size\n")))
	readUntil(t, h.fromBox, "48 180")

	// The operator types `exit`; the job ends like any other job.
	h.send(t, server.TTYData([]byte("exit\n")))
	require.Eventually(t, func() bool { return h.state(1) != "" },
		20*time.Second, 50*time.Millisecond, "the interactive job never reported a terminal state")
	require.Equal(t, string(model.JobRunning), h.state(0))
	require.Equal(t, string(model.JobSucceeded), h.state(1))
}

// A terminal's bytes must not be copied into the log store as well. That is
// not tidiness: the same relay carries `rc cp`'s tar stream, and a controller
// that wrote those bytes to its own disk would be the upload service this
// design exists to avoid — on a scheduler whose database is one connection
// wide.
func TestAnAttachedJobsOutputNeverReachesTheLogStore(t *testing.T) {
	h := newRelayHarness(t, map[string]any{
		"job_id":    "job1",
		"device_id": "gpubox:gpu0",
		"command":   []string{"/bin/sh", "-c", "echo SECRET-PAYLOAD; sleep 0.2"},
		"stdio":     model.StdioTTY,
	})
	h.run(t, 60*time.Second)

	readUntil(t, h.fromBox, "SECRET-PAYLOAD")
	require.Eventually(t, func() bool { return h.state(1) != "" },
		20*time.Second, 50*time.Millisecond, "the job never finished")
	require.NotContains(t, h.log(), "SECRET-PAYLOAD",
		"the terminal's bytes were also written to the log store")
}

// Pipe mode is the same relay with no terminal in the way, and it has to be
// byte-exact: it is what carries a tar stream. A PTY here would rewrite every
// \n and swallow every ^D, which is the whole reason this is a second mode
// rather than a reuse of the first.
func TestWorkerStreamsRawStdinAndStdoutInPipeMode(t *testing.T) {
	h := newRelayHarness(t, map[string]any{
		"job_id":    "job1",
		"device_id": "gpubox:gpu0",
		"command":   []string{"/bin/cat"},
		"stdio":     model.StdioPipe,
	})
	h.run(t, 60*time.Second)

	// A lone \n, a \r and a 0xff: through a terminal the first would come
	// back as \r\n and the last would not survive at all.
	payload := "line-one\nline\rtwo\xffend\n"
	h.sendRaw(t, func() error { _, err := io.WriteString(h.toBox, payload); return err })

	got := readUntil(t, h.fromBox, "end")
	require.Equal(t, payload, got, "pipe mode altered the bytes")

	// Closing the input is an ordinary stdin EOF: cat exits 0.
	require.NoError(t, h.toBox.Close())
	require.Eventually(t, func() bool { return h.state(1) != "" },
		20*time.Second, 50*time.Millisecond, "cat never saw EOF on its stdin")
	require.Equal(t, string(model.JobSucceeded), h.state(1))
}

// The regression guard the plan asks for: every job that exists today must be
// untouched by any of this. An ordinary assignment carries no stdio mode, so
// its output goes to the log store exactly as before and the worker never
// dials the relay at all.
func TestAnOrdinaryJobIsUnaffectedByTheRelay(t *testing.T) {
	h := newRelayHarness(t, map[string]any{
		"job_id":    "job1",
		"device_id": "gpubox:gpu0",
		"command":   []string{"/bin/sh", "-c", "echo ordinary-output"},
	})
	h.run(t, 60*time.Second)

	require.Eventually(t, func() bool { return strings.Contains(h.log(), "ordinary-output") },
		20*time.Second, 50*time.Millisecond, "an ordinary job's output stopped reaching the log store")
	require.Equal(t, string(model.JobSucceeded), h.state(1))
	require.Zero(t, h.ttyRequests.Load(),
		"an ordinary job opened a relay stream; every existing job would now depend on a controller half that nobody attaches")
}
