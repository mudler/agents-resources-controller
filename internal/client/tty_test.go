package client_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

// relayFake is the controller's two client-side relay routes and nothing
// else. It hands the test the far end of each: what the client sent, and a
// writer standing in for whatever the box is printing.
type relayFake struct {
	ts *httptest.Server

	// sent is everything the client POSTed on the input half, still framed.
	sent *io.PipeReader
	// print writes to the client's output half, as the box would.
	print *io.PipeWriter

	release func()
}

func newRelayFake(t *testing.T) *relayFake {
	t.Helper()
	sentR, sentW := io.Pipe()
	printR, printW := io.Pipe()
	f := &relayFake{
		sent:    sentR,
		print:   printW,
		release: func() { sentR.Close(); sentW.Close(); printR.Close(); printW.Close() },
	}

	f.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		switch {
		case r.URL.Path == "/v1/jobs/j1/tty/out" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusOK)
			rc.Flush()
			buf := make([]byte, 4096)
			for {
				n, err := printR.Read(buf)
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

		case r.URL.Path == "/v1/jobs/j1/tty/in" && r.Method == http.MethodPost:
			// The relay's own EnableFullDuplex, for the same reason: this
			// body only ends when the terminal does, and net/http otherwise
			// refuses to write a response header until it has drained one.
			_ = rc.EnableFullDuplex()
			w.WriteHeader(http.StatusOK)
			rc.Flush()
			_, _ = io.Copy(sentW, r.Body)
			sentW.Close()

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(func() { f.release(); f.ts.Close() })
	return f
}

// nextFrame reads one frame off what the client sent, failing rather than
// hanging.
func nextFrame(t *testing.T, r *server.TTYFrameReader) server.TTYFrame {
	t.Helper()
	type res struct {
		f   server.TTYFrame
		err error
	}
	out := make(chan res, 1)
	go func() {
		f, err := r.Next()
		out <- res{f, err}
	}()
	select {
	case got := <-out:
		require.NoError(t, got.err)
		return got.f
	case <-time.After(5 * time.Second):
		t.Fatal("the client never sent another frame")
		return server.TTYFrame{}
	}
}

// A terminal has to announce its size before anything else, and then carry
// keystrokes out and output back. Raw mode and SIGWINCH need a real tty and
// belong to the caller; everything downstream of them is here.
func TestAttachTTYSendsASizeThenCarriesBothDirections(t *testing.T) {
	f := newRelayFake(t)
	c := client.New(f.ts.URL, "ctok")

	keysR, keysW := io.Pipe()
	t.Cleanup(func() { keysR.Close(); keysW.Close() })
	var screen syncBuffer

	done := make(chan error, 1)
	go func() {
		done <- c.AttachTTY(context.Background(), "j1", client.Terminal{
			In:   keysR,
			Out:  &screen,
			Size: func() (uint16, uint16, error) { return 48, 180, nil },
		})
	}()

	frames := server.NewTTYFrameReader(f.sent)

	// The size first. A session that never sends one leaves the far side at
	// the PTY's 24x80 default, which is why remote shells wrap forever.
	first := nextFrame(t, frames)
	require.Equal(t, server.TTYFrameResize, first.T)
	require.Equal(t, uint16(48), first.Rows)
	require.Equal(t, uint16(180), first.Cols)

	// Anything typed becomes a data frame, base64 and all, so a terminal's
	// arbitrary bytes survive newline-delimited framing.
	_, err := keysW.Write([]byte{'l', 's', 0x03, '\n'})
	require.NoError(t, err)
	second := nextFrame(t, frames)
	require.Equal(t, server.TTYFrameData, second.T)
	require.Equal(t, []byte{'l', 's', 0x03, '\n'}, second.B)

	// And what the box prints reaches the operator's screen unaltered.
	_, err = io.WriteString(f.print, "root@gpubox:~# ")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return strings.Contains(screen.String(), "root@gpubox:~# ") },
		5*time.Second, 20*time.Millisecond, "the box's output never reached the terminal")

	// The job's terminal ends; so does the attachment. An operator whose
	// shell exited must get their own prompt back rather than being left in
	// a session with nothing on the far end.
	require.NoError(t, f.print.Close())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("AttachTTY did not return when the job's output stream ended")
	}
}

// A resize while the session is live has to reach the far side, or a window
// the operator dragged wider stays 80 columns for the rest of the job.
func TestAttachTTYForwardsALaterResize(t *testing.T) {
	f := newRelayFake(t)
	c := client.New(f.ts.URL, "ctok")

	keysR, keysW := io.Pipe()
	t.Cleanup(func() { keysR.Close(); keysW.Close() })
	resized := make(chan struct{}, 1)

	var rows, cols uint16 = 24, 80
	var mu sync.Mutex
	size := func() (uint16, uint16, error) {
		mu.Lock()
		defer mu.Unlock()
		return rows, cols, nil
	}

	go func() {
		_ = c.AttachTTY(context.Background(), "j1", client.Terminal{
			In: keysR, Out: io.Discard, Size: size, Resized: resized,
		})
	}()

	frames := server.NewTTYFrameReader(f.sent)
	require.Equal(t, uint16(80), nextFrame(t, frames).Cols)

	mu.Lock()
	rows, cols = 60, 200
	mu.Unlock()
	resized <- struct{}{}

	next := nextFrame(t, frames)
	require.Equal(t, server.TTYFrameResize, next.T)
	require.Equal(t, uint16(60), next.Rows)
	require.Equal(t, uint16(200), next.Cols)
}

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
