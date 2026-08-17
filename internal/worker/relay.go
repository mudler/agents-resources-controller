package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
)

// This is the worker's end of the controller's interactive relay
// (internal/server/tty.go). A job whose assignment carries an attached stdio
// mode has its standard streams spliced onto two chunked-HTTP connections
// this worker OPENS — the controller never dials a box, which is what makes a
// NAT'd host work with no inbound port and no credentials held anywhere but
// here.
//
// Everything after the process exits is unchanged: the terminal report, the
// verify probes, the release linger, the hooks. An attached job is an
// ordinary job with different stdio, and execute treats it as one.

// ttyDrainGrace bounds how long the output copy is given to finish after the
// process has been reaped, so the last screen the process printed — the error
// message it died complaining about, most of the time — still reaches the
// operator. It is a grace, not a wait: the copy is unblocked unconditionally
// afterwards by closing the terminal.
const ttyDrainGrace = 2 * time.Second

// relayStreams is this worker's two halves of one job's session. Both are
// opened by us.
type relayStreams struct {
	// out is the job's output on its way to whoever is watching. Closing it
	// ends the operator's stream, which is how a finished job stops a
	// terminal hanging open forever.
	out io.WriteCloser
	// in is what the operator sends: raw bytes in pipe mode, newline-
	// delimited frames in TTY mode (see server.TTYFrame).
	in io.ReadCloser

	closeOnce func()
}

func (s *relayStreams) close() { s.closeOnce() }

// dialRelay opens both of this worker's halves for jobID.
//
// The out half is a POST whose body we keep writing for the life of the job,
// so the request is built around an io.Pipe: http.Client.Do returns as soon
// as the controller flushes its 200, and everything written to the returned
// writer streams up from there. Go's transport flushes each chunk of a
// chunked request body as it is written (internal.FlushAfterChunkWriter),
// which is what keeps an interactive terminal interactive rather than
// arriving 4KB at a time.
func (w *Worker) dialRelay(ctx context.Context, jobID string) (*relayStreams, error) {
	base := w.cfg.ControllerURL + "/v1/jobs/" + jobID + "/tty/"

	pr, pw := io.Pipe()
	outReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"out", pr)
	if err != nil {
		return nil, err
	}
	outReq.Header.Set("Authorization", "Bearer "+w.cfg.Token)
	outReq.Header.Set("Content-Type", "application/octet-stream")
	outResp, err := w.stream.Do(outReq)
	if err != nil {
		_ = pw.Close()
		return nil, fmt.Errorf("open output stream: %w", err)
	}
	if outResp.StatusCode != http.StatusOK {
		outResp.Body.Close()
		_ = pw.Close()
		return nil, fmt.Errorf("open output stream: controller returned %s", outResp.Status)
	}

	inReq, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"in", nil)
	if err != nil {
		outResp.Body.Close()
		_ = pw.Close()
		return nil, err
	}
	inReq.Header.Set("Authorization", "Bearer "+w.cfg.Token)
	inResp, err := w.stream.Do(inReq)
	if err != nil {
		outResp.Body.Close()
		_ = pw.Close()
		return nil, fmt.Errorf("open input stream: %w", err)
	}
	if inResp.StatusCode != http.StatusOK {
		inResp.Body.Close()
		outResp.Body.Close()
		_ = pw.Close()
		return nil, fmt.Errorf("open input stream: controller returned %s", inResp.Status)
	}

	return &relayStreams{
		out: pw,
		in:  inResp.Body,
		closeOnce: func() {
			// Closing the request body pipe is what ends the POST, and the
			// controller treats that as the end of the session. Both response
			// bodies are closed too so the connections are released rather
			// than left for the transport to reap.
			_ = pw.Close()
			outResp.Body.Close()
			inResp.Body.Close()
		},
	}, nil
}

// runAttached runs a job whose stdio is spliced onto the relay instead of
// batched into the log store, and returns the same Result Run does so
// everything downstream of it — the terminal report, verify, the release
// linger — cannot tell the difference.
//
// sink is the job's log sink. In pipe mode it carries stderr only, which is
// where a far-side `tar`'s complaints go: stdout is the payload and must stay
// byte-exact, and merging the two would corrupt every archive. In TTY mode
// nothing goes to it at all — a terminal has one stream, it is full of
// control sequences that are useless as a log, and the same relay carries
// `rc cp`'s bytes, which must never land on the controller's disk.
func (w *Worker) runAttached(ctx context.Context, a assignment, spec JobSpec, sink io.Writer) Result {
	streams, err := w.dialRelay(ctx, a.JobID)
	if err != nil {
		return Result{ExitCode: -1, Err: fmt.Errorf("attach %s stdio: %w", a.Stdio, err)}
	}
	defer streams.close()

	if a.Stdio == model.StdioTTY {
		return runTTY(ctx, spec, streams)
	}
	spec.Stdin = streams.in
	spec.Stderr = sink
	return Run(ctx, spec, streams.out)
}

// runTTY is the interactive half: a real pseudo-terminal, its output raw on
// the way up and its input decoded from frames on the way down.
func runTTY(ctx context.Context, spec JobSpec, streams *relayStreams) Result {
	s, err := startPTY(ctx, spec)
	if err != nil {
		return Result{ExitCode: -1, Err: err}
	}

	outDone := make(chan struct{})
	go func() {
		defer close(outDone)
		// Ends with EIO from the PTY master once the last slave fd closes,
		// which is exactly the process exiting. That is not an error.
		_, _ = io.Copy(streams.out, s)
	}()

	// The input direction is deliberately not waited on. A dropped keystroke
	// connection is an ordinary event — the operator's laptop slept, they
	// reconnected — and `rc run` has always kept a job running when its
	// client goes away. Ending the session here instead would make a network
	// blip kill a shell, and a shell that dies when a terminal does is the
	// property `--tty` exists to NOT have.
	go func() {
		r := server.NewTTYFrameReader(streams.in)
		for {
			f, err := r.Next()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					slog.Warn("tty input stream ended badly", "err", err)
				}
				return
			}
			switch f.T {
			case server.TTYFrameData:
				if _, err := s.Write(f.B); err != nil {
					return
				}
			case server.TTYFrameResize:
				if err := s.Resize(f.Rows, f.Cols); err != nil {
					slog.Warn("tty resize failed", "rows", f.Rows, "cols", f.Cols, "err", err)
				}
			}
		}
	}()

	res := s.Wait()

	// Give the last screen a moment to reach the operator before the terminal
	// is torn down under the copy. Bounded, then unblocked unconditionally:
	// a wedged relay must not hold the device.
	select {
	case <-outDone:
	case <-time.After(ttyDrainGrace):
	}
	_ = s.Close()
	return res
}
