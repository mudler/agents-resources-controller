package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// This is the client's end of the controller's interactive relay
// (internal/server/tty.go): the two halves an operator's terminal — or a file
// transfer — opens for one job.
//
// It is deliberately one piece of transport shared by both callers rather
// than one per feature. AttachTTY and CopyTo/CopyFrom differ only in what
// they push through these two streams; a second copy of the connect dance
// would be a second place for the "attach before the job is scheduled" rule
// to be got wrong.

// relayStreams is one job's session as this client sees it.
type relayStreams struct {
	// out carries whatever the job wrote: raw terminal bytes in TTY mode,
	// the process's stdout in pipe mode. It ends when the job's output
	// stream does, which is how a finished job ends the session.
	out io.ReadCloser
	// in carries what we send: framed keystrokes and resizes in TTY mode,
	// raw bytes (a tar stream) in pipe mode. Closing it is an ordinary stdin
	// EOF to the process, and the far end keeps running.
	in io.WriteCloser

	closeOnce func()
}

func (s *relayStreams) close() { s.closeOnce() }

// attachRelay opens both halves for jobID.
//
// Either half may be opened before the worker's — the operator's terminal and
// the worker's dial-out genuinely race, and the relay holds both orders
// without dropping a byte — so a caller should attach as soon as it has a job
// ID rather than waiting for the job to be scheduled. That is what makes
// type-ahead during a queue wait work.
func (c *Client) attachRelay(ctx context.Context, jobID string) (*relayStreams, error) {
	base := c.baseURL + "/v1/jobs/" + jobID + "/tty/"

	outReq, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"out", nil)
	if err != nil {
		return nil, err
	}
	outReq.Header.Set("Authorization", "Bearer "+c.token)
	outResp, err := c.http.Do(outReq)
	if err != nil {
		return nil, fmt.Errorf("attach output stream: %w", err)
	}
	if outResp.StatusCode != http.StatusOK {
		defer outResp.Body.Close()
		return nil, fmt.Errorf("attach output stream: %w", apiError(outResp))
	}

	// The input half is a POST whose body we keep writing for the life of
	// the session, so it is built around an io.Pipe: Do returns as soon as
	// the controller flushes its 200, and Go's transport flushes each chunk
	// of a chunked request body as it is written — which is the difference
	// between a keystroke arriving now and arriving once 4KB have piled up.
	pr, pw := io.Pipe()
	inReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"in", pr)
	if err != nil {
		outResp.Body.Close()
		return nil, err
	}
	inReq.Header.Set("Authorization", "Bearer "+c.token)
	inReq.Header.Set("Content-Type", "application/octet-stream")
	inResp, err := c.http.Do(inReq)
	if err != nil {
		outResp.Body.Close()
		_ = pw.Close()
		return nil, fmt.Errorf("attach input stream: %w", err)
	}
	if inResp.StatusCode != http.StatusOK {
		defer inResp.Body.Close()
		outResp.Body.Close()
		_ = pw.Close()
		return nil, fmt.Errorf("attach input stream: %w", apiError(inResp))
	}

	return &relayStreams{
		out: outResp.Body,
		in:  pw,
		closeOnce: func() {
			_ = pw.Close()
			inResp.Body.Close()
			outResp.Body.Close()
		},
	}, nil
}
