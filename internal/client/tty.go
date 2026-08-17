package client

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/mudler/resource-controller/internal/server"
)

// Terminal is the local terminal AttachTTY drives, as an interface rather
// than an *os.File so the part that CAN be tested is tested. Raw mode and
// SIGWINCH need a real tty and belong to the caller (see cli.attachTerminal);
// what happens to the bytes afterwards does not, and that is what this
// covers.
type Terminal struct {
	// In is where keystrokes come from, normally os.Stdin in raw mode.
	In io.Reader
	// Out is where the job's output is written, normally os.Stdout.
	Out io.Writer
	// Size reports the terminal's current size. Required: a session that
	// never announces a size leaves the far side at the PTY's 24x80 default,
	// which is why remote shells wrap at 80 columns forever.
	Size func() (rows, cols uint16, err error)
	// Resized, if non-nil, fires whenever the terminal changed size. Every
	// tick sends a fresh resize frame.
	Resized <-chan struct{}
}

// AttachTTY joins a job's terminal: it copies the job's output to the local
// terminal and everything typed locally back to the job, until the job's
// output stream ends.
//
// It may be called the moment a job has an ID, before it is scheduled. The
// relay holds both connect orders, and anything typed during a queue wait is
// held rather than dropped — which for a controller whose whole job is
// leasing exclusive GPUs is the ordinary case, not an edge one.
func (c *Client) AttachTTY(ctx context.Context, jobID string, term Terminal) error {
	if term.Size == nil {
		return errors.New("a terminal attachment needs a Size function")
	}
	streams, err := c.attachRelay(ctx, jobID)
	if err != nil {
		return err
	}
	defer streams.close()

	// One writer goroutine could interleave a resize frame into the middle of
	// a data frame, and half a JSON line is not a frame. WriteTTYFrame emits
	// each frame in a single Write; this mutex is what makes those Writes
	// atomic with respect to each other.
	var mu sync.Mutex
	send := func(f server.TTYFrame) error {
		mu.Lock()
		defer mu.Unlock()
		return server.WriteTTYFrame(streams.in, f)
	}

	// The size goes first, before anything is typed. A shell that has already
	// drawn its prompt at the wrong width redraws on the next command; one
	// that never learns the width is wrong forever.
	//
	// Unless there is no size to send. A zero dimension is what
	// ioctl(TIOCGWINSZ) reports when the far end is not a real terminal, and
	// sending it would replace the PTY's usable 24x80 default with a window
	// no full-screen program can draw in. The far side rejects one too — a
	// worker must not be talked into 0x0 by any client — but not sending
	// nonsense in the first place is this side's job.
	if rows, cols, err := term.Size(); err == nil && rows > 0 && cols > 0 {
		if err := send(server.TTYResize(rows, cols)); err != nil {
			return err
		}
	}

	done := make(chan struct{})
	defer close(done)

	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := term.In.Read(buf)
			if n > 0 {
				if err := send(server.TTYData(buf[:n])); err != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	if term.Resized != nil {
		go func() {
			for {
				select {
				case <-done:
					return
				case <-term.Resized:
					rows, cols, err := term.Size()
					if err != nil || rows == 0 || cols == 0 {
						continue
					}
					if err := send(server.TTYResize(rows, cols)); err != nil {
						return
					}
				}
			}
		}()
	}

	// The output direction is the session's life: it ends when the job's
	// terminal is gone, which is the signal to give the operator their own
	// terminal back.
	_, err = io.Copy(term.Out, streams.out)
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
