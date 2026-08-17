package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"golang.org/x/term"

	"github.com/mudler/resource-controller/internal/client"
)

// defaultTTYCommand is what `rc run --tty` runs when the caller names no
// command. bash when the box has it, sh when it does not: the worker image is
// ours and has bash, but a systemd worker on a minimal host may not, and
// "your shell did not start" is a poor first impression for a feature whose
// entire promise is "a shell on the box". `-i` because a shell that does not
// know it is interactive prints no prompt.
var defaultTTYCommand = []string{"/bin/sh", "-c", "exec $(command -v bash || command -v sh) -i"}

// attachTerminal puts the local terminal in raw mode and joins jobID's
// session, restoring the terminal on every path out.
//
// Raw mode is the part no test covers, and getting it wrong is expensive in a
// way the feature is not worth: a client that exits without restoring leaves
// the operator typing blind into a shell with no echo and no line editing,
// which they then have to `reset` out of. So the restore is a sync.OnceFunc
// deferred immediately after MakeRaw — before anything that can fail — and
// therefore runs on a normal return, an error, and a panic alike. A signal is
// covered by the caller's signal.NotifyContext: cancelling ctx ends AttachTTY
// and unwinds through that same defer. Ctrl-C is not a signal here at all —
// in raw mode it is a byte, which is the point: it reaches the far side's
// terminal and interrupts the job, not this client.
func attachTerminal(ctx context.Context, c *client.Client, jobID string, stderr io.Writer) error {
	in, out := os.Stdin, os.Stdout
	fd := int(in.Fd())
	if !term.IsTerminal(fd) {
		return errors.New("--tty needs a terminal on stdin; run it from a shell, or drop --tty and pass a command")
	}

	state, err := term.MakeRaw(fd)
	if err != nil {
		return fmt.Errorf("put the terminal in raw mode: %w", err)
	}
	restore := sync.OnceFunc(func() {
		if err := term.Restore(fd, state); err != nil {
			fmt.Fprintf(stderr, "\r\nrc: could not restore the terminal (%v); run `reset`\r\n", err)
		}
	})
	defer restore()

	size := func() (uint16, uint16, error) {
		w, h, err := term.GetSize(fd)
		if err != nil {
			return 0, 0, err
		}
		return uint16(h), uint16(w), nil
	}

	// SIGWINCH is coalesced into a one-deep channel: a drag resizes a window
	// dozens of times a second, and every tick reads the CURRENT size, so
	// dropping the ones in between costs nothing and sending them all would
	// put a burst of frames on the keystroke path.
	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	defer signal.Stop(winch)

	resized := make(chan struct{}, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-winch:
				select {
				case resized <- struct{}{}:
				default:
				}
			}
		}
	}()

	err = c.AttachTTY(ctx, jobID, client.Terminal{
		In: in, Out: out, Size: size, Resized: resized,
	})
	// Restored before anything else is printed, so the caller's own messages
	// land on a terminal that has its newline translation back.
	restore()
	return err
}
