package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// The framing for the `in` direction of an interactive session, defined here
// so the worker end and the client end agree on one wire format rather than
// each writing their own encoder against the same prose.
//
// The `in` direction carries two kinds of message and is tiny — a keystroke
// at a time — so it is newline-delimited JSON:
//
//	{"t":"d","b":"bHM="}             data: base64 keystrokes
//	{"t":"r","rows":48,"cols":180}   resize
//
// The `out` direction has no framing at all: it is high-volume,
// single-purpose and raw.
//
// The relay itself (tty.go) never decodes any of this. It copies bytes, so a
// frame type added to this format later reaches a worker through a controller
// that predates it.
const (
	TTYFrameData   = "d"
	TTYFrameResize = "r"
)

// maxTTYFrame bounds one line. A paste can be large, but a megabyte with no
// newline in it is not a keystroke; without a bound, a peer could make the
// reader buffer without limit.
const maxTTYFrame = 1 << 20

// TTYFrame is one message on the `in` direction. B is base64 on the wire:
// encoding/json does that for []byte, which is also why the terminal's bytes
// need no escaping of their own and a frame can never contain a raw newline —
// what makes newline-delimited framing safe for arbitrary keystrokes.
type TTYFrame struct {
	T    string `json:"t"`
	B    []byte `json:"b,omitempty"`
	Rows uint16 `json:"rows,omitempty"`
	Cols uint16 `json:"cols,omitempty"`
}

// TTYData is a frame carrying keystrokes.
func TTYData(b []byte) TTYFrame { return TTYFrame{T: TTYFrameData, B: b} }

// TTYResize is a frame announcing the terminal's new size.
func TTYResize(rows, cols uint16) TTYFrame {
	return TTYFrame{T: TTYFrameResize, Rows: rows, Cols: cols}
}

// WriteTTYFrame encodes f and writes it as a single line in a single Write,
// so a frame is never interleaved with another writer's.
func WriteTTYFrame(w io.Writer, f TTYFrame) error {
	line, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("encode tty frame: %w", err)
	}
	if _, err := w.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

// TTYFrameReader reads newline-delimited frames off a stream, reassembling
// frames that arrive split across chunks — which they will, since the relay
// copies bytes and respects no message boundary.
type TTYFrameReader struct {
	sc *bufio.Scanner
}

func NewTTYFrameReader(r io.Reader) *TTYFrameReader {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 4096), maxTTYFrame)
	return &TTYFrameReader{sc: sc}
}

// Next returns the next frame, or io.EOF when the stream ends.
//
// A frame whose type this build does not know is skipped rather than
// rejected: the format is meant to grow, and an old worker must not kill a
// terminal because a newer client sent it something extra. Malformed JSON is
// a different matter — the framing itself is lost — and is an error.
func (r *TTYFrameReader) Next() (TTYFrame, error) {
	for {
		if !r.sc.Scan() {
			if err := r.sc.Err(); err != nil {
				return TTYFrame{}, err
			}
			return TTYFrame{}, io.EOF
		}
		line := bytes.TrimSpace(r.sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var f TTYFrame
		if err := json.Unmarshal(line, &f); err != nil {
			return TTYFrame{}, fmt.Errorf("decode tty frame: %w", err)
		}
		if f.T != TTYFrameData && f.T != TTYFrameResize {
			continue
		}
		return f, nil
	}
}
