package server

import (
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// An interactive session is four chunked-HTTP streams the controller splices
// together in memory:
//
//	client  GET  /v1/jobs/{id}/tty/out  ◀── relay ──  POST /v1/jobs/{id}/tty/out  worker
//	client  POST /v1/jobs/{id}/tty/in   ──▶ relay ──▶ GET  /v1/jobs/{id}/tty/in   worker
//
// Both of the worker's connections are opened by the worker. That is the
// whole point: the controller never dials a box, so a NAT'd or firewalled
// host works with no inbound port and the controller holds no credentials
// for it. Every other design here follows from keeping that true.
//
// Nothing in this file touches the store. SQLite runs at MaxOpenConns(1),
// which the scheduler holds while it allocates; a keystroke that had to wait
// on that connection would make typing stutter every time somebody submitted
// a job. The cost is that this relay cannot check that a job exists — see
// ttyRegistry.acquire.

// ttyWriteTimeout bounds every write to a TTY stream's socket, exactly as
// logWriteTimeout does for the log stream and eventWriteTimeout for SSE: a
// wedged peer (a suspended laptop, a blackholed route) otherwise blocks the
// handler inside a write forever, since r.Context().Done() cannot preempt a
// write already in flight, and the handler's goroutine, its connection and
// its session slot leak for the life of the process. A terminal that has not
// accepted a byte in this long is gone, whatever its TCP socket still claims.
//
// A variable rather than a constant so tests can shrink it.
var ttyWriteTimeout = 10 * time.Second

// ttyHandoffTimeout bounds how long one half may block handing bytes to the
// other. Reads from a session pipe are deliberately unbounded — an idle
// terminal is a normal terminal, and a read is interruptible through the
// request context (see ttyAttach) — but a *write* has no such escape: it
// blocks in io.Pipe, which no context can preempt, and the two POST halves
// get no context cancellation at all while their request body is unread
// (net/http only starts the background read that notices a dropped
// connection once the body hits EOF). So a half that cannot hand its bytes
// over within this window treats the direction as dead and closes it, which
// unblocks the writer and lets the handler return.
//
// Generous, because "the other half has not connected yet" is legitimate for
// as long as it takes a queued job to start; but bounded, because "the other
// half is never coming" must not cost a goroutine and a socket forever.
var ttyHandoffTimeout = 60 * time.Second

var (
	errTTYClosed   = errors.New("tty session closed")
	errTTYSlotBusy = errors.New("that half of this job's terminal is already connected")
)

// ttySlot identifies one of the four streams. Each may have exactly one
// connection at a time: two readers on one pipe would split the byte stream
// between them and hand each half a corrupt terminal.
type ttySlot int

const (
	slotClientOut ttySlot = iota // GET  /tty/out — the operator's terminal, reading
	slotWorkerOut                // POST /tty/out — the box, writing PTY output
	slotClientIn                 // POST /tty/in  — the operator's keystrokes
	slotWorkerIn                 // GET  /tty/in  — the box, reading keystrokes
	ttySlotCount
)

// ttyDir is a direction of flow. The two slots of a direction are the two
// ends of one pipe.
type ttyDir int

const (
	dirOut ttyDir = iota // worker → client: raw PTY bytes, no framing
	dirIn                // client → worker: newline-delimited frames (tty_frame.go)
)

func (s ttySlot) dir() ttyDir {
	if s == slotClientOut || s == slotWorkerOut {
		return dirOut
	}
	return dirIn
}

// ttyPipe is one direction of a session: an unbuffered io.Pipe, so a byte is
// never acknowledged to the side that sent it until the other side has taken
// it. That is what makes "either half may connect first" hold without a
// buffer to size or drop from — a worker that starts writing before the
// operator's terminal attaches simply blocks until it does.
type ttyPipe struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func newTTYPipe() *ttyPipe {
	r, w := io.Pipe()
	return &ttyPipe{r: r, w: w}
}

func (p *ttyPipe) Read(b []byte) (int, error)  { return p.r.Read(b) }
func (p *ttyPipe) Write(b []byte) (int, error) { return p.w.Write(b) }

// Close ends both ends of the direction: a blocked reader sees a clean EOF
// (the stream ended, which is not an error for the peer to report), a blocked
// writer sees errTTYClosed. Both are idempotent, so Close may be called from
// the handler, from its context watcher and from a handoff timer at once.
func (p *ttyPipe) Close() error {
	_ = p.w.Close()
	return p.r.CloseWithError(errTTYClosed)
}

// ttySession is one job's live terminal. It is held only in memory and
// deliberately never persisted: a controller restart drops every session,
// which is correct, because the terminals on the other end are gone too.
// Persisting them would put a keystroke on the SQLite path.
type ttySession struct {
	jobID string
	out   *ttyPipe
	in    *ttyPipe

	done      chan struct{}
	closeOnce sync.Once

	// attached is guarded by the owning registry's mutex rather than by a
	// second lock here, so "does this session still have anyone on it" and
	// "is this session still in the map" are decided under one lock and
	// cannot disagree.
	attached [ttySlotCount]bool
}

func newTTYSession(jobID string) *ttySession {
	return &ttySession{
		jobID: jobID,
		out:   newTTYPipe(),
		in:    newTTYPipe(),
		done:  make(chan struct{}),
	}
}

func (s *ttySession) pipe(d ttyDir) *ttyPipe {
	if d == dirOut {
		return s.out
	}
	return s.in
}

func (s *ttySession) close() {
	s.closeOnce.Do(func() {
		_ = s.out.Close()
		_ = s.in.Close()
		close(s.done)
	})
}

// ttyRegistry maps job ID → live session. Keyed by job, not by worker: a
// worker runs jobs for several submitters, and keying by worker would splice
// one operator's keystrokes into another operator's terminal.
type ttyRegistry struct {
	mu       sync.Mutex
	sessions map[string]*ttySession
}

func newTTYRegistry() *ttyRegistry {
	return &ttyRegistry{sessions: map[string]*ttySession{}}
}

// acquire attaches slot to the session for jobID, creating the session if
// this is the first half to arrive. Either half may be first — an operator's
// terminal and a worker's dial-out genuinely race — so neither order is
// privileged and neither drops bytes.
//
// The job ID is not validated, because validating it would mean a store read
// on the connect path. The exposure is that a client token can hold a session
// for a job that does not exist; each such session costs one map entry and
// requires the caller to hold an HTTP connection open, which the server
// already bounds.
func (r *ttyRegistry) acquire(jobID string, slot ttySlot) (*ttySession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sess := r.sessions[jobID]
	if sess == nil {
		sess = newTTYSession(jobID)
		r.sessions[jobID] = sess
	}
	if sess.attached[slot] {
		return nil, errTTYSlotBusy
	}
	sess.attached[slot] = true
	return sess, nil
}

// release detaches slot. A session nobody is attached to is closed and
// dropped rather than kept: the pipes hold no buffered bytes, so there is
// nothing to preserve, and leaving it in the map would mean a job ID that
// nobody is on still occupies memory until the controller restarts.
func (r *ttyRegistry) release(sess *ttySession, slot ttySlot) {
	r.mu.Lock()
	sess.attached[slot] = false
	idle := true
	for _, a := range sess.attached {
		if a {
			idle = false
			break
		}
	}
	if idle {
		r.forgetLocked(sess)
	}
	r.mu.Unlock()

	if idle {
		sess.close()
	}
}

// discard ends a session outright and unregisters it, so the next connection
// for that job builds a fresh one instead of attaching to a corpse whose
// pipes are already closed.
func (r *ttyRegistry) discard(sess *ttySession) {
	r.mu.Lock()
	r.forgetLocked(sess)
	r.mu.Unlock()
	sess.close()
}

// forgetLocked removes sess only if it is still the session registered for
// its job. The identity check matters: a half that is slow to unwind must not
// delete the replacement session a reconnecting peer has already registered.
func (r *ttyRegistry) forgetLocked(sess *ttySession) {
	if r.sessions[sess.jobID] == sess {
		delete(r.sessions, sess.jobID)
	}
}

// ttyAttach is the prologue and epilogue every one of the four handlers
// shares. It returns the session, the teardown to defer, and whether the
// stream may proceed; when it returns false it has already written the error.
func (s *Server) ttyAttach(w http.ResponseWriter, r *http.Request, slot ttySlot) (*ttySession, func(), bool) {
	if _, ok := w.(http.Flusher); !ok {
		writeErr(w, http.StatusInternalServerError, "unsupported", "streaming unsupported")
		return nil, nil, false
	}
	// Both POST halves interleave reading their request body with writing
	// their response, and by default net/http refuses to: before it writes a
	// response header it drains up to 256KB of any request body the handler
	// has not consumed, so it can reuse the connection. For a stream that
	// only ends when the terminal does, that is a deadlock — the server sits
	// draining a body that will not end, and the peer waits for the 200 that
	// tells it the relay took its stream. EnableFullDuplex turns the drain
	// off; it is the exact mechanism net/http provides for this shape.
	//
	// HTTP/1 always supports it, so an error there is fatal. HTTP/2 is
	// always full duplex and simply does not implement the method, which is
	// why the error is only fatal for HTTP/1.
	if err := http.NewResponseController(w).EnableFullDuplex(); err != nil && r.ProtoMajor == 1 {
		writeErr(w, http.StatusInternalServerError, "unsupported", "full-duplex streaming unsupported")
		return nil, nil, false
	}
	sess, err := s.tty.acquire(r.PathValue("id"), slot)
	if err != nil {
		writeErr(w, http.StatusConflict, "conflict", err.Error())
		return nil, nil, false
	}

	// A half blocked reading an idle pipe has no other way to learn that its
	// peer's socket died: nothing will ever arrive to fail on. Both GET
	// halves get a request context that net/http cancels when the connection
	// closes, so watch it and close the direction, which returns the handler.
	stop := make(chan struct{})
	ctx := r.Context()
	go func() {
		select {
		case <-ctx.Done():
			_ = sess.pipe(slot.dir()).Close()
		case <-sess.done:
		case <-stop:
		}
	}()

	teardown := func() {
		close(stop)
		if slot.dir() == dirOut {
			// The output direction is the session's life. The worker half
			// ending means the PTY is gone — a killed job must not leave an
			// operator staring at a terminal that will never say anything
			// again — and the client half ending means there is nobody left
			// to show output to. Either way the session is over.
			s.tty.discard(sess)
		} else {
			// The input direction ending is an EOF on stdin, not the end of
			// the session: a program that has had its stdin closed keeps
			// running and keeps printing, and that output still has to
			// reach the terminal.
			_ = sess.in.Close()
		}
		s.tty.release(sess, slot)
	}
	return sess, teardown, true
}

// ttyStreamWriter writes a session's bytes to a peer's socket, flushing each
// chunk under a deadline. Flushing is the difference between an interactive
// terminal and a terminal that answers in paragraphs.
type ttyStreamWriter struct {
	w  http.ResponseWriter
	rc *http.ResponseController
}

func newTTYStreamWriter(w http.ResponseWriter, contentType string) *ttyStreamWriter {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	return &ttyStreamWriter{w: w, rc: http.NewResponseController(w)}
}

func (t *ttyStreamWriter) Write(p []byte) (int, error) {
	if err := t.rc.SetWriteDeadline(time.Now().Add(ttyWriteTimeout)); err != nil {
		return 0, err
	}
	n, err := t.w.Write(p)
	if err != nil {
		return n, err
	}
	return n, t.rc.Flush()
}

// flushHeader pushes the status line out before any payload exists.
// WriteHeader only buffers it, and both peers need to know they are connected
// before the first byte is typed — the client so it can leave the "waiting"
// state, the worker so it knows the relay took the stream rather than 409'd.
func (t *ttyStreamWriter) flushHeader() bool {
	if err := t.rc.SetWriteDeadline(time.Now().Add(ttyWriteTimeout)); err != nil {
		return false
	}
	return t.rc.Flush() == nil
}

// ttyHandoff writes into a session pipe under ttyHandoffTimeout. See that
// variable: this is the only bound a blocked pipe write can have.
type ttyHandoff struct{ pipe *ttyPipe }

func (h ttyHandoff) Write(p []byte) (int, error) {
	t := time.AfterFunc(ttyHandoffTimeout, func() { _ = h.pipe.Close() })
	defer t.Stop()
	return h.pipe.Write(p)
}

// handleTTYWorkerOut takes the PTY's output from the box. Raw bytes, no
// framing: this direction is high-volume and single-purpose.
func (s *Server) handleTTYWorkerOut(w http.ResponseWriter, r *http.Request) {
	sess, teardown, ok := s.ttyAttach(w, r, slotWorkerOut)
	if !ok {
		return
	}
	defer teardown()

	tw := newTTYStreamWriter(w, "text/plain; charset=utf-8")
	if !tw.flushHeader() {
		return
	}
	_, _ = io.Copy(ttyHandoff{sess.out}, r.Body)
}

// handleTTYClientOut hands that output to the operator's terminal.
func (s *Server) handleTTYClientOut(w http.ResponseWriter, r *http.Request) {
	sess, teardown, ok := s.ttyAttach(w, r, slotClientOut)
	if !ok {
		return
	}
	defer teardown()

	tw := newTTYStreamWriter(w, "application/octet-stream")
	if !tw.flushHeader() {
		return
	}
	_, _ = io.Copy(tw, sess.out)
}

// handleTTYClientIn takes keystrokes and resizes from the operator. The relay
// does not decode them: it is byte-opaque, so a frame type added later works
// through a controller that predates it. See tty_frame.go for the format the
// two ends agree on.
func (s *Server) handleTTYClientIn(w http.ResponseWriter, r *http.Request) {
	sess, teardown, ok := s.ttyAttach(w, r, slotClientIn)
	if !ok {
		return
	}
	defer teardown()

	tw := newTTYStreamWriter(w, "text/plain; charset=utf-8")
	if !tw.flushHeader() {
		return
	}
	_, _ = io.Copy(ttyHandoff{sess.in}, r.Body)
}

// handleTTYWorkerIn hands them to the box.
func (s *Server) handleTTYWorkerIn(w http.ResponseWriter, r *http.Request) {
	sess, teardown, ok := s.ttyAttach(w, r, slotWorkerIn)
	if !ok {
		return
	}
	defer teardown()

	tw := newTTYStreamWriter(w, "application/x-ndjson")
	if !tw.flushHeader() {
		return
	}
	_, _ = io.Copy(tw, sess.in)
}
