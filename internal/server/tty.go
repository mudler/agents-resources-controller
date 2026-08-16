package server

import (
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
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
// Nothing on the relay path touches the store. SQLite runs at
// MaxOpenConns(1), which the scheduler holds while it allocates; a keystroke
// that had to wait on that connection would make typing stutter every time
// somebody submitted a job. The job is looked up exactly once, when a half
// connects (see ttyAttach), and never again for the life of the session.

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

// ttyHandoffTimeout bounds how long a half may block handing bytes to the
// other, and exists for one case only: a half that is the only thing left
// attached to its session.
//
// Reads from a session pipe are deliberately unbounded — an idle terminal is
// a normal terminal — and are interruptible through the request context (see
// ttyAttach). A *write* has neither escape. It blocks in io.Pipe, which no
// context can preempt, and a POST half gets no context cancellation while it
// sits in that write: net/http only reports a dropped connection to a handler
// that is *currently blocked in a body read*, and only starts the background
// read that would notice one once the body hits EOF. A handler blocked in the
// pipe write is in neither state, which is why this timer is its only escape.
//
// See handoffGuard.fire for why expiry only ends a half that is alone: a
// blocked write is normal and may last as long as a queue does.
var ttyHandoffTimeout = 60 * time.Second

var (
	errTTYClosed   = errors.New("tty session closed")
	errTTYSlotBusy = errors.New("that half of this job's terminal is already connected")
)

// ttySlot identifies one of the four streams. Each may have exactly one
// connection at a time: two readers on one pipe would split the byte stream
// between them and hand each a corrupt terminal.
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
	r      *io.PipeReader
	w      *io.PipeWriter
	closed atomic.Bool
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
// the handler, from its context watcher and from a handoff guard at once.
func (p *ttyPipe) Close() error {
	p.closed.Store(true)
	_ = p.w.Close()
	return p.r.CloseWithError(errTTYClosed)
}

func (p *ttyPipe) isClosed() bool { return p.closed.Load() }

// ttySession is one job's live terminal. It is held only in memory and
// deliberately never persisted: a controller restart drops every session,
// which is correct, because the terminals on the other end are gone too.
// Persisting them would put a keystroke on the SQLite path.
type ttySession struct {
	jobID string

	// out is fixed for the session's life, because the session's life *is*
	// the output direction: either out half ending ends the session.
	out *ttyPipe

	// in is replaceable, and mu guards it. The input direction closing is an
	// ordinary event — a deliberate stdin EOF, or a dropped connection, which
	// look identical from here — and must not leave the terminal permanently
	// output-only, so a half that reconnects gets a fresh pipe. See renewIn.
	mu   sync.Mutex
	in   *ttyPipe
	dead bool

	done      chan struct{}
	closeOnce sync.Once

	// attached is guarded by the owning registry's mutex rather than by mu,
	// so "does this session still have anyone on it" and "is this session
	// still in the map" are decided under one lock and cannot disagree.
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

func (s *ttySession) inPipe() *ttyPipe {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.in
}

// renewIn replaces the input pipe if it has been closed, so a reconnecting
// half gets a live one. Without it, a single dropped keystroke connection —
// or an input half that was closed because it was briefly the only thing
// attached — would leave an operator with a terminal that prints perfectly
// and swallows every key for the rest of the job, reporting nothing.
//
// A session that is already dead is never resurrected: its halves are gone
// and it is no longer in the registry.
func (s *ttySession) renewIn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dead && s.in.isClosed() {
		s.in = newTTYPipe()
	}
}

func (s *ttySession) pipe(d ttyDir) *ttyPipe {
	if d == dirOut {
		return s.out
	}
	return s.inPipe()
}

func (s *ttySession) close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.dead = true
		in := s.in
		s.mu.Unlock()

		_ = s.out.Close()
		_ = in.Close()
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
// this is the first half to arrive, and returns the pipe that half will use
// for its whole life. Either half may be first — an operator's terminal and a
// worker's dial-out genuinely race — so neither order is privileged and
// neither drops bytes.
func (r *ttyRegistry) acquire(jobID string, slot ttySlot) (*ttySession, *ttyPipe, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sess := r.sessions[jobID]
	if sess == nil {
		sess = newTTYSession(jobID)
		r.sessions[jobID] = sess
	}
	if sess.attached[slot] {
		return nil, nil, errTTYSlotBusy
	}
	sess.attached[slot] = true
	if slot.dir() == dirIn {
		sess.renewIn()
	}
	return sess, sess.pipe(slot.dir()), nil
}

// release detaches slot. A session nobody is attached to is closed and
// dropped rather than kept: the pipes hold no buffered bytes, so there is
// nothing to preserve, and leaving it in the map would mean every job ID a
// terminal was ever opened for occupies memory until the controller restarts.
func (r *ttyRegistry) release(sess *ttySession, slot ttySlot) {
	r.mu.Lock()
	sess.attached[slot] = false
	idle := r.attachedLocked(sess) == 0
	if idle {
		r.forgetLocked(sess)
	}
	r.mu.Unlock()

	if idle {
		sess.close()
	}
}

// alone reports whether at most one half is attached to sess — that is,
// whether a half blocked handing bytes over has anybody left to hand them to.
func (r *ttyRegistry) alone(sess *ttySession) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attachedLocked(sess) <= 1
}

func (r *ttyRegistry) attachedLocked(sess *ttySession) int {
	n := 0
	for _, a := range sess.attached {
		if a {
			n++
		}
	}
	return n
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
// delete the replacement session a reconnecting peer has already registered,
// which would leave the two ends of one terminal on two different sessions,
// each silent to the other.
func (r *ttyRegistry) forgetLocked(sess *ttySession) {
	if r.sessions[sess.jobID] == sess {
		delete(r.sessions, sess.jobID)
	}
}

// ttyHalf is one attached stream: the session it belongs to and the pipe it
// is bound to for its whole life. A handler holds its own pipe rather than
// re-reading the session's, so a stale half that is slow to unwind cannot
// close the fresh pipe a reconnecting peer is already using.
type ttyHalf struct {
	reg  *ttyRegistry
	sess *ttySession
	pipe *ttyPipe
	dir  ttyDir
}

// ttyAttach is the prologue and epilogue every one of the four handlers
// shares. It returns the half, the teardown to defer, and whether the stream
// may proceed; when it returns false it has already written the error.
func (s *Server) ttyAttach(w http.ResponseWriter, r *http.Request, slot ttySlot) (*ttyHalf, func(), bool) {
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

	// One store read, on connect, for the same reason handleStreamLogs does
	// one: without it any token can pin a goroutine, an fd and a registry
	// entry per request against a job ID that was never allocated. It is one
	// read per *session*, not per keystroke — the relay loop below never
	// touches the store, which is the constraint that actually matters.
	jobID := r.PathValue("id")
	if _, err := s.cfg.Store.Job(jobID); err != nil {
		writeJobLookupError(w, err)
		return nil, nil, false
	}

	sess, pipe, err := s.tty.acquire(jobID, slot)
	if err != nil {
		writeErr(w, http.StatusConflict, "conflict", err.Error())
		return nil, nil, false
	}
	half := &ttyHalf{reg: s.tty, sess: sess, pipe: pipe, dir: slot.dir()}

	// A half blocked reading an idle pipe has no other way to learn that its
	// peer's socket died: nothing will ever arrive to fail on. Both GET
	// halves get a request context that net/http cancels when the connection
	// closes, so watch it and close the pipe, which returns the handler.
	stop := make(chan struct{})
	ctx := r.Context()
	go func() {
		select {
		case <-ctx.Done():
			_ = half.pipe.Close()
		case <-sess.done:
		case <-stop:
		}
	}()

	teardown := func() {
		close(stop)
		if half.dir == dirOut {
			// The output direction is the session's life. The worker half
			// ending means the PTY is gone — a killed job must not leave an
			// operator staring at a terminal that will never say anything
			// again — and the client half ending means there is nobody left
			// to show output to. Either way the session is over.
			s.tty.discard(sess)
		} else {
			// The input direction ending is an EOF on stdin, not the end of
			// the session: a program that has had its stdin closed keeps
			// running and keeps printing, and that output still has to reach
			// the terminal. Only this half's own pipe is closed; a peer that
			// reconnects gets a fresh one from renewIn.
			_ = half.pipe.Close()
		}
		s.tty.release(sess, slot)
	}
	return half, teardown, true
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

// ttyHandoff writes into this half's pipe under a guard. See
// ttyHandoffTimeout: this is the only bound a blocked pipe write can have.
type ttyHandoff struct{ half *ttyHalf }

func (h ttyHandoff) Write(p []byte) (int, error) {
	g := newHandoffGuard(h.half)
	defer g.stop()
	return h.half.pipe.Write(p)
}

// handoffGuard watches one blocked write.
type handoffGuard struct {
	half *ttyHalf

	mu      sync.Mutex
	timer   *time.Timer
	stopped bool
}

func newHandoffGuard(half *ttyHalf) *handoffGuard {
	g := &handoffGuard{half: half}
	// Armed under the lock so fire cannot read timer before it is set.
	g.mu.Lock()
	defer g.mu.Unlock()
	g.timer = time.AfterFunc(ttyHandoffTimeout, g.fire)
	return g
}

// fire runs when a write has been blocked for the whole window. Only a half
// that is the sole thing attached to its session is abandoned; everything
// else re-arms and keeps waiting.
//
// That distinction is the difference between a working feature and a subtle
// one-way terminal. An operator attaches both halves the moment they submit,
// and then the job waits for a GPU — which on a controller whose whole
// purpose is leasing exclusive GPUs is routinely longer than any timeout
// worth having. Their opening resize frame, or anything they type, blocks in
// exactly this write until the worker dials in. Closing the pipe there would
// give them a terminal that prints perfectly and silently swallows every
// keystroke for the rest of the job, with no error anywhere.
//
// Nothing else needs this timer: every other pin unwinds through the output
// direction, whose halves do get context cancellation.
func (g *handoffGuard) fire() {
	if g.half.reg.alone(g.half.sess) {
		_ = g.half.pipe.Close()
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.stopped {
		g.timer.Reset(ttyHandoffTimeout)
	}
}

func (g *handoffGuard) stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stopped = true
	g.timer.Stop()
}

// handleTTYWorkerOut takes the PTY's output from the box. Raw bytes, no
// framing: this direction is high-volume and single-purpose.
func (s *Server) handleTTYWorkerOut(w http.ResponseWriter, r *http.Request) {
	half, teardown, ok := s.ttyAttach(w, r, slotWorkerOut)
	if !ok {
		return
	}
	defer teardown()

	tw := newTTYStreamWriter(w, "text/plain; charset=utf-8")
	if !tw.flushHeader() {
		return
	}
	_, _ = io.Copy(ttyHandoff{half}, r.Body)
}

// handleTTYClientOut hands that output to the operator's terminal.
func (s *Server) handleTTYClientOut(w http.ResponseWriter, r *http.Request) {
	half, teardown, ok := s.ttyAttach(w, r, slotClientOut)
	if !ok {
		return
	}
	defer teardown()

	tw := newTTYStreamWriter(w, "application/octet-stream")
	if !tw.flushHeader() {
		return
	}
	_, _ = io.Copy(tw, half.pipe)
}

// handleTTYClientIn takes keystrokes and resizes from the operator. The relay
// does not decode them: it is byte-opaque, so a frame type added later works
// through a controller that predates it. See tty_frame.go for the format the
// two ends agree on.
func (s *Server) handleTTYClientIn(w http.ResponseWriter, r *http.Request) {
	half, teardown, ok := s.ttyAttach(w, r, slotClientIn)
	if !ok {
		return
	}
	defer teardown()

	tw := newTTYStreamWriter(w, "text/plain; charset=utf-8")
	if !tw.flushHeader() {
		return
	}
	_, _ = io.Copy(ttyHandoff{half}, r.Body)
}

// handleTTYWorkerIn hands them to the box.
func (s *Server) handleTTYWorkerIn(w http.ResponseWriter, r *http.Request) {
	half, teardown, ok := s.ttyAttach(w, r, slotWorkerIn)
	if !ok {
		return
	}
	defer teardown()

	tw := newTTYStreamWriter(w, "application/x-ndjson")
	if !tw.flushHeader() {
		return
	}
	_, _ = io.Copy(tw, half.pipe)
}
