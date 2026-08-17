package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
)

// defaultRows and defaultCols are what a session starts at before any
// caller has sent a real terminal size. Leaving a PTY at its zero value
// (what pty.Start alone gives you — nothing calls TIOCSWINSZ before the
// process starts) is a real 0x0 to any program that asks: vim, less, and
// top all misbehave on it. 24x80 is the traditional terminal default and a
// safe size for a client that never gets around to sending a resize.
const (
	defaultRows = 24
	defaultCols = 80
)

// ptySession is a job whose stdio is a real pseudo-terminal instead of
// pipes. It is a sibling of Run: startPTY spawns the process, and Wait
// applies the identical SIGTERM -> grace -> SIGKILL supervision (via
// runSupervised, shared with Run) and returns the same Result — a caller
// can branch on one flag and otherwise treat both the same way. The only
// thing that changes is what the process sees on fd 1: a terminal, which is
// the entire reason `--tty` exists over shelling out to something
// unsupervised.
type ptySession struct {
	f    *os.File // the PTY master; doubles as the terminal's ReadWriteCloser.
	cmd  *exec.Cmd
	sid  int // session id; pty.Start makes the child lead a new session, so sid == the child's own pid.
	spec JobSpec

	ctx    context.Context
	cancel context.CancelFunc

	clock *idleClock

	waitOnce sync.Once
	waitRes  Result
}

// startPTY spawns spec.Command attached to a new pseudo-terminal, in its own
// SESSION (pty.Start sets Setsid+Setctty), which also makes it the leader of
// its own process group — the same starting point Run gives a piped job via
// Setpgid. The difference matters once the shell is interactive: attached to
// a controlling terminal, it enables job control, and a command backgrounded
// inside it (`sleep 300 &`) gets moved to its OWN process group, distinct
// from the shell's, even though both stay in the same session. Wait's
// sessionTarget (below) is what reaches those backgrounded children; a plain
// pgid kill would leave them holding the GPU after the shell itself was gone.
func startPTY(ctx context.Context, spec JobSpec) (*ptySession, error) {
	if len(spec.Command) == 0 {
		return nil, errors.New("empty command")
	}

	cmd := exec.Command(spec.Command[0], spec.Command[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env = os.Environ()
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	// A PTY has exactly one stream in each direction: stdout and stderr are
	// the same terminal fd, same as any real interactive shell. JobSpec.Stderr
	// (which lets Run split them for a sink that parses stdout on its own) has
	// no meaning here and is intentionally not consulted.

	f, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: defaultRows, Cols: defaultCols})
	if err != nil {
		return nil, fmt.Errorf("start pty: %w", err)
	}

	// sessionCtx/cancel are internal to the session, derived from the
	// caller's ctx: cancelling ctx cancels sessionCtx exactly as before, but
	// Close (below) can now also trip it directly, without requiring the
	// caller to hold onto and cancel its own context just to tear a session
	// down.
	sessionCtx, cancel := context.WithCancel(ctx)

	return &ptySession{
		f:      f,
		cmd:    cmd,
		sid:    cmd.Process.Pid,
		spec:   spec,
		ctx:    sessionCtx,
		cancel: cancel,
		clock:  newIdleClock(time.Now()),
	}, nil
}

// Read satisfies io.Reader over the PTY master, touching the idle clock on
// every read that actually returns bytes — the same "output means progress"
// signal Run gets for free from watchdogWriter wrapping its sink.
func (s *ptySession) Read(p []byte) (int, error) {
	n, err := s.f.Read(p)
	if n > 0 {
		s.clock.touch()
	}
	return n, err
}

// Write satisfies io.Writer over the PTY master: bytes written here are
// keystrokes delivered to the process's stdin.
func (s *ptySession) Write(p []byte) (int, error) {
	return s.f.Write(p)
}

// Close ends the session and reaps it: it cancels the session's internal
// context — the same trigger cancelling the ctx passed to startPTY would
// pull — which drives Wait's full supervised kill sequence (SIGTERM, grace,
// SIGKILL, and the session-wide straggler sweep across every process group
// job control created), then closes the PTY master once that has finished.
//
// This used to just close the master and rely on the kernel's SIGHUP, which
// only reaches the foreground process group — a caller that called Close
// without ever separately calling Wait (as the tests in this file's first
// three cases do) leaked one unreaped zombie per session, permanently: the
// process had exited, but nothing had called cmd.Wait() to collect it.
// Making Close itself perform the full teardown, rather than documenting
// that callers must remember to call Wait first, is deliberate: a future
// caller — the streaming relay this task exists for — closing the terminal
// on client disconnect is exactly the scenario that leaked, and that rule is
// exactly the kind a refactor silently breaks if it's only written down.
//
// Calling Wait yourself first (the normal interactive-session shape: a
// goroutine blocked in Wait while the caller relays I/O) is unaffected —
// Close's internal call to Wait is idempotent and returns the same cached
// Result.
func (s *ptySession) Close() error {
	s.cancel()
	s.Wait()
	return s.f.Close()
}

// Resize changes the terminal's window size, visible to the process the
// next time it asks (e.g. via TIOCGWINSZ, what `stty size` reports).
func (s *ptySession) Resize(rows, cols uint16) error {
	return pty.Setsize(s.f, &pty.Winsize{Rows: rows, Cols: cols})
}

// Wait blocks until the process exits or the context passed to startPTY (or
// Close) is cancelled, then applies the same supervised kill sequence Run
// uses — escalated to the whole session (sessionTarget) rather than one
// process group, for the job-control reason explained on startPTY. It
// returns a Result compatible with Run's.
//
// Wait runs the supervision sequence exactly once, however many times it or
// Close are called (from however many goroutines): the first call does the
// work, every other call blocks until that finishes and then returns the
// same cached Result. That is what lets Close call Wait unconditionally
// without either re-running the kill sequence on an already-finished
// session or racing a caller who is already blocked in its own Wait call.
func (s *ptySession) Wait() Result {
	s.waitOnce.Do(func() {
		s.waitRes = runSupervised(s.ctx, s.cmd, sessionTarget(s.sid), s.spec, s.clock)
	})
	return s.waitRes
}

// sessionTarget targets every process group that belongs to Linux session
// sid, not just one. It exists because job control — which an interactive
// shell attached to a controlling terminal enables — moves each backgrounded
// command into its own process group for signal-per-job delivery (that's how
// Ctrl-C hits only the foreground job); those groups all remain members of
// the same session, but a plain kill(-pgid) aimed at just the shell's own
// group never reaches them.
//
// Known residual, accepted rather than closed here: a process that calls
// setsid(2) itself — most commonly a daemonizing tool like tmux or screen,
// which is often the very first thing an operator runs in a session they
// might get disconnected from — leaves the session it was started in
// entirely and becomes invisible to this scan. It reports a normal
// termination signal delivered to everything WE can still see, so Wait can
// still return Killed: true while that detached process keeps running.
// Double-fork and ordinary nested backgrounding do not escape this — only
// an explicit new session does. Closing that fully needs
// PR_SET_CHILD_SUBREAPER plus a descendant walk, or a cgroup v2 scope per
// lease; neither is implemented here. The existing backstop is Stage 4's
// post-job verify probes, which quarantine a device whose VRAM is still
// pinned after the job the controller thinks ended — precisely this
// failure — so it does not go undetected, just undetected by this function.
type sessionTarget int

func (t sessionTarget) signal(sig syscall.Signal) (delivered bool) {
	for _, pgid := range sessionProcessGroups(int(t)) {
		if syscall.Kill(-pgid, sig) == nil {
			delivered = true
		}
	}
	return delivered
}

// alive asks whether anything in the session is a real process. A session
// whose only remaining members are zombies is empty for every purpose this
// worker has: nothing holds the GPU, nothing runs, and nothing can receive a
// signal. See groupAlive in exec.go for what treating them as alive cost.
func (t sessionTarget) alive() bool {
	live, _ := liveProcExists(func(st procStat) bool { return st.sid == int(t) })
	return live
}

// sessionProcessGroups returns every distinct process-group id currently
// present in Linux session sid, discovered by walking /proc. There is no
// kill-a-whole-session syscall; this is the standard way to approximate one
// from userspace on Linux, the only platform this worker targets (see
// BootID in bootid.go for the same /proc-or-nothing precedent).
//
// Zombies are deliberately NOT filtered out here, unlike in alive: this list
// is what signal aims at, and a group that currently holds only a zombie may
// still hold a live process a moment later (the reverse ordering of the same
// race). Signalling one is harmless — it is what kill(2) against a zombie
// already does — whereas skipping a group is not.
func sessionProcessGroups(sid int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	seen := make(map[int]struct{})
	var pgids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue // not a /proc/<pid> entry
		}
		st, ok := readProcStat(pid)
		if !ok || st.sid != sid {
			continue
		}
		if _, dup := seen[st.pgid]; dup {
			continue
		}
		seen[st.pgid] = struct{}{}
		pgids = append(pgids, st.pgid)
	}
	return pgids
}

// liveProcExists reports whether any process matching want is more than a
// zombie — an entry that has already exited and is only waiting for its
// parent to reap it.
//
// scanned reports whether /proc could be read at all, because "found nothing"
// and "could not look" mean opposite things to a caller deciding whether to
// declare a target empty.
func liveProcExists(want func(procStat) bool) (live, scanned bool) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false, false
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue // not a /proc/<pid> entry
		}
		st, ok := readProcStat(pid)
		if !ok {
			continue // exited while we walked; it holds nothing either
		}
		if st.state == 'Z' || !want(st) {
			continue
		}
		return true, true
	}
	return false, true
}

// procStat is the four fields this worker reads out of /proc/<pid>/stat.
type procStat struct {
	// pid is the process's own id (field 1). It is here so a predicate
	// handed to liveProcExists can exclude a specific process — the worker
	// itself, when asking "is anything ELSE alive in my PID namespace" — and
	// so a predicate can go on to read another file under /proc/<pid>/ for
	// the same process without a second walk of /proc. See recovery.go.
	pid int
	// state is proc(5)'s single-character state. Only 'Z' (zombie: exited,
	// unreaped) is interpreted here, and only to mean "not really there".
	state byte
	pgid  int
	sid   int
}

// readProcStat reads /proc/<pid>/stat and parses it via parseProcStat.
func readProcStat(pid int) (procStat, bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return procStat{}, false
	}
	return parseProcStat(data)
}

// parseProcStat extracts the pid, state, process group and session ids
// (fields 1, 3, 5 and 6 in proc(5), 1-indexed) from the raw contents of a
// /proc/<pid>/stat file. The comm field (field 2) is parenthesized and may
// itself contain spaces or even parens (a process can name itself anything
// via prctl/argv[0]), so the fields after it are located from the LAST ')'
// rather than by naive whitespace splitting, exactly as proc(5) documents —
// and the pid, which precedes comm, is read from the front for the same
// reason.
func parseProcStat(data []byte) (procStat, bool) {
	s := string(data)
	open := strings.IndexByte(s, '(')
	i := strings.LastIndexByte(s, ')')
	if open < 0 || i < open || i+2 >= len(data) {
		return procStat{}, false
	}
	pid, err0 := strconv.Atoi(strings.TrimSpace(s[:open]))
	// Fields after ") ": state(1) ppid(2) pgrp(3) session(4) ...
	fields := strings.Fields(s[i+2:])
	if len(fields) < 4 {
		return procStat{}, false
	}
	pgrp, err1 := strconv.Atoi(fields[2])
	sess, err2 := strconv.Atoi(fields[3])
	if err0 != nil || err1 != nil || err2 != nil || fields[0] == "" {
		return procStat{}, false
	}
	return procStat{pid: pid, state: fields[0][0], pgid: pgrp, sid: sess}, true
}
