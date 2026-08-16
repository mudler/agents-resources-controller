package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/creack/pty"
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
	ctx  context.Context
	spec JobSpec

	clock *idleClock
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

	f, err := pty.Start(cmd)
	if err != nil {
		return nil, fmt.Errorf("start pty: %w", err)
	}

	return &ptySession{
		f:     f,
		cmd:   cmd,
		sid:   cmd.Process.Pid,
		ctx:   ctx,
		spec:  spec,
		clock: newIdleClock(time.Now()),
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

// Close closes the PTY master. The process is not explicitly signalled here
// — losing the master hangs up the terminal, which the kernel delivers to
// the session's foreground process group as SIGHUP — but a caller that wants
// the supervised kill sequence (and the guarantee that backgrounded children
// die too) should cancel the context passed to startPTY and call Wait.
func (s *ptySession) Close() error {
	return s.f.Close()
}

// Resize changes the terminal's window size, visible to the process the
// next time it asks (e.g. via TIOCGWINSZ, what `stty size` reports).
func (s *ptySession) Resize(rows, cols uint16) error {
	return pty.Setsize(s.f, &pty.Winsize{Rows: rows, Cols: cols})
}

// Wait blocks until the process exits or the context passed to startPTY is
// cancelled, then applies the same supervised kill sequence Run uses —
// escalated to the whole session (sessionTarget) rather than one process
// group, for the job-control reason explained on startPTY. It returns a
// Result compatible with Run's.
func (s *ptySession) Wait() Result {
	return runSupervised(s.ctx, s.cmd, sessionTarget(s.sid), s.spec, s.clock)
}

// sessionTarget targets every process group that belongs to Linux session
// sid, not just one. It exists because job control — which an interactive
// shell attached to a controlling terminal enables — moves each backgrounded
// command into its own process group for signal-per-job delivery (that's how
// Ctrl-C hits only the foreground job); those groups all remain members of
// the same session, but a plain kill(-pgid) aimed at just the shell's own
// group never reaches them.
type sessionTarget int

func (t sessionTarget) signal(sig syscall.Signal) (delivered bool) {
	for _, pgid := range sessionProcessGroups(int(t)) {
		if syscall.Kill(-pgid, sig) == nil {
			delivered = true
		}
	}
	return delivered
}

func (t sessionTarget) alive() bool {
	return len(sessionProcessGroups(int(t))) > 0
}

// sessionProcessGroups returns every distinct process-group id currently
// present in Linux session sid, discovered by walking /proc. There is no
// kill-a-whole-session syscall; this is the standard way to approximate one
// from userspace on Linux, the only platform this worker targets (see
// BootID in bootid.go for the same /proc-or-nothing precedent).
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
		pgid, psid, ok := procGroupAndSession(pid)
		if !ok || psid != sid {
			continue
		}
		if _, dup := seen[pgid]; dup {
			continue
		}
		seen[pgid] = struct{}{}
		pgids = append(pgids, pgid)
	}
	return pgids
}

// procGroupAndSession reads /proc/<pid>/stat and extracts the process group
// and session ids (fields 5 and 6 in proc(5), 1-indexed). The comm field
// (field 2) is parenthesized and may itself contain spaces or parens, so
// fields are located from the LAST ')' rather than by naive whitespace
// splitting, exactly as proc(5) documents.
func procGroupAndSession(pid int) (pgid, sid int, ok bool) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, 0, false
	}
	i := strings.LastIndexByte(string(data), ')')
	if i < 0 || i+2 >= len(data) {
		return 0, 0, false
	}
	// Fields after ") ": state(1) ppid(2) pgrp(3) session(4) ...
	fields := strings.Fields(string(data[i+2:]))
	if len(fields) < 4 {
		return 0, 0, false
	}
	pgrp, err1 := strconv.Atoi(fields[2])
	sess, err2 := strconv.Atoi(fields[3])
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return pgrp, sess, true
}
