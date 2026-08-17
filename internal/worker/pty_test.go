package worker

import (
	"bufio"
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ptyGraceCeiling is used by every test below whose teardown (via Close or
// an explicit cancel) needs the session to actually be force-killed. This
// matters more here than it does in exec_test.go's equivalent tests: an
// INTERACTIVE shell attached to a controlling terminal ignores SIGTERM by
// default (verified directly — /proc/<pid>/status's SigIgn bitmask for a
// bare `/bin/sh` run under a PTY includes SIGTERM; a non-interactive
// `sh -c "..."` does not). That's normal, deliberate shell behavior — it's
// why closing a terminal doesn't kill your login shell — but it means the
// SIGTERM step of every PTY session's kill sequence is a no-op against the
// shell itself, every time; only SIGKILL after grace ever ends it. Leaving
// spec.GraceCeiling at its 10s default would make every such test (and,
// worth flagging for whoever wires up `rc run --tty`'s real defaults, every
// real cancelled session) take a genuine 10 seconds before SIGKILL fires.
const ptyGraceCeiling = 300 * time.Millisecond

// A PTY is not just "a pipe with extra steps": programs behave differently
// when stdout is a terminal, and that difference is the whole reason this
// exists. `test -t 1` is the smallest thing that can tell them apart.
func TestPTYLooksLikeATerminalToTheProcess(t *testing.T) {
	s, err := startPTY(context.Background(), JobSpec{
		Command: []string{"/bin/sh", "-c", "test -t 1 && echo IS_TTY || echo NOT_TTY"},
	})
	require.NoError(t, err)
	defer s.Close()

	line, err := bufio.NewReader(s).ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, "IS_TTY",
		"the process must see a terminal on stdout, or this is just a pipe")
}

// Keystrokes go in, output comes back. The interactive loop in one test.
func TestPTYCarriesInputBackToTheProcess(t *testing.T) {
	s, err := startPTY(context.Background(), JobSpec{Command: []string{"/bin/sh"}, GraceCeiling: ptyGraceCeiling})
	require.NoError(t, err)
	defer s.Close()

	_, err = s.Write([]byte("echo hello-from-stdin\n"))
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	var got strings.Builder
	buf := make([]byte, 1024)
	for time.Now().Before(deadline) {
		n, err := s.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
			if strings.Contains(got.String(), "hello-from-stdin") {
				return
			}
		}
		if err != nil {
			break
		}
	}
	t.Fatalf("never saw the command's output; got: %q", got.String())
}

// A shell asks the terminal how big it is. Getting this wrong is why
// remote shells wrap text at 80 columns forever.
func TestPTYResizeIsVisibleToTheProcess(t *testing.T) {
	s, err := startPTY(context.Background(), JobSpec{Command: []string{"/bin/sh"}, GraceCeiling: ptyGraceCeiling})
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Resize(48, 180))
	_, err = s.Write([]byte("stty size\n"))
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	var got strings.Builder
	buf := make([]byte, 1024)
	for time.Now().Before(deadline) {
		n, _ := s.Read(buf)
		if n > 0 {
			got.WriteString(string(buf[:n]))
			if strings.Contains(got.String(), "48 180") {
				return
			}
		}
	}
	t.Fatalf("resize never reached the process; got: %q", got.String())
}

// The process group rule the rest of this worker lives by: a PTY session
// must be killable as a group, or an interactive shell's children outlive it
// and keep the GPU.
//
// The brief's version of this test only asserted res.Killed, which a broken
// implementation could satisfy while leaving `sleep 300` running forever
// (e.g. by only signalling the shell's own pgid and missing the separate
// process group job control gives a backgrounded command). This version
// captures the backgrounded child's real PID from inside the shell and
// polls the kernel directly to prove it is actually gone after cancellation.
//
// It also checks two things the round-2 review found missing:
//
//   - The shell itself must die too, not just the child. A mutation of
//     sessionTarget that skips the session leader's OWN process group (only
//     signalling everything else) previously passed this test 3/3, because
//     nothing reachable through `s` after Wait() returns keeps the shell
//     process referenced, so the Go runtime finalizing s.f closes the PTY
//     master and the kernel's own SIGHUP-on-hangup — a mechanism this code
//     does not control — killed the shell by accident. A caller that keeps
//     `s` alive (a pump goroutine relaying I/O, exactly Task 3's shape) gets
//     no such accidental help: that mutation would hang in cmd.Wait() until
//     something else intervenes. Asserting shellPID directly, independent of
//     whatever finalizes s.f, closes that gap.
//   - res.Reason must be exactly "cancelled", not just non-empty. A
//     sessionTarget.alive() that always reports true (e.g. by finding stale
//     entries, or misreading /proc) would silently downgrade every teardown
//     to "cancelled; survivors remained after SIGKILL" and pay stragglerWait
//     on every single call — the failure mode is invisible unless something
//     checks the exact string, mirroring exec_test.go's own
//     `require.Equal(t, "cancelled", res.Reason)` for the equivalent case.
func TestPTYKillsTheWholeProcessGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s, err := startPTY(ctx, JobSpec{Command: []string{"/bin/sh"}, GraceCeiling: ptyGraceCeiling})
	require.NoError(t, err)
	shellPID := s.cmd.Process.Pid

	_, err = s.Write([]byte("sleep 300 & echo child:$!\n"))
	require.NoError(t, err)
	childPID := waitForBackgroundChildPID(t, s)
	require.True(t, processAlive(t, childPID), "sanity: the backgrounded sleep must actually be running before we test that cancellation kills it")

	cancel()
	res := s.Wait()
	require.True(t, res.Killed, "cancelling the context must kill the session")
	require.Equal(t, "cancelled", res.Reason,
		"a straggler-recovery reason here would mean the session-wide kill routinely misses on the first pass")

	require.Eventually(t, func() bool {
		return !processAlive(t, childPID)
	}, 5*time.Second, 50*time.Millisecond,
		"backgrounded child %d survived cancellation — killing the shell's own process group is not enough, job control gave the child its own", childPID)

	require.Eventually(t, func() bool {
		return !processAlive(t, shellPID)
	}, 5*time.Second, 50*time.Millisecond,
		"the shell itself (pid %d) must not survive cancellation either", shellPID)
}

// The ordinary way an interactive session ends: the operator types `exit`,
// the shell exits cleanly (code 0), and cancel() is never called at all —
// ctx passed to startPTY simply stays live. Round-2 review found that this
// path skipped the straggler sweep entirely: Run's equivalent case is
// accidentally protected because cmd.Wait() blocks on the inherited stdout
// pipe until every process holding it (including a detached grandchild)
// closes it, but a PTY has no such pipe — Wait() returns the instant the
// shell itself exits, leaving a backgrounded job with nothing left to
// signal it.
func TestPTYCleanShellExitReapsBackgroundedStragglers(t *testing.T) {
	s, err := startPTY(context.Background(), JobSpec{Command: []string{"/bin/sh"}})
	require.NoError(t, err)
	defer s.Close()

	_, err = s.Write([]byte("sleep 300 & echo child:$!\n"))
	require.NoError(t, err)
	childPID := waitForBackgroundChildPID(t, s)
	require.True(t, processAlive(t, childPID), "sanity: the backgrounded sleep must actually be running before we test that a clean exit reaps it")

	_, err = s.Write([]byte("exit\n"))
	require.NoError(t, err)

	res := s.Wait()
	require.True(t, res.Killed,
		"a clean shell exit that leaves a background job running must not be reported as an unremarkable success")

	require.Eventually(t, func() bool {
		return !processAlive(t, childPID)
	}, 5*time.Second, 50*time.Millisecond,
		"backgrounded child %d survived a clean shell exit — Wait returning is not the same as the session being empty", childPID)
}

// Close() without any separate call to Wait() is exactly the shape the
// brief's own first three tests use (`defer s.Close()`, nothing else), and
// it used to leak a zombie every time: closing the PTY master lets the
// kernel's SIGHUP-on-hangup end the shell, but nothing had ever called
// cmd.Wait() to reap it. processAlive returning false requires ESRCH — a
// zombie (exited but unreaped) still answers kill(pid, 0) successfully, so
// this genuinely distinguishes "gone" from "merely dead."
func TestPTYCloseReapsWithoutAnExplicitWait(t *testing.T) {
	s, err := startPTY(context.Background(), JobSpec{Command: []string{"/bin/sh"}, GraceCeiling: ptyGraceCeiling})
	require.NoError(t, err)
	shellPID := s.cmd.Process.Pid

	require.NoError(t, s.Close())

	require.Eventually(t, func() bool {
		return !processAlive(t, shellPID)
	}, 5*time.Second, 50*time.Millisecond,
		"shell pid %d was left an unreaped zombie by Close — Close must perform the full wait/reap sequence itself, not just close the master fd", shellPID)
}

// A freshly started session must already have a usable window size, without
// the caller having to send an explicit Resize first: pty.Start (used by an
// earlier version of startPTY) never calls TIOCSWINSZ at all, leaving a real
// 0x0 that breaks any full-screen program (vim, less, top) before the first
// keystroke.
func TestPTYHasAUsableDefaultWindowSize(t *testing.T) {
	s, err := startPTY(context.Background(), JobSpec{Command: []string{"/bin/sh"}, GraceCeiling: ptyGraceCeiling})
	require.NoError(t, err)
	defer s.Close()

	_, err = s.Write([]byte("stty size\n"))
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	var got strings.Builder
	buf := make([]byte, 1024)
	for time.Now().Before(deadline) {
		n, _ := s.Read(buf)
		if n > 0 {
			got.WriteString(string(buf[:n]))
			if strings.Contains(got.String(), "24 80") {
				return
			}
		}
	}
	t.Fatalf("no usable default window size before any Resize call; got: %q", got.String())
}

// For an interactive session, the idle watchdog is the main defence against
// an operator who walks away holding a GPU — nobody set a MaxRuntime on a
// shell they intend to sit in for hours, but IdleTimeout still applies. This
// was entirely untested on the PTY path.
func TestPTYIdleWatchdogEndsAnAbandonedSession(t *testing.T) {
	s, err := startPTY(context.Background(), JobSpec{
		Command:      []string{"/bin/sh"},
		IdleTimeout:  200 * time.Millisecond,
		GraceCeiling: ptyGraceCeiling,
	})
	require.NoError(t, err)
	defer s.Close()

	// Deliberately never read or write: an abandoned session produces no
	// consumer-side activity, and the idle clock (touched on Read, see
	// ptySession.Read) must reflect that rather than running forever.
	res := s.Wait()
	require.True(t, res.Killed, "an abandoned interactive session must not run forever")
	require.Contains(t, res.Reason, "idle",
		"the reason should say WHICH watchdog fired, not just that something killed it")
}

// spec.Cwd and spec.Env were tested for Run (TestRunRunsInRequestedDirectory,
// TestRunSetsJobEnvironment) but never for startPTY, which builds cmd the
// same way but is a separate code path.
func TestPTYRunsInRequestedDirectoryWithRequestedEnv(t *testing.T) {
	dir := t.TempDir()
	s, err := startPTY(context.Background(), JobSpec{
		Command:      []string{"/bin/sh"},
		Cwd:          dir,
		Env:          map[string]string{"RC_PTY_TEST": "pty-env-value"},
		GraceCeiling: ptyGraceCeiling,
	})
	require.NoError(t, err)
	defer s.Close()

	_, err = s.Write([]byte("pwd; echo $RC_PTY_TEST\n"))
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	var got strings.Builder
	buf := make([]byte, 1024)
	for time.Now().Before(deadline) {
		n, rerr := s.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
			if strings.Contains(got.String(), "pty-env-value") {
				require.Contains(t, got.String(), dir, "cwd was not the requested directory")
				return
			}
		}
		if rerr != nil {
			break
		}
	}
	t.Fatalf("never saw expected cwd/env output; got: %q", got.String())
}

// parseProcStat pins the "locate fields from the last ')'" rule against
// synthetic /proc/<pid>/stat contents, since exercising it for real only ever
// happens to touch the common case (a comm field with no spaces or parens in
// it), and pins the state character, which is what tells a zombie from a
// process that is genuinely still holding the device.
func TestParseProcStat(t *testing.T) {
	cases := []struct {
		name      string
		stat      string
		wantState byte
		wantPgid  int
		wantSid   int
		wantOK    bool
	}{
		{
			name:      "ordinary comm",
			stat:      "12345 (sh) S 1 12345 12345 34816 12345 4194304 10 0 0 0 0 0 0 0 20 0 1 0\n",
			wantState: 'S',
			wantPgid:  12345,
			wantSid:   12345,
			wantOK:    true,
		},
		{
			name:      "comm containing spaces and parens",
			stat:      "999 (my (weird) cmd) S 100 200 300 34816 300 4194304 10 0 0 0 0 0 0 0 20 0 1 0\n",
			wantState: 'S',
			wantPgid:  200,
			wantSid:   300,
			wantOK:    true,
		},
		{
			// The whole reason state is parsed: this entry is a member of
			// group 200 as far as kill(2) is concerned, and holds nothing.
			name:      "zombie is reported as such",
			stat:      "999 (sleep) Z 100 200 300 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0\n",
			wantState: 'Z',
			wantPgid:  200,
			wantSid:   300,
			wantOK:    true,
		},
		{
			name:   "truncated line has too few fields after comm",
			stat:   "999 (sh) S 100\n",
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, ok := parseProcStat([]byte(tc.stat))
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantState, st.state)
				require.Equal(t, tc.wantPgid, st.pgid)
				require.Equal(t, tc.wantSid, st.sid)
			}
		})
	}
}

// childPIDRe matches the numeric PID printed by "echo child:$!". A plain
// strings.Cut on "child:" would instead match the PTY's own local echo of
// the literal input line ("child:$!", unexpanded) before the shell ever
// evaluates it, so this looks specifically for digits.
var childPIDRe = regexp.MustCompile(`child:(\d+)`)

// waitForBackgroundChildPID reads from s until it sees a line of the form
// "child:<pid>", bounded by a deadline; no sleeps, just polling on real I/O.
func waitForBackgroundChildPID(t *testing.T, s *ptySession) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got strings.Builder
	buf := make([]byte, 1024)
	for time.Now().Before(deadline) {
		n, err := s.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
			if m := childPIDRe.FindStringSubmatch(got.String()); m != nil {
				pid, perr := strconv.Atoi(m[1])
				require.NoError(t, perr)
				return pid
			}
		}
		if err != nil {
			break
		}
	}
	t.Fatalf("never saw the backgrounded child's pid; got: %q", got.String())
	return 0
}

// processAlive probes for a process's existence without signalling it.
// kill(pid, 0) is a pure existence/permission check: ESRCH means gone, EPERM
// means it exists but isn't ours to signal (still alive), anything else is
// unexpected and fails the test loudly rather than being read as "dead".
func processAlive(t *testing.T, pid int) bool {
	t.Helper()
	err := syscall.Kill(pid, 0)
	switch {
	case err == nil:
		return true
	case errors.Is(err, syscall.ESRCH):
		return false
	case errors.Is(err, syscall.EPERM):
		return true
	default:
		t.Fatalf("unexpected error probing pid %d: %v", pid, err)
		return false
	}
}
