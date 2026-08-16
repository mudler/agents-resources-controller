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
	s, err := startPTY(context.Background(), JobSpec{Command: []string{"/bin/sh"}})
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
	s, err := startPTY(context.Background(), JobSpec{Command: []string{"/bin/sh"}})
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
func TestPTYKillsTheWholeProcessGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s, err := startPTY(ctx, JobSpec{Command: []string{"/bin/sh"}})
	require.NoError(t, err)

	_, err = s.Write([]byte("sleep 300 & echo child:$!\n"))
	require.NoError(t, err)
	childPID := waitForBackgroundChildPID(t, s)
	require.True(t, processAlive(t, childPID), "sanity: the backgrounded sleep must actually be running before we test that cancellation kills it")

	cancel()
	res := s.Wait()
	require.True(t, res.Killed, "cancelling the context must kill the session")

	require.Eventually(t, func() bool {
		return !processAlive(t, childPID)
	}, 5*time.Second, 50*time.Millisecond,
		"backgrounded child %d survived cancellation — killing the shell's own process group is not enough, job control gave the child its own", childPID)
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
