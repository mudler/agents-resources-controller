package worker

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startMarked starts a real process carrying RC_JOB_ID=jobID and waits until
// its environment is actually readable in /proc, so a sweep that runs next is
// looking at a process that exists rather than racing one that is still being
// exec'd. It is cleaned up whatever the test does to it.
func startMarked(t *testing.T, jobID string, setsid bool, script string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = append(os.Environ(), "RC_JOB_ID="+jobID)
	// Setsid, when asked for, puts the process exactly where a job's escapee
	// ends up: outside the process group and session anything else would
	// signal, reachable only by the environment marker. It is set INSTEAD of
	// Setpgid, never alongside it — setsid(2) already makes the process the
	// leader of a new group, and the setpgid(2) that would follow it fails
	// with EPERM on a session leader.
	if setsid {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	} else {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid
	t.Cleanup(func() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
	})
	require.Eventually(t, func() bool {
		data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/environ")
		return err == nil && strings.Contains(string(data), "RC_JOB_ID="+jobID+"\x00")
	}, 5*time.Second, 20*time.Millisecond, "the marked process never became visible in /proc")
	return cmd
}

// alive reports whether pid is a real process rather than gone or a zombie.
//
// The zombie half matters here in a way it does not in the external tests: a
// process this file's sweeps kill is a direct CHILD of the test binary, so it
// stays in the process table unreaped until t.Cleanup gets round to Wait —
// and kill(pid, 0) answers "yes, something is there" for a zombie exactly as
// it does for a live process. Probing with a signal would make every "it was
// killed" assertion below fail against perfectly correct code, which is the
// same mistake 337e23d had to undo one level down.
func alive(pid int) bool {
	st, ok := readProcStat(pid)
	return ok && st.state != 'Z'
}

// A worker started from inside a job (`rc run rc worker`) inherits that job's
// RC_JOB_ID, and so does everything that started IT. A sweep for that id
// finding the worker would not merely mislabel something, as the equivalent
// bug in the recovery check would: it signals, so it would kill the worker,
// and then the supervisor that would have restarted it, and the box would
// stop running jobs entirely.
//
// Both halves are asserted against real processes: self is a live process
// carrying the marker, its parent shell is another, and an unrelated process
// carrying the same marker proves the sweep was running at all rather than
// quietly doing nothing.
func TestSweepNeverKillsItselfOrItsAncestors(t *testing.T) {
	jobID := "sweep-self-" + strconv.Itoa(os.Getpid())

	// A parent shell carrying the marker, which starts a marked child and
	// stays alive: the child stands in for the worker, the shell for the
	// ancestor that would have restarted it.
	pidFile := t.TempDir() + "/pid"
	parent := startMarked(t, jobID, false,
		"sleep 300 & echo $! > "+pidFile+"; wait")

	var self int
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(pidFile)
		if err != nil {
			return false
		}
		self, err = strconv.Atoi(strings.TrimSpace(string(data)))
		return err == nil && self > 0
	}, 5*time.Second, 20*time.Millisecond)

	victim := startMarked(t, jobID, true, "exec sleep 300")

	rep := sweepJobSurvivors(jobID, self)

	require.True(t, alive(self), "the sweep killed the process it was told was its own")
	require.True(t, alive(parent.Process.Pid), "the sweep killed its own ancestor")
	require.NotContains(t, rep.Found, self)
	require.NotContains(t, rep.Found, parent.Process.Pid)

	// ...and it was not simply inert: the unrelated process carrying the same
	// job id was found and reaped.
	require.Contains(t, rep.Found, victim.Process.Pid)
	require.False(t, alive(victim.Process.Pid))
}

// A survivor that ignores SIGTERM must still go. The sweep escalates exactly
// the way the process-group teardown does — ask, wait out the grace window,
// then insist — and the elapsed time is asserted because it is the only thing
// that distinguishes "SIGKILL did it" from "it died on the SIGTERM like every
// other test process here does".
//
// The shell traps TERM and keeps forking short sleeps, so the sweep also has
// to cope with a survivor that is producing NEW marked processes while it
// works: a phase that iterated one captured pid list instead of re-scanning
// would leave those children running.
func TestSweepEscalatesToSIGKILLWhenSIGTERMIsIgnored(t *testing.T) {
	jobID := "sweep-stubborn-" + strconv.Itoa(os.Getpid())
	stubborn := startMarked(t, jobID, true, `trap "" TERM; while :; do sleep 0.05; done`)
	pid := stubborn.Process.Pid

	start := time.Now()
	rep := sweepJobSurvivors(jobID, os.Getpid())
	elapsed := time.Since(start)

	require.Contains(t, rep.Found, pid)
	require.GreaterOrEqual(t, elapsed, sweepGrace,
		"a process that ignores SIGTERM must have been given the full grace window before SIGKILL; a shorter run means it was never escalated")
	require.False(t, alive(pid), "a survivor that ignores SIGTERM must still be killed")
	require.Empty(t, rep.Remaining, "nothing was left for an operator to worry about, so nothing may be reported as left")
}

// A process this worker cannot look into — anything it does not own, which is
// every process on the box when the worker is not root — is not a survivor
// and is not proof of a clean device either. The sweep says so: it reports
// Blind, and Clean stays false, so a caller can never read "I found nothing"
// as "there is nothing".
func TestSweepDoesNotClaimACleanBoxWhenAProcessCannotBeInspected(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root: every /proc/<pid>/environ is readable, so the blind case cannot be produced")
	}
	// PID 1 alone is enough: it is init, owned by root, and its environ is
	// unreadable to everyone else.
	rep := sweepJobSurvivors("sweep-blind-"+strconv.Itoa(os.Getpid()), os.Getpid())

	require.True(t, rep.Scanned, "/proc itself was readable")
	require.Empty(t, rep.Found, "nothing on this box carries a job id invented one line ago")
	require.True(t, rep.Blind, "a worker that cannot read part of the process table must say so")
	require.False(t, rep.Clean(), "a sweep that could not see everything has not established that the device is clean")
}

// The empty job id is the one input that could turn this mechanism into a
// machine for killing everything on the box. jobEnvID reports "" both for a
// lifecycle hook run with no job behind it (see runStartupReleaseHooks) and
// for every ordinary process on the machine that has no RC_JOB_ID at all, so
// an id-equality check on "" matches the entire process table. The guard is
// at the front of sweepJobSurvivors and this is what holds it there.
func TestSweepWithNoJobIDSweepsNothing(t *testing.T) {
	hook := startMarked(t, "", false, "exec sleep 300")
	unmarked := exec.Command("sleep", "300")
	require.NoError(t, unmarked.Start())
	t.Cleanup(func() {
		_ = unmarked.Process.Kill()
		_, _ = unmarked.Process.Wait()
	})

	rep := sweepJobSurvivors("", os.Getpid())

	require.Empty(t, rep.Found)
	require.False(t, rep.Scanned, "a sweep that never ran must not claim it looked")
	require.True(t, alive(hook.Process.Pid), "a hook with no job behind it is not a survivor")
	require.True(t, alive(unmarked.Process.Pid), "a process with no RC_JOB_ID at all is not a survivor")
}
