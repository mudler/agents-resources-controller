package worker

import (
	"syscall"
	"time"
)

// sweepGrace is the SIGTERM -> SIGKILL window the sweep gives a survivor,
// deliberately shorter than JobSpec.GraceCeiling's default 10s. It is paid on
// the critical path between two jobs on the same device, and the process it
// is being spent on is by definition one that already outlived the job it
// belonged to: it was never going to be signalled again by anything else, and
// a detached process that has not finished tidying up within two seconds of
// being asked is not going to.
const sweepGrace = 2 * time.Second

// sweepPoll is how often the sweep re-walks /proc while waiting for the
// processes it signalled to actually disappear. Same cadence as
// awaitTargetExit in exec.go, for the same reason: fast enough that the
// common case (a process that dies on the first SIGTERM) costs one poll, slow
// enough that the walk itself is not the load.
const sweepPoll = 20 * time.Millisecond

// SweepReport is what the RC_JOB_ID sweep saw and did, and it exists as its
// own field on Result for one reason, which is the most important sentence in
// this file:
//
// REAPING A SURVIVOR MUST NEVER RELABEL A SUCCESSFUL JOB AS KILLED.
//
// On 2026-08-17 exactly that mistake shipped. A straggler sweep set
// Killed = true on the clean-exit path, and because cmd.Wait() returns the
// instant the last holder of the inherited stdout pipe becomes a zombie, 40
// out of 40 ordinary successful jobs came back reporting themselves killed
// (fixed in 337e23d, "a zombie is not a straggler"). The blast radius was not
// cosmetic: worker.go maps Killed onto model.JobKilled, hooks.go treats a
// killed hook as a failed hook — so an on_acquire hook that backgrounds
// anything refuses the lease — and verify.go treats a killed probe as a
// failed probe, which quarantines the device.
//
// A job that exited 0 and happened to leave a detached process behind is a
// SUCCESSFUL job that left a mess. The mess is reported here and logged by
// execute(); the job's own outcome is untouched. Nothing in this file writes
// to Result.Killed, Result.Reason or Result.ExitCode, and nothing in it
// should ever be made to.
type SweepReport struct {
	// Scanned reports whether /proc could be walked at all. "Found nothing"
	// and "could not look" are opposite answers and are never conflated
	// here — the same distinction liveProcExists draws for recovery.
	Scanned bool
	// Found is every live process that carried this job's exact RC_JOB_ID
	// after the job's process group had been torn down. Empty is the
	// overwhelmingly common case: the group kill already did the work.
	Found []int
	// Remaining is what was still alive after SIGTERM, the grace window and
	// SIGKILL. Non-empty means the kernel is holding a process we cannot
	// remove (uninterruptible sleep in a driver call, most realistically),
	// and the device it is sitting on is still dirty.
	Remaining []int
	// Blind reports that at least one process could not be inspected —
	// EACCES on the /proc/<pid>/environ of a process this worker does not
	// own, i.e. a worker not running as root. A process we could not look at
	// is not evidence of a survivor AND not evidence of a clean box; it is
	// only evidence that the answer here is incomplete.
	Blind bool
}

// Clean reports whether the sweep positively established that nothing of this
// job is left running: it looked, it could see everything it looked at, and
// either found nothing or removed everything it found. An empty Found on its
// own is NOT that claim — a worker that is not root cannot read most of the
// process table, so "I found nothing" and "there is nothing" are different
// sentences and only this one says the second.
func (s SweepReport) Clean() bool {
	return s.Scanned && !s.Blind && len(s.Remaining) == 0
}

// sweepJobSurvivors kills whatever is still alive carrying RC_JOB_ID=jobID
// after the job's process group has already been torn down, and reports what
// it saw. self is the worker's own pid, passed in rather than looked up so a
// test can stand somewhere interesting in the process tree (the same reason
// recoveryProofFor takes one).
//
// WHY THIS EXISTS AT ALL. Run puts each job in its own process group and
// kills that group; startPTY additionally reaches every group in the job's
// session. Neither reaches a process that called setsid(2) — directly, by
// double-forking into a daemon, or through any tool that daemonises itself —
// because setsid(2) is precisely the syscall that leaves both. exec.go has
// always admitted this: it has an outcome string for "stragglers remained
// after exit". On most systems that leak is bounded by container teardown,
// but not here: jobs run INSIDE the worker's own long-lived container, so a
// survivor is cleaned up by nothing. It keeps running, it keeps whatever GPU
// memory it had, and the next job scheduled onto that device gets a dirty
// one — until somebody restarts the pod.
//
// WHY THE ENVIRONMENT IS THE RIGHT HANDLE. Every process a job spawns
// inherits RC_JOB_ID (execute() puts it in the job's environment, and a child
// inherits its parent's). setsid(2), double-forking and daemonising all
// change the process group and the session; none of them clear the
// environment. So a /proc scan for the variable catches exactly the escapees
// the group kill misses, and it needs no state persisted anywhere — the same
// reasoning survivorsFromPreviousRun already relies on for autonomous
// recovery.
//
// THE HONEST RESIDUAL. A process that re-execs with a SCRUBBED environment —
// a daemon started through systemd-run, say, or anything that rebuilds its
// environ from scratch — loses the marker and is invisible to this sweep,
// exactly as it is invisible to the survivor check autonomous recovery uses
// (see survivorsFromPreviousRun's own note). This is a cheaper mechanism than
// the definitive one, not a replacement for it. The backstops, in order: the
// post-job verify probes, which quarantine a device whose VRAM is still
// pinned by something we cannot name; and a per-job cgroup, which kills by
// membership rather than by marker and cannot be escaped by any userspace
// trick at all — that one needs the worker pod to be allowed to create
// cgroups (/sys/fs/cgroup is read-only inside the container today, so mkdir
// fails), which is a pod-spec privilege change and not something this
// mechanism can decide for itself.
func sweepJobSurvivors(jobID string, self int) SweepReport {
	if jobID == "" {
		// A lifecycle hook with no job behind it runs with RC_JOB_ID set to
		// the empty string (see runStartupReleaseHooks), and jobEnvID
		// reports an empty value for every process that has no RC_JOB_ID at
		// all. Sweeping on "" would therefore mean "sweep everything", which
		// is the single worst thing this function could be made to do.
		// Callers opt in with a real job id or they get no sweep.
		return SweepReport{}
	}

	protected := ancestorSet(self)

	found, blind, scanned := jobSurvivorPids(jobID, protected)
	rep := SweepReport{Scanned: scanned, Blind: blind, Found: found}
	if !scanned || len(found) == 0 {
		return rep
	}

	// Same escalation the process-group teardown uses, for the same reason:
	// ask first, because a survivor that flushes a checkpoint on SIGTERM
	// should be allowed to, then insist.
	if remaining := sweepPhase(jobID, protected, syscall.SIGTERM, sweepGrace); len(remaining) > 0 {
		rep.Remaining = sweepPhase(jobID, protected, syscall.SIGKILL, stragglerWait)
	}
	return rep
}

// sweepPhase signals every survivor of jobID with sig and then waits, bounded
// by timeout, for them all to disappear, returning whatever still matches
// when it gives up. Nothing here blocks indefinitely.
//
// It re-scans rather than iterating a fixed pid list because a survivor can
// fork while we are working: a daemon's children inherit RC_JOB_ID too, and a
// list captured once would leave them running. Each pid is signalled once per
// phase — a process already sent SIGTERM is being given its grace window, not
// nagged every 20ms — but a pid seen for the first time is signalled the
// moment it appears.
func sweepPhase(jobID string, protected map[int]struct{}, sig syscall.Signal, timeout time.Duration) []int {
	deadline := time.Now().Add(timeout)
	signalled := make(map[int]struct{})
	for {
		pids, _, scanned := jobSurvivorPids(jobID, protected)
		if scanned && len(pids) == 0 {
			return nil
		}
		for _, pid := range pids {
			if _, done := signalled[pid]; done {
				continue
			}
			signalled[pid] = struct{}{}
			// The error is deliberately ignored: ESRCH means it exited
			// between the scan and the signal, which is the outcome we
			// wanted, and the confirming re-scan above is what decides
			// whether this worked — not the return value of one kill(2).
			_ = syscall.Kill(pid, sig)
		}
		if !time.Now().Before(deadline) {
			return pids
		}
		time.Sleep(sweepPoll)
	}
}

// jobSurvivorPids returns the live processes whose RC_JOB_ID is EXACTLY
// jobID, excluding protected. blind reports that at least one process could
// not be inspected, which is neither a survivor nor proof of a clean box.
//
// The match is on the whole value. A prefix or substring match would be a
// serious bug rather than a sloppy one: this box runs jobs back to back and
// several at once, so "kill everything whose id starts like mine" is a
// mechanism for killing somebody else's training run.
//
// Zombies are excluded upstream by scanLiveProcs, and that is correct here
// too: an exited-but-unreaped process holds no device and has no user-space
// code left to receive a signal. Counting one as a survivor is exactly the
// mistake 337e23d had to undo.
func jobSurvivorPids(jobID string, protected map[int]struct{}) (pids []int, blind, scanned bool) {
	var couldNotLook bool
	pids, scanned = liveProcPids(func(st procStat) bool {
		if _, skip := protected[st.pid]; skip {
			return false
		}
		id, err := jobEnvID(st.pid)
		if err != nil {
			couldNotLook = true
			return false
		}
		return id == jobID
	})
	return pids, couldNotLook, scanned
}

// ancestorSet is the worker's own pid plus every ancestor up to init: the
// processes this sweep must never signal no matter what their environment
// says.
//
// This is not theoretical. A worker started from inside a job — `rc run rc
// worker`, which is how one gets tested — inherits RC_JOB_ID from that job,
// and so does the shell that started it. A sweep for that id would find the
// worker itself and kill it, along with the supervisor that would have
// restarted it, taking down the box's whole capacity to run anything.
// survivorsFromPreviousRun excludes the worker's own pid for the same reason;
// this walks the chain as well because the sweep does not merely report, it
// signals, and a dead ancestor is a dead worker one process later.
//
// A /proc entry that cannot be read stops the walk. That truncates the
// protected set rather than extending it, which sounds like the unsafe
// direction and is not: the entries being walked are this process's own
// ancestors, and on Linux /proc/<pid>/stat is world-readable — a failure here
// means the ancestor has already exited, i.e. has been reparented to init,
// so the chain genuinely ends.
func ancestorSet(self int) map[int]struct{} {
	set := map[int]struct{}{self: {}}
	for pid := self; pid > 0; {
		st, ok := readProcStat(pid)
		if !ok || st.ppid <= 0 {
			break
		}
		if _, seen := set[st.ppid]; seen {
			break // a cycle is impossible in a process tree; refuse to loop on one anyway
		}
		set[st.ppid] = struct{}{}
		pid = st.ppid
	}
	return set
}
