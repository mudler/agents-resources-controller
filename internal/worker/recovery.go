package worker

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"strconv"
	"syscall"

	"github.com/mudler/resource-controller/internal/model"
)

// jobEnvPrefix is the marker every process this worker has ever started for
// a job carries: execute() puts RC_JOB_ID in the environment of the job it
// spawns, and a child inherits its parent's environment, so the whole tree of
// a running job is stamped with it. Lifecycle hooks (hooks.go) and verify
// scripts (verify.go) are given the same variable, so a hung release hook or
// a wedged verify script left over from a previous run is found here too —
// which is right: either can still be touching the device.
//
// The trailing '=' is part of it, and the value must be non-empty: lifecycle
// hooks are run with RC_JOB_ID set to "" when no job is behind them (see
// runStartupReleaseHooks), and a bare prefix match would count one of those
// as a survivor.
const jobEnvPrefix = "RC_JOB_ID="

// procScan is liveProcExists (pty.go), indirected through a var for exactly
// one reason: the isolated case this feature exists for is a PID namespace
// containing a single process, and creating one in a test needs privileges
// the test runner does not have (CLONE_NEWPID, plus a remounted /proc, plus
// an unprivileged user namespace that several distributions now block
// outright). Swapping the scan lets the DECISION be tested against a
// namespace of one; the scan itself is tested against real /proc by the
// survivor tests, which can create a real, findable process without any
// privilege at all.
//
// Same rationale as terminalReportAttemptTimeout being a var: a bound (or
// here, a boundary) that cannot be reached in a test is one nobody ever
// proves anything about.
var procScan = liveProcExists

// recoveryProof is what this worker can prove, at registration, about
// processes left behind by a job its previous incarnation was running. It is
// the cheap sibling of BootID(): both answer "can anything from before still
// be holding a device?", one by having rebooted the machine and one by
// looking at what is actually running.
//
// The deployment mode is DERIVED here, never declared. There is deliberately
// no "i am containerised" configuration option, because a flag that must
// match reality eventually will not, and the dangerous direction fails
// silently — a worker wrongly claiming isolation while running under systemd
// would return a device whose previous job is still training on it. What is
// reported is what was observed, and the observation is conservative by
// construction: a false negative costs a manual clear, and a false positive
// is not reachable by misconfiguration because there is nothing to
// misconfigure.
func (w *Worker) recoveryProof() model.RecoveryProof {
	return w.recoveryProofFor(os.Getpid())
}

// recoveryProofFor is recoveryProof with this process's own pid passed in
// rather than looked up, so a test can put the worker where the interesting
// answers are — PID 1 of a namespace — without needing to actually be it.
func (w *Worker) recoveryProofFor(self int) model.RecoveryProof {
	if w.cfg.RequireManualClear {
		// The one setting, and it only ever points one way: it suppresses
		// this worker's claims so its devices keep today's behaviour (an
		// admin's `rc clear`, or a proven reboot, and nothing else). There is
		// no setting that forces auto-recovery — that is the only direction
		// in which a misconfiguration hands out an occupied GPU.
		//
		// Logged, because a silent worker and a silenced one look identical
		// from the controller and an operator wondering why a device never
		// comes back deserves the answer on the worker's own first line.
		slog.Info("require_manual_clear is set: this worker claims nothing about leftover processes, so its quarantined devices wait for an operator",
			"host", w.cfg.Host)
		return model.RecoveryProof{}
	}

	if isolatedPIDNamespace(self) {
		slog.Info("registering as isolated: this worker is PID 1 in a namespace containing only itself, so no process from a previous job can exist",
			"host", w.cfg.Host)
		return model.RecoveryProof{Isolated: true}
	}

	checked, found := survivorsFromPreviousRun(self)
	switch {
	case !checked:
		slog.Info("could not determine whether processes from a previous run survive; claiming nothing, so quarantined devices wait for an operator",
			"host", w.cfg.Host)
	case found:
		slog.Warn("a process from a previous run of this worker is still alive; its device stays quarantined",
			"host", w.cfg.Host)
	}
	return model.RecoveryProof{SurvivorsChecked: checked, SurvivorsFound: found}
}

// isolatedPIDNamespace reports whether self is PID 1 in a PID namespace that
// contains nothing but itself — the containerised (pod/DaemonSet)
// deployment, where jobs run as children of this worker inside its own
// container. When that container restarts, the PID namespace is destroyed
// and every process from the previous job dies with it; the GPU is released
// because the processes holding it no longer exist. The restart IS the
// proof, and this is how the worker establishes it.
//
// This is strictly better than probing the hardware for the answer. It is
// definitive rather than heuristic, and it works where probing cannot:
// nvidia-smi reports [N/A] for total memory on the GB10 and the Thor, and
// orin has no nvidia-smi at all, so a VRAM-based check would be reading a
// number two boxes cannot report and the third cannot produce.
//
// Both halves are required. Being PID 1 alone is not enough — a container
// image whose entrypoint is this worker but which also runs an sshd, or a
// pod with a shared process namespace, is PID 1 with company, and one of
// that company could be a job from before. Nor is an empty scan alone
// enough: on a host, "no other processes" is not a state that ever occurs,
// and if it somehow appeared it would mean /proc was lying rather than that
// the machine was idle.
//
// A /proc that cannot be read at all proves nothing and is reported as such:
// "found nothing" and "could not look" are opposite answers, which is why
// liveProcExists returns the two separately.
func isolatedPIDNamespace(self int) bool {
	if self != 1 {
		return false
	}
	other, scanned := procScan(func(st procStat) bool { return st.pid != self })
	return scanned && !other
}

// survivorsFromPreviousRun looks for processes this worker's previous
// incarnation started and did not take with it — the host/systemd case,
// where jobs are children of the worker on a shared machine and a worker
// restart does NOT kill them: they are reparented to init and may still be
// running, and one of them may still be holding a GPU.
//
// checked reports whether the question could be answered at all; found is
// the answer. The two are separate because a worker that could not look must
// say so rather than report a comforting "nothing found" — silence is the
// only honest output when the scan was incomplete, and silence means the
// device keeps today's behaviour and waits for an operator.
//
// A survivor is identified by RC_JOB_ID in its environment, which every
// process a job spawns inherits (see jobEnvPrefix). That is deliberately not
// a process-group or session check: job control moves a backgrounded command
// into its own group, an interactive `--tty` job gets its own session, and a
// tool like tmux calls setsid(2) and leaves the session it was started in
// entirely — all of which escape a group- or session-scoped scan (see
// sessionTarget's own note on that residual), while none of them escape the
// environment they were started with. It also needs no state persisted
// between worker restarts, which is the other reason to prefer it: a record
// of what to look for is only as good as the last write that made it, and a
// missed write would produce exactly the false "nothing survived" this must
// never produce.
//
// Two limitations, stated rather than papered over:
//
//   - A process that re-execs with a scrubbed environment (a daemon a job
//     starts through systemd-run, say) loses the marker and is invisible
//     here. It is invisible to the worker's own kill path too, and the
//     backstop for both is the same: the post-job verify probes, which
//     quarantine a device whose VRAM is still pinned.
//   - A worker that cannot read another process's environ — which is every
//     process it does not own, so effectively "a worker not running as
//     root" — reports checked=false and proves nothing. That is correct and
//     deliberate: a worker that can only see half the process table has not
//     established that the other half is empty.
func survivorsFromPreviousRun(self int) (checked, found bool) {
	// blind records whether any process could not be inspected. It is only
	// consulted when nothing was found: a scan that positively identifies a
	// survivor is complete regardless of what it could not read.
	var blind bool
	live, scanned := procScan(func(st procStat) bool {
		if st.pid == self {
			// This worker's own process. It carries no RC_JOB_ID of its own
			// — but it does if an operator started it from inside a job
			// (`rc run rc worker`), and finding itself would quarantine the
			// whole host forever.
			return false
		}
		match, err := hasJobEnv(st.pid)
		if err != nil {
			blind = true
			return false
		}
		return match
	})
	switch {
	case !scanned:
		return false, false
	case live:
		return true, true
	case blind:
		return false, false
	default:
		return true, false
	}
}

// hasJobEnv reports whether the process's environment carries a non-empty
// RC_JOB_ID, which marks it as something a worker started for a job.
func hasJobEnv(pid int) (bool, error) {
	id, err := jobEnvID(pid)
	return id != "", err
}

// jobEnvID returns the value of RC_JOB_ID in the process's environment, or
// "" if it carries none. It is hasJobEnv's own implementation, widened to
// hand back the id itself, because the post-job sweep (sweep.go) has to
// answer a stricter question than "is this one of ours": it has to answer
// "is this THIS job's", and killing another job's processes because their
// ids share a prefix would be a worse bug than the leak the sweep fixes.
// One reader, one parse, one place to be wrong.
//
// A process that has exited by the time we get to it is not an error and not
// a survivor: it holds nothing. Anything else — most realistically EACCES on
// a process this worker does not own — IS an error, because it is the
// difference between "this is not one of ours" and "I could not tell".
//
// A kernel thread reads back as zero bytes rather than an error (it has no
// mm for the kernel to read an environment out of), so it falls out as a
// plain non-match without needing a special case.
func jobEnvID(pid int) (string, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/environ")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			return "", nil
		}
		return "", err
	}
	// environ is NUL-separated, not newline-separated, and the last entry
	// carries a trailing NUL — so an empty final element is normal and must
	// not be mistaken for an empty variable.
	for _, entry := range bytes.Split(data, []byte{0}) {
		if v, ok := bytes.CutPrefix(entry, []byte(jobEnvPrefix)); ok && len(v) > 0 {
			return string(v), nil
		}
	}
	return "", nil
}
