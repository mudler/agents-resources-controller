package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// verifyReasonPrefix marks a Reason as coming from runVerify rather than
// from a lifecycle hook's own free-text failure. A later task reads this
// exact prefix back off a device's quarantine reason (set through the same
// free-text /v1/devices/{id}/fault endpoint a hook failure uses) to tell a
// verify-sourced fault apart from a hook-sourced one. Changing this string
// is a breaking change for that consumer, not a cosmetic one.
const verifyReasonPrefix = "verify failed: "

// VerifyResult is the outcome of one runVerify pass over VerifyDir.
type VerifyResult struct {
	// OK is true when every verify script exited zero, or there were none
	// to run (including a VerifyDir that does not exist at all — the
	// feature is simply off on a host that ships no scripts).
	OK bool
	// Reason is empty when OK. Otherwise it always starts with
	// verifyReasonPrefix, followed by the first failing script's name and
	// the tail of its stderr — everything an operator needs to see WHY a
	// device was quarantined from `rc ps`/`rc devices`, and everything a
	// later task needs to recognise this fault as verify-sourced.
	Reason string
}

// runVerify runs every executable in cfg.VerifyDir, in name order, after a
// job's process tree is already gone (Run has returned) and before the
// terminal report that would otherwise let the controller hand this device
// to the next job. This ordering is the entire point of the feature: a
// verify script exists to catch "the job exited but VRAM is still pinned",
// and that is only worth checking BEFORE the device looks free again.
//
// Modelled closely on probe.go's gatherLabels/runProbe, which already solve
// the same mechanical problems — name-order execution, each script through
// Run for its own process group and a bounded lifetime, a missing directory
// treated as "nothing to do". The differences follow from what a verify
// script is FOR, not from a probe: its EXIT CODE is the result (a probe's
// stdout is), its STDERR is the reason an operator reads (a probe's stdout
// is data, its stderr is incidental), and a failure is not silently skipped
// the way a broken probe is — it fails the whole pass. Every script still
// runs regardless of an earlier one's outcome, exactly as gatherLabels keeps
// going after one probe fails; only the FIRST failure's detail is kept as
// Reason, since one is already enough to quarantine the device and a
// second failing script's stderr adds nothing an operator needs that the
// first one didn't already say.
//
// Unlike a probe, whose failure is invisible-by-design (a dropped label is
// harmless; gatherLabels' whole contract is "keep whatever succeeded"), a
// verify script that cannot even be inspected must fail loud, not skip
// silently: OK=true is read as "this device is clean", so anything that
// stops a script from actually running — including a stat error, e.g. a
// dangling symlink left behind by a package upgrade that removed a script's
// target — has to be treated exactly like a script that ran and said no,
// never like a script that was never there. gatherLabels' own analogous stat
// branch (probe.go) can afford to just log and move on, because a probe
// that never ran costs a label, not a false "verified clean".
//
// The whole pass is additionally bounded by cfg.VerifyPassBudget, not just
// each script's own VerifyTimeout — see that field's doc comment for why —
// and hitting that budget is treated the same way a stat error is: the pass
// FAILS, with a reason that names the budget, rather than quietly returning
// whatever partial result had accumulated so far. Stopping there (instead of
// still attempting every remaining script) is deliberate: the budget's own
// purpose is bounding how long this can run, and once it's gone, every
// script still queued would fail the exact same way for the exact same
// reason.
func (w *Worker) runVerify(ctx context.Context, deviceID, jobID string) VerifyResult {
	// The pass-wide budget is the only thing that bounds this call at all:
	// runVerify is invoked with reportCtx (see execute), which is
	// deliberately immune to Start's own shutdown cancellation, so nothing
	// upstream will ever cut this short on its own.
	ctx, cancel := context.WithTimeout(ctx, w.cfg.VerifyPassBudget)
	defer cancel()

	entries, err := os.ReadDir(w.cfg.VerifyDir)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("read verify dir", "dir", w.cfg.VerifyDir, "err", err)
		}
		return VerifyResult{OK: true}
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // execution order is name order, same convention as probe.d

	// The same environment a hook gets, built via hookEnv so the two
	// mechanisms never drift apart: RC_EVENT/RC_DEVICE/RC_JOB_ID/
	// RC_SUBMITTER, plus CUDA_VISIBLE_DEVICES when it resolves. A verify
	// pass always has a job ID in hand (unlike an on_release hook, which
	// may run with none at startup), so RC_JOB_ID here is never blank.
	env := hookEnv("verify", deviceID, jobID, "")

	// failOnce records the pass's first failure, whatever kind, and is a
	// no-op on every call after the first — see the doc comment above for
	// why only the first failure's reason survives.
	result := VerifyResult{OK: true}
	failOnce := func(reason string) {
		if result.OK {
			result = VerifyResult{OK: false, Reason: reason}
		}
	}

	for _, name := range names {
		if ctx.Err() != nil {
			failOnce(budgetExceededReason(w.cfg.VerifyPassBudget))
			break
		}

		path := filepath.Join(w.cfg.VerifyDir, name)
		info, err := os.Stat(path)
		if err != nil {
			// Fail loud, not skip silently — see the doc comment above.
			slog.Error("stat verify script; failing the pass", "script", path, "err", err)
			failOnce(verifyReasonPrefix + name + ": stat: " + err.Error())
			continue
		}
		if info.Mode()&0o111 == 0 {
			continue // not executable: e.g. a README dropped alongside scripts
		}

		stdout := &boundedBuffer{limit: maxProbeOutputBytes}
		stderr := &boundedBuffer{limit: maxProbeOutputBytes}
		res := Run(ctx, JobSpec{
			Command:    []string{path},
			Env:        env,
			MaxRuntime: w.cfg.VerifyTimeout,
			Stderr:     stderr,
		}, stdout)

		if res.Err == nil && !res.Killed && res.ExitCode == 0 {
			continue // this script passed; keep going regardless
		}

		if ctx.Err() != nil {
			// The pass budget, not this script's own outcome, is why this
			// run didn't finish cleanly: attribute the failure to the
			// budget, not to whatever half-finished detail Run captured on
			// its way out, and stop — every script still queued would fail
			// the same way for the same reason.
			failOnce(budgetExceededReason(w.cfg.VerifyPassBudget))
			break
		}

		reason := verifyReasonPrefix + name + ": " + verifyFailureDetail(res)
		if tail := verifyStderrTail(stderr); tail != "" {
			reason += ": " + tail
		}
		failOnce(reason)
	}
	return result
}

// budgetExceededReason renders the Reason used when a verify pass is cut
// short by cfg.VerifyPassBudget rather than by any single script's own
// outcome.
func budgetExceededReason(budget time.Duration) string {
	return fmt.Sprintf("%spass exceeded its %s budget", verifyReasonPrefix, budget)
}

// verifyFailureDetail renders why a verify script's Run result counts as a
// failure: the ordinary case (a non-zero exit), a kill (timeout or
// otherwise, in which case Run's own Reason already says which), or a
// start/wait error from Run itself.
func verifyFailureDetail(res Result) string {
	switch {
	case res.Err != nil:
		return "run: " + res.Err.Error()
	case res.Killed:
		if res.Reason != "" {
			return res.Reason
		}
		return "killed"
	default:
		return fmt.Sprintf("exited %d", res.ExitCode)
	}
}

// maxVerifyStderrTail bounds how much of a failing verify script's stderr is
// folded into Reason. Reason is read by an operator from `rc ps`/`rc
// devices`, not a log file, so it stays short; the script's full stderr
// (still capped separately, at maxProbeOutputBytes, by the boundedBuffer
// Run writes into) is not the point here.
const maxVerifyStderrTail = 200

// verifyStderrTail renders the TAIL of a failing verify script's stderr —
// deliberately the end, not the start (unlike a probe's stderrNote): a
// script that prints context first and its actual complaint last (a common
// shell-script style — "checking X... checking Y... FATAL: 72G still
// allocated") would otherwise have exactly the useful line cut off.
func verifyStderrTail(stderr *boundedBuffer) string {
	s := strings.TrimSpace(stderr.buf.String())
	if s == "" {
		return ""
	}
	if len(s) > maxVerifyStderrTail {
		s = "...(truncated)" + s[len(s)-maxVerifyStderrTail:]
	}
	if stderr.truncated {
		s += " (stderr itself was truncated)"
	}
	return s
}
