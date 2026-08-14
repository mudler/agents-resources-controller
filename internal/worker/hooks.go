package worker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// hookSpec is the resolved (defaults-applied) hook configuration for one
// device, derived once from Config at New().
type hookSpec struct {
	onAcquire     string
	onRelease     string
	timeout       time.Duration
	releaseLinger time.Duration
}

// configured reports whether this device has anything for the hook machinery
// to do at all. A device with neither hook set never needs a deviceHookState
// or a place in Worker.hooks in the first place.
func (h hookSpec) configured() bool {
	return h.onAcquire != "" || h.onRelease != ""
}

// deviceHookState is the worker-side "held" bit for one device: whether the
// device is currently considered acquired (its on_acquire hook has run, or
// would have, and its on_release hook has not run since). This is
// per-device, in-memory, process-local state — it is what lets a burst of
// back-to-back jobs on the same device produce exactly one acquire and one
// release instead of one pair per job.
type deviceHookState struct {
	mu    sync.Mutex
	held  bool
	timer *time.Timer
	// gen is bumped every time the pending release timer is superseded
	// (cancelled by a new acquire, or replaced by a later release
	// schedule). A fireRelease goroutine compares the generation it was
	// launched with against the current one before doing anything, so a
	// timer that fires in the same instant it is being cancelled can never
	// run the release hook for a device that was, by then, already
	// reacquired.
	gen int64
	// epoch identifies WHICH job currently owns the device. It is bumped by
	// every acquireDevice call that leaves the device held — including the
	// one that skips the hook because an earlier job still holds it, since
	// that call is precisely what transfers ownership to the newer job. A
	// job carries the epoch its own acquire returned and hands it back at
	// scheduleRelease; a mismatch means a later job owns the device now and
	// this job's release is void.
	//
	// gen cannot serve this purpose: gen distinguishes one *timer* from
	// another (a stale AfterFunc firing as it is cancelled), and it cannot
	// see the inversion where a finished job arms a release AFTER a
	// subsequent job has already acquired — at that moment there is no timer
	// to be stale, and st.held is true for a reason that has nothing to do
	// with the job doing the arming.
	epoch int64
	// pendingJobID/pendingSubmitter identify the job whose end scheduled
	// the currently pending release timer, so the release hook's
	// RC_JOB_ID/RC_SUBMITTER reflect the job that actually triggered it.
	pendingJobID     string
	pendingSubmitter string
}

// hookOutputTailBytes bounds how much of a hook's combined output rides
// along in a failure report to the controller — enough to show why from `rc
// ps` without shipping unbounded output over a call meant to report one
// job's outcome. The full output is always logged locally regardless.
const hookOutputTailBytes = 4 << 10

func tailOutput(s string) string {
	if len(s) <= hookOutputTailBytes {
		return s
	}
	return s[len(s)-hookOutputTailBytes:]
}

// hookEnv builds the environment a hook receives on top of the worker's own
// (Run always starts from os.Environ()): RC_EVENT/RC_DEVICE/RC_JOB_ID/
// RC_SUBMITTER, plus CUDA_VISIBLE_DEVICES derived the same way job execution
// derives it — reusing cudaDeviceIndex rather than duplicating that logic.
func hookEnv(event, deviceID, jobID, submitter string) map[string]string {
	env := map[string]string{
		"RC_EVENT":     event,
		"RC_DEVICE":    deviceID,
		"RC_JOB_ID":    jobID,
		"RC_SUBMITTER": submitter,
	}
	if idx, ok := cudaDeviceIndex(deviceID); ok {
		env["CUDA_VISIBLE_DEVICES"] = idx
	}
	return env
}

// runHook executes one hook script under the same process-group supervision
// a job gets (Run, in exec.go): its own process group, killed whole on
// timeout, combined stdout+stderr captured. A non-zero exit or a timeout
// (the process group gets killed exactly like an over-budget job's would) is
// reported as an error; the output is always returned so the caller can log
// it and, on failure, tail it into whatever the controller is told.
func (w *Worker) runHook(ctx context.Context, path, event, deviceID, jobID, submitter string, timeout time.Duration) (string, error) {
	var buf bytes.Buffer
	res := Run(ctx, JobSpec{
		Command:    []string{path},
		Env:        hookEnv(event, deviceID, jobID, submitter),
		MaxRuntime: timeout,
	}, &buf)
	out := buf.String()

	var err error
	switch {
	case res.Err != nil:
		err = fmt.Errorf("hook %s: %w", path, res.Err)
	case res.Killed:
		reason := res.Reason
		if reason == "" {
			reason = "killed"
		}
		err = fmt.Errorf("hook %s: %s", path, reason)
	case res.ExitCode != 0:
		err = fmt.Errorf("hook %s: exited %d", path, res.ExitCode)
	}

	// A hook that did its job has nothing to say at INFO on every single
	// acquire and release — an operator's stop-LocalAI script can be chatty,
	// and on a busy box that output is pure noise in the worker's log. It is
	// kept at debug so it can be turned back on when a hook misbehaves, and
	// logged at error, in full, whenever the hook actually failed: that is
	// the one case where the output is the diagnosis.
	if err != nil {
		slog.Error("hook failed", "event", event, "device", deviceID, "job", jobID, "path", path,
			"exit_code", res.ExitCode, "killed", res.Killed, "err", err, "output", out)
		return out, err
	}
	slog.Debug("hook ran", "event", event, "device", deviceID, "job", jobID, "path", path,
		"exit_code", res.ExitCode, "output", out)
	return out, nil
}

// hookState returns this device's in-memory held/linger state, creating it
// on first use. Worker.hooksMu guards only the map lookup/insert; the
// returned state's own mutex guards everything about that one device from
// then on, so devices never contend with each other.
func (w *Worker) hookState(deviceID string) *deviceHookState {
	w.hooksMu.Lock()
	defer w.hooksMu.Unlock()
	st, ok := w.deviceHooks[deviceID]
	if !ok {
		st = &deviceHookState{}
		w.deviceHooks[deviceID] = st
	}
	return st
}

// acquireResult is what acquireDevice decided: whether it ran the on_acquire
// hook at all (a no-op skip vs. an actual attempt), its output, whether it
// failed, and the acquire epoch now in force for the device — the caller's
// proof, when it later releases, that it is still the device's owner.
type acquireResult struct {
	ranHook bool
	output  string
	err     error
	// epoch is the device's acquire epoch established (or, when the hook was
	// skipped because the device was already held, transferred) by this
	// call. It must be handed back to scheduleRelease unchanged. Zero on
	// failure, and for a device with no hooks configured at all, where
	// scheduleRelease is a no-op regardless.
	epoch int64
}

// acquireDevice is called before a job touches its device. If the device is
// not currently held, it runs the on_acquire hook (if any) and marks the
// device held on success. If the device IS already held — an earlier job in
// the same back-to-back run acquired it and its release linger has not yet
// fired — the acquire hook is skipped entirely and any pending release timer
// is cancelled: the device was never actually released, so re-running
// on_acquire would be wrong (and, worse, would race the operator's own
// idempotent-but-not-instant restart of the service on_release starts).
func (w *Worker) acquireDevice(ctx context.Context, deviceID string, spec hookSpec, jobID, submitter string) acquireResult {
	if !spec.configured() {
		return acquireResult{}
	}
	st := w.hookState(deviceID)
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.timer != nil {
		st.timer.Stop()
		st.timer = nil
	}
	st.gen++ // invalidate any fireRelease race from the timer just stopped

	if st.held {
		// The device was never released, so no hook runs — but THIS job is
		// the device's owner from here on, and the epoch says so. Whatever
		// held it before (a job whose own scheduleRelease may still be
		// stuck behind a retrying terminal report) can no longer release it.
		st.epoch++
		return acquireResult{epoch: st.epoch}
	}
	if spec.onAcquire == "" {
		// Nothing to run, but the device is now considered held so a
		// configured on_release still fires once it goes idle.
		st.held = true
		st.epoch++
		return acquireResult{epoch: st.epoch}
	}

	out, err := w.runHook(ctx, spec.onAcquire, "acquire", deviceID, jobID, submitter, spec.timeout)
	if err != nil {
		return acquireResult{ranHook: true, output: out, err: err}
	}
	st.held = true
	st.epoch++
	return acquireResult{ranHook: true, output: out, epoch: st.epoch}
}

// scheduleRelease is called once a job on this device ends, for any outcome.
// It arms a timer that runs the on_release hook after spec.releaseLinger —
// UNLESS acquireDevice cancels it first because another assignment for the
// same device arrived in the meantime, in which case the device was never
// actually idle and no release ever fires for that gap. jobID/submitter
// identify the job that triggered this particular scheduling; if the timer
// is later superseded by a subsequent job ending, that job's identity
// replaces these for the eventual release.
//
// epoch is what the job's own acquireDevice returned. A job's end says
// nothing about a device it no longer owns: if the epoch has moved on, a
// later job acquired the device in the meantime — routinely, when this job's
// terminal report response was lost and reportTerminalWithRetry burned its
// retry budget while the controller had already freed the device and handed
// the next job out — and arming anything here would restart the operator's
// service on top of a running job. That release is simply void; the owner
// that superseded it will release when it ends.
func (w *Worker) scheduleRelease(deviceID string, spec hookSpec, jobID, submitter string, epoch int64) {
	if !spec.configured() {
		return
	}
	st := w.hookState(deviceID)
	st.mu.Lock()
	defer st.mu.Unlock()

	if !st.held {
		// The acquire hook never actually succeeded (or there was nothing
		// to acquire in the first place) — nothing to release.
		return
	}
	if st.epoch != epoch {
		slog.Info("release skipped: the device was re-acquired by a later job",
			"device", deviceID, "job", jobID, "job_epoch", epoch, "device_epoch", st.epoch)
		return
	}
	if st.timer != nil {
		st.timer.Stop()
	}
	st.gen++
	gen := st.gen
	st.pendingJobID = jobID
	st.pendingSubmitter = submitter
	st.timer = time.AfterFunc(spec.releaseLinger, func() {
		w.fireRelease(context.Background(), deviceID, spec, gen)
	})
}

// fireRelease runs once a release linger has elapsed uninterrupted. gen
// pins it to the specific scheduling call that armed it: if a newer
// acquire or release scheduling happened in between arming and firing,
// this generation is stale and fireRelease does nothing — the newer call
// already took over (or will take over) responsibility for this device.
//
// The timer path passes context.Background(): a job's own context ending is
// what triggered the release in the first place, so it must not be what
// cancels it. The shutdown drain (drainPendingReleases) passes a bounded
// context instead, because there it is the drain's own budget — not the
// worker's liveness — that decides how long a slow hook may hold up the
// exit. Either way, a release that never finishes is covered by the
// crash-safety startup pass (runStartupReleaseHooks) on the next start,
// which is exactly why that pass requires the hook to be idempotent.
func (w *Worker) fireRelease(ctx context.Context, deviceID string, spec hookSpec, gen int64) {
	st := w.hookState(deviceID)
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.gen != gen {
		return // superseded before this fired
	}
	st.timer = nil
	jobID, submitter := st.pendingJobID, st.pendingSubmitter

	if spec.onRelease != "" {
		out, err := w.runHook(ctx, spec.onRelease, "release", deviceID, jobID, submitter, spec.timeout)
		if err != nil {
			// Release failure is never a scheduling problem: the device is
			// genuinely free (the job that held it is long done), so it
			// stays in the pool. Log loudly — an operator's service being
			// down is their problem to notice, and this is the only place
			// that will ever say so.
			slog.Error("release hook failed; device remains schedulable", "device", deviceID, "err", err, "output_tail", tailOutput(out))
		}
	}
	st.held = false
}

// releaseDrainBudget bounds the whole shutdown drain: every pending release
// across every device, together. It is deliberately well under a single
// device's default hook timeout (60s): a shutdown already spends up to
// shutdownGrace waiting on jobs, so the drain has to be a known, small
// addition to that rather than a second open-ended wait. A hook that
// does not finish inside it is killed like any over-budget job's process
// group and left to the next start's reconciliation pass — the same
// crash-safety net that has always covered a worker that died mid-linger.
//
// A var, not a const, so tests can shrink it.
var releaseDrainBudget = 20 * time.Second

// drainPendingReleases fires every armed-but-not-yet-fired release, now,
// without waiting out the remaining linger. It is the shutdown counterpart
// to the timers scheduleRelease arms: those live on bare time.AfterFunc
// goroutines that nothing waits for, so a clean `systemctl stop rc-worker`
// after a job would otherwise return with the operator's inference server
// still stopped and no job running — a state only a restart heals. Stopping
// the worker should leave the node the way the operator expects to find it.
//
// Devices with nothing pending are skipped, so a worker with no hooks (or
// none armed) pays nothing at shutdown.
func (w *Worker) drainPendingReleases(budget time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	w.hooksMu.Lock()
	ids := make([]string, 0, len(w.deviceHooks))
	for id := range w.deviceHooks {
		ids = append(ids, id)
	}
	w.hooksMu.Unlock()

	for _, deviceID := range ids {
		st := w.hookState(deviceID)
		st.mu.Lock()
		if st.timer == nil {
			st.mu.Unlock()
			continue // nothing pending, or its release already ran
		}
		st.timer.Stop()
		st.timer = nil
		// Bump the generation before firing: a timer that fired in the same
		// instant we stopped it is now stale and will no-op instead of
		// running the release hook a second time behind us.
		st.gen++
		gen := st.gen
		st.mu.Unlock()

		slog.Info("firing pending release before shutdown", "device", deviceID)
		w.fireRelease(ctx, deviceID, w.hooks[deviceID], gen)
	}
}
