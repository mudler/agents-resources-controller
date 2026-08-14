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

	slog.Info("hook ran", "event", event, "device", deviceID, "job", jobID, "path", path,
		"exit_code", res.ExitCode, "killed", res.Killed, "output", out)

	switch {
	case res.Err != nil:
		return out, fmt.Errorf("hook %s: %w", path, res.Err)
	case res.Killed:
		reason := res.Reason
		if reason == "" {
			reason = "killed"
		}
		return out, fmt.Errorf("hook %s: %s", path, reason)
	case res.ExitCode != 0:
		return out, fmt.Errorf("hook %s: exited %d", path, res.ExitCode)
	}
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
// hook at all (a no-op skip vs. an actual attempt), its output, and whether
// it failed.
type acquireResult struct {
	ranHook bool
	output  string
	err     error
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
		return acquireResult{}
	}
	if spec.onAcquire == "" {
		// Nothing to run, but the device is now considered held so a
		// configured on_release still fires once it goes idle.
		st.held = true
		return acquireResult{}
	}

	out, err := w.runHook(ctx, spec.onAcquire, "acquire", deviceID, jobID, submitter, spec.timeout)
	if err != nil {
		return acquireResult{ranHook: true, output: out, err: err}
	}
	st.held = true
	return acquireResult{ranHook: true, output: out}
}

// scheduleRelease is called once a job on this device ends, for any outcome.
// It arms a timer that runs the on_release hook after spec.releaseLinger —
// UNLESS acquireDevice cancels it first because another assignment for the
// same device arrived in the meantime, in which case the device was never
// actually idle and no release ever fires for that gap. jobID/submitter
// identify the job that triggered this particular scheduling; if the timer
// is later superseded by a subsequent job ending, that job's identity
// replaces these for the eventual release.
func (w *Worker) scheduleRelease(deviceID string, spec hookSpec, jobID, submitter string) {
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
	if st.timer != nil {
		st.timer.Stop()
	}
	st.gen++
	gen := st.gen
	st.pendingJobID = jobID
	st.pendingSubmitter = submitter
	st.timer = time.AfterFunc(spec.releaseLinger, func() {
		w.fireRelease(deviceID, spec, gen)
	})
}

// fireRelease runs once a release linger has elapsed uninterrupted. gen
// pins it to the specific scheduling call that armed it: if a newer
// acquire or release scheduling happened in between arming and firing,
// this generation is stale and fireRelease does nothing — the newer call
// already took over (or will take over) responsibility for this device.
//
// Deliberately uses context.Background(), not any request or job context:
// this runs off a bare timer goroutine, nothing hands it a context, and a
// worker shutdown must not be what decides whether the release hook gets to
// run — if it doesn't finish before the process exits, the crash-safety
// startup pass (runStartupReleaseHooks) covers it on the next start, which
// is exactly why that pass requires the hook to be idempotent.
func (w *Worker) fireRelease(deviceID string, spec hookSpec, gen int64) {
	st := w.hookState(deviceID)
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.gen != gen {
		return // superseded before this fired
	}
	st.timer = nil
	jobID, submitter := st.pendingJobID, st.pendingSubmitter

	if spec.onRelease != "" {
		out, err := w.runHook(context.Background(), spec.onRelease, "release", deviceID, jobID, submitter, spec.timeout)
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
