# Resource Controller Stage 4 Implementation Plan

> **STATUS:** SHIPPED. Merged to master (19 commits, reviewed per task) and deployed; the checkboxes below were never maintained during execution.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the controller notice trouble and tell someone — verify a device is genuinely clean before it returns to the pool, push events to a webhook, and let the dashboard act.

**Architecture:** Verify probes run on the worker after a job's process tree is gone and *before* its terminal report, so a device that fails verification is quarantined before the controller can hand it out again. Notifications are a controller-side goroutine with a bounded queue that drops rather than blocks. Dashboard actions reuse the existing kill and clear routes.

**Tech Stack:** Go 1.26, `modernc.org/sqlite` (no cgo), `spf13/cobra`, `stretchr/testify`, stdlib `net/http`, `gopkg.in/yaml.v3`.

**Spec:** `docs/superpowers/specs/2026-08-13-resource-controller-stages-2-4-design.md` (Stage 4 section)

## Global Constraints

- Go 1.26, no cgo. Module `github.com/mudler/agents-resources-controller`.
- SQLite at `MaxOpenConns(1)`. Any query that iterates rows and then issues another query MUST drain its rows to completion first, or it deadlocks.
- **Migrations are append-only.** The runner tracks `PRAGMA user_version`; editing an applied entry means deployed databases never receive the change. There is a live controller with a real database.
- Controller-side time goes through the `Clock` interface; store and server tests never sleep. Worker and e2e tests use real time with `require.Eventually`.
- **The allocation transaction is not to be restructured.**
- **Notification delivery must never block scheduling.** An unreachable webhook must not stop a job from starting, a device from being released, or a sweep from completing.
- **A verify probe that fails must quarantine the device before the job's terminal report reaches the controller**, or the device is briefly schedulable while still dirty.
- Probes, hooks and verify scripts all execute operator-supplied code as the worker user. That is already documented once in the README; do not repeat it per feature, but do not weaken it.

## Two carry-forwards from Stage 3, folded into this stage

Both are recorded in Stage 3's final review as accepted costs that belong here:

- **Label preservation is pass-wide.** One failing drop-in probe currently freezes *every* detected label on *every* device of that host, including host facts that were gathered successfully — so a stale `disk_free_bytes` can route a job to a full disk. Task 5 makes preservation per-source.
- **`rc describe` computes label and sheet ages on the CLI host** while heartbeat age is server-computed, so a skewed clock reads a month-old label as fresh. Task 6 moves them server-side.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/worker/verify.go` | Runs `verify.d` after a job, before its terminal report. New. |
| `internal/worker/worker.go` | Calls verify in `execute`; raises a fault on failure. |
| `internal/notify/notify.go` | Event type, bounded-queue notifier, retry/backoff. New, dependency-free. |
| `internal/notify/webhook.go` | HTTP delivery. New. |
| `internal/server/server.go` | Holds a notifier; `Publish` fans out to SSE and the notifier. |
| `internal/server/worker_api.go` | Emits `verify_failed`, `device_unhealthy`. |
| `internal/cli/serve.go` | Builds the notifier from config; emits sweep-sourced events. |
| `internal/store/reaper.go` | `SweepResult` already reports what changed; no new writes. |
| `internal/worker/probe.go` | Per-source failure tracking replaces the pass-wide bool. |
| `internal/server/client_api.go` | Server-computed label and sheet ages in `DescribeResponse`. |
| `internal/cli/describe.go` | Renders the server's ages instead of computing its own. |
| `internal/server/dashboard/index.html` | Kill-my-job and clear-device actions. |
| `e2e/verify_test.go`, `e2e/notify_test.go` | End-to-end coverage. New. |

---

### Task 1: Verify probes on the worker

**Files:**
- Create: `internal/worker/verify.go`
- Modify: `internal/worker/config.go` (verify dir + timeout), `internal/worker/worker.go` (call it)
- Test: `internal/worker/verify_test.go`

**Interfaces:**
- Consumes: `Run(ctx, JobSpec, io.Writer) Result`; `Config`; `(*Worker).reportFault(ctx, deviceID, reason string)` which already exists from the hooks work and retries.
- Produces: `worker.verifyResult{OK bool, Reason string}`; `(*Worker).runVerify(ctx context.Context, deviceID, jobID string) verifyResult`; `Config.VerifyDir string` (default `/etc/rc/verify.d`) and `Config.VerifyTimeout time.Duration` (default 30s), both applied in the existing `withDefaults`.

**The ordering that makes this feature work, and the reason it is the whole point:**

A verify probe exists to catch "the job exited but VRAM is still pinned". If it ran *after* the terminal report, the controller would already have flipped the device back to `ready` and possibly handed it to the next job — which then OOMs, which is the failure this project exists to prevent. So:

1. The job's process tree is gone (`Run` has returned).
2. Run `verify.d`. Each script gets `VerifyTimeout`, its own process group via `Run`, and the same environment a hook gets plus `RC_JOB_ID`.
3. **If any script fails**, call `reportFault` and wait for it — the device becomes `unhealthy` — *then* send the terminal report.
4. If all pass (or there are none), send the terminal report as today.

Step 3's ordering is load-bearing: `Store.Release` only flips `busy → ready`, so a device already `unhealthy` stays quarantined when the terminal report lands. That is existing behaviour this task relies on rather than new code — do not change `Release`.

- [ ] **Step 1: Write the failing tests**

Create `internal/worker/verify_test.go`:

```go
package worker_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

func writeVerify(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755))
}

func newVerifyWorker(t *testing.T, dir string) *worker.Worker {
	t.Helper()
	return worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:       []worker.DeviceConfig{{Name: "gpu0"}},
		VerifyDir:     dir,
		VerifyTimeout: 2 * time.Second,
	})
}

func TestVerifyPassesWhenEveryScriptSucceeds(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-ok.sh", `exit 0`)
	writeVerify(t, dir, "20-ok.sh", `exit 0`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.True(t, res.OK)
	require.Empty(t, res.Reason)
}

func TestVerifyFailsAndCarriesTheScriptsStderr(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-vram.sh", `echo "72G still allocated" >&2; exit 1`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.False(t, res.OK)
	require.Contains(t, res.Reason, "72G still allocated",
		"the operator needs to know WHY the device was quarantined")
	require.Contains(t, res.Reason, "10-vram.sh", "and which script said so")
}

// A later script must still run after an earlier one passes, and one failure
// is enough to fail the whole pass.
func TestVerifyRunsEveryScriptAndFailsIfAnyFails(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")
	writeVerify(t, dir, "10-bad.sh", `exit 1`)
	writeVerify(t, dir, "20-good.sh", `touch `+marker+`; exit 0`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.False(t, res.OK)
	require.FileExists(t, marker, "a failing script must not stop the rest of the pass")
}

func TestVerifyTimesOutAsAFailure(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-hang.sh", `sleep 60`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:       []worker.DeviceConfig{{Name: "gpu0"}},
		VerifyDir:     dir,
		VerifyTimeout: 300 * time.Millisecond,
	})

	start := time.Now()
	res := w.RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.False(t, res.OK)
	require.Less(t, time.Since(start), 10*time.Second, "a hanging script must not stall the pass")
	require.Contains(t, res.Reason, "10-hang.sh")
}

// The feature is off unless a host ships scripts.
func TestVerifyPassesWhenThereAreNoScripts(t *testing.T) {
	res := newVerifyWorker(t, t.TempDir()).RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.True(t, res.OK)
}

func TestVerifyPassesWhenTheDirectoryDoesNotExist(t *testing.T) {
	res := newVerifyWorker(t, filepath.Join(t.TempDir(), "nope")).
		RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.True(t, res.OK)
}

func TestVerifyScriptSeesTheDeviceAndJob(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-env.sh", `[ "$RC_DEVICE" = "box:gpu0" ] && [ "$RC_JOB_ID" = "job1" ] || exit 1`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1")
	require.True(t, res.OK, "the script did not receive RC_DEVICE and RC_JOB_ID")
}
```

Add to `internal/worker/export_test.go` (package `worker`), beside the existing helper:

```go
func (w *Worker) RunVerifyForTest(ctx context.Context, deviceID, jobID string) VerifyResult {
	return w.runVerify(ctx, deviceID, jobID)
}
```

The type is `VerifyResult` with exported `OK` and `Reason`, while `runVerify` stays unexported — the production API gains nothing.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/worker/ -run Verify -v`
Expected: FAIL — `VerifyDir` undefined.

- [ ] **Step 3: Implement**

Create `internal/worker/verify.go`. Model it closely on `probe.go`'s drop-in runner, which already solves the same problems: executables in name order, each through `Run` so it gets its own process group and a bounded lifetime, output captured with a cap, and a missing directory treated as "nothing to do".

Differences from a probe: a verify script's **exit code is the result** (a probe's output is), its **stderr is the reason** an operator will read, and a failure is not skipped — it fails the pass. Collect the failing script's name and the tail of its stderr into `Reason`.

Add `VerifyDir` (default `/etc/rc/verify.d`) and `VerifyTimeout` (default 30s) to `Config`, applied in `withDefaults` so a zero value can never mean unbounded.

- [ ] **Step 4: Call it from `execute`, in the right order**

In `internal/worker/worker.go`'s `execute`, after `Run` returns and before the terminal report:

```go
	// A device that fails verification must be quarantined BEFORE the
	// controller learns the job is over: Release only flips busy -> ready, so
	// a device already unhealthy stays quarantined when the report lands. The
	// other order leaves it briefly schedulable while still dirty, which is
	// the OOM this check exists to prevent.
	if v := w.runVerify(reportCtx, a.DeviceID, a.JobID); !v.OK {
		slog.Error("verify failed; quarantining device",
			"device", a.DeviceID, "job", a.JobID, "reason", v.Reason)
		w.reportFault(reportCtx, a.DeviceID, v.Reason)
	}
```

Use `reportCtx` (the `context.WithoutCancel` one), not `jobCtx`: a job ending during shutdown must still have its device verified, since that is exactly when a dirty device would otherwise be handed out on restart.

- [ ] **Step 5: Run the tests and the whole suite**

Run: `go clean -testcache && go test ./internal/worker/ -race -count=2`, then `go test ./... -race`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/worker/verify.go internal/worker/verify_test.go internal/worker/config.go internal/worker/worker.go internal/worker/export_test.go
git commit -m "feat: verify probes quarantine a dirty device before it returns to the pool"
```

---

### Task 2: The notifier

**Files:**
- Create: `internal/notify/notify.go`, `internal/notify/webhook.go`
- Test: `internal/notify/notify_test.go`

**Interfaces:**
- Consumes: nothing from this repo — keep it dependency-free so its delivery semantics test without a server.
- Produces: `notify.Event{Kind, Device, Job, Reason string, At time.Time}`; `notify.Sink` interface with `Deliver(ctx context.Context, e Event) error`; `notify.New(sink Sink, opts notify.Options) *notify.Notifier` with `Options{QueueSize int, Attempts int, Backoff time.Duration}`; `(*Notifier).Notify(e Event)` which never blocks; `(*Notifier).Close(ctx context.Context) error` draining what it can; `notify.NewWebhook(url string, client *http.Client) notify.Sink`; and the event-kind constants `notify.KindWatchdogTrip`, `KindDeviceUnhealthy`, `KindWorkerLost`, `KindJobLost`, `KindVerifyFailed`, `KindLeaseExpired`.

**The property everything else depends on:** `Notify` must never block its caller, and must never fail one. It is called from the scheduler's path, from the sweep, and from HTTP handlers. When the queue is full, drop the event and count the drop — a lost notification is an inconvenience; a stalled scheduler is an outage.

- [ ] **Step 1: Write the failing tests**

Create `internal/notify/notify_test.go`:

```go
package notify_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/notify"
	"github.com/stretchr/testify/require"
)

type recordingSink struct {
	mu       sync.Mutex
	events   []notify.Event
	failFor  int32 // fail this many calls before succeeding
	attempts atomic.Int32
	block    chan struct{}
}

func (s *recordingSink) Deliver(ctx context.Context, e notify.Event) error {
	s.attempts.Add(1)
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.attempts.Load() <= s.failFor {
		return errors.New("boom")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *recordingSink) delivered() []notify.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]notify.Event(nil), s.events...)
}

func opts() notify.Options {
	return notify.Options{QueueSize: 8, Attempts: 3, Backoff: time.Millisecond}
}

func TestNotifyDeliversAnEvent(t *testing.T) {
	sink := &recordingSink{}
	n := notify.New(sink, opts())
	defer n.Close(context.Background())

	n.Notify(notify.Event{Kind: notify.KindWatchdogTrip, Device: "gpubox:gpu0"})

	require.Eventually(t, func() bool { return len(sink.delivered()) == 1 },
		2*time.Second, 10*time.Millisecond)
	require.Equal(t, "gpubox:gpu0", sink.delivered()[0].Device)
}

func TestNotifyRetriesThenSucceeds(t *testing.T) {
	sink := &recordingSink{failFor: 2}
	n := notify.New(sink, opts())
	defer n.Close(context.Background())

	n.Notify(notify.Event{Kind: notify.KindJobLost})

	require.Eventually(t, func() bool { return len(sink.delivered()) == 1 },
		2*time.Second, 10*time.Millisecond)
	require.EqualValues(t, 3, sink.attempts.Load(), "two failures then one success")
}

func TestNotifyGivesUpAfterTheRetryBudget(t *testing.T) {
	sink := &recordingSink{failFor: 1000}
	n := notify.New(sink, opts())
	defer n.Close(context.Background())

	n.Notify(notify.Event{Kind: notify.KindJobLost})

	require.Eventually(t, func() bool { return sink.attempts.Load() >= 3 },
		2*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.EqualValues(t, 3, sink.attempts.Load(), "no attempts beyond the budget")
	require.Empty(t, sink.delivered())
}

// The property the whole design rests on.
func TestNotifyNeverBlocksWhenTheQueueIsFull(t *testing.T) {
	sink := &recordingSink{block: make(chan struct{})}
	n := notify.New(sink, notify.Options{QueueSize: 2, Attempts: 1, Backoff: time.Millisecond})
	defer func() { close(sink.block); n.Close(context.Background()) }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			n.Notify(notify.Event{Kind: notify.KindWatchdogTrip})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Notify blocked behind a stuck sink; scheduling would stall")
	}
	require.Positive(t, n.Dropped(), "a full queue must drop and count, not block")
}

func TestCloseDrainsWhatItCan(t *testing.T) {
	sink := &recordingSink{}
	n := notify.New(sink, opts())

	n.Notify(notify.Event{Kind: notify.KindDeviceUnhealthy})
	require.NoError(t, n.Close(context.Background()))
	require.Len(t, sink.delivered(), 1, "a queued event should survive a graceful close")
}

func TestNotifyAfterCloseDoesNotPanic(t *testing.T) {
	sink := &recordingSink{}
	n := notify.New(sink, opts())
	require.NoError(t, n.Close(context.Background()))

	require.NotPanics(t, func() { n.Notify(notify.Event{Kind: notify.KindJobLost}) })
}

// A nil notifier is the "no webhook configured" case and must be safe, since
// every call site would otherwise need a nil check.
func TestNilNotifierIsSafe(t *testing.T) {
	var n *notify.Notifier
	require.NotPanics(t, func() { n.Notify(notify.Event{Kind: notify.KindJobLost}) })
	require.NotPanics(t, func() { _ = n.Close(context.Background()) })
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/notify/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement the notifier**

Create `internal/notify/notify.go` with: the `Event` struct (JSON tags `event`, `device`, `job`, `reason`, `at` to match the spec's example payload), the six `Kind` constants, `Sink`, `Options`, and `Notifier`.

`New` starts one goroutine reading a buffered channel of `QueueSize`. `Notify` does a non-blocking send and increments a dropped counter on the default branch; it must be safe on a nil receiver and after `Close`. Delivery retries up to `Attempts` with `Backoff` doubling between tries. `Close` stops accepting, drains the channel with the caller's context bounding it, and waits for the goroutine.

Log a dropped event and a give-up at warn level, naming the kind and device — an operator who never sees their webhook fire needs to find out why from the controller's own logs.

- [ ] **Step 4: Implement the webhook sink**

Create `internal/notify/webhook.go`: `NewWebhook(url string, client *http.Client) Sink` posting the event as JSON with `Content-Type: application/json`. Treat 2xx as success and everything else as an error carrying the status. Give the request a bounded timeout of its own if the client has none.

- [ ] **Step 5: Run and commit**

Run: `go clean -testcache && go test ./internal/notify/ -race -count=2`
Expected: all PASS.

```bash
git add internal/notify/
git commit -m "feat: bounded-queue notifier that drops rather than blocking"
```

---

### Task 3: Wire the six events to their sources

**Files:**
- Modify: `internal/server/server.go`, `internal/server/worker_api.go`, `internal/server/client_api.go`, `internal/cli/serve.go`
- Test: `internal/server/notify_api_test.go`, `internal/cli/serve_notify_test.go`

**Interfaces:**
- Consumes: `notify.Notifier`, `notify.Event`, the six kind constants; `store.SweepResult` which already reports `DevicesUnknown`, `DevicesUnhealthy`, `JobsLost` and `LeasesExpired`.
- Produces: `server.Config` gains `Notifier *notify.Notifier`; `(*Server).notify(e notify.Event)` as the single internal emit point; `cli` gains `--webhook-url` on `rc serve` and reads `RC_WEBHOOK_URL`.

**Where each event comes from — this mapping is the task:**

| Kind | Source | Carries |
|---|---|---|
| `verify_failed` | `handleDeviceFault` when the reason came from a verify probe | device, job if known, the probe's reason |
| `device_unhealthy` | `handleDeviceFault`, and the sweep's `DevicesUnhealthy` | device, reason |
| `worker_lost` | the sweep, when a worker's devices are quarantined for worker loss | device, reason |
| `job_lost` | the sweep's `JobsLost` | job, device, reason |
| `lease_expired` | the sweep's `LeasesExpired` | job, device |
| `watchdog_trip` | `handleJobStatus` when a terminal report's reason names a watchdog | job, device, the watchdog reason |

Two of these need care. **`watchdog_trip` must not fire for every killed job** — a job killed by `rc kill` or by shutdown is not a watchdog trip; match on the reason the watchdog actually writes (`max_runtime exceeded`, `idle: no output for`) rather than on the `killed` state. And **`verify_failed` versus `device_unhealthy`** overlap: a verify failure is *also* a device going unhealthy. Emit the specific one (`verify_failed`) and not both, so a webhook consumer counting `device_unhealthy` does not double count.

- [ ] **Step 1: Write the failing tests**

In `internal/server/notify_api_test.go`, using a fake notifier that records events (the server package can define a tiny local recorder rather than importing the notify package's test helpers), assert: a fault carrying a verify reason emits exactly one `verify_failed` and no `device_unhealthy`; a fault from another source emits `device_unhealthy`; a terminal report with `max_runtime exceeded (4h)` emits `watchdog_trip` carrying that reason; a terminal report with reason `cancelled` emits nothing; a `killed` report from `rc kill` emits nothing.

In `internal/cli/serve_notify_test.go`, assert that a sweep which quarantines a device and loses its job emits `job_lost` and `device_unhealthy`/`worker_lost` as the table above requires, and that with no webhook configured the notifier is nil and nothing panics.

Write these as real Go tests with concrete assertions, following the shape of the existing tests in each package — `newServer(t)` returns four values in `server_test`, and `internal/cli` already has `serve_test.go` driving a real controller.

- [ ] **Step 2: Run to verify failure, then implement**

Add `Notifier` to `server.Config` and a small `(*Server).notify` helper that tolerates a nil notifier, so no call site needs a check. Emit from the sites in the table. In `internal/cli/serve.go`, build the notifier when a webhook URL is configured, pass it into `server.Config`, emit the sweep-sourced events after each sweep, and `Close` it during shutdown alongside the existing goroutine joins.

- [ ] **Step 3: Confirm the no-blocking property survives integration**

Run the whole suite with a webhook pointed at a server that never answers, and confirm jobs still schedule. A unit test proved `Notify` does not block; this step proves nothing downstream of it does either. If a unit test cannot express that cleanly, drive it in `internal/cli/serve_notify_test.go` with an `httptest` server that sleeps, and assert a job still gets assigned.

- [ ] **Step 4: Run and commit**

Run: `go clean -testcache && go test ./... -race`

```bash
git add internal/server/ internal/cli/serve.go internal/cli/serve_notify_test.go
git commit -m "feat: emit the six operational events to a configured webhook"
```

---

### Task 4: Dashboard actions

**Files:**
- Modify: `internal/server/dashboard/index.html`
- Test: `internal/server/dashboard_test.go`

**Interfaces:**
- Consumes: `POST /v1/jobs/{id}/kill` (client role, ownership-checked against the submitter) and `POST /v1/devices/{id}/clear` (admin role).
- Produces: no new routes. This task is deliberately all client-side.

**The auth shape, which is the point of the task:** the client token pasted into the page lives in `sessionStorage` for the tab and covers reading and killing **your own** jobs. Clearing an unhealthy device is admin-only, and the page must prompt for an admin token *at the moment you click*, use it for that one request, and never store it — not in `sessionStorage`, not in a variable that outlives the call. The credential resident in the browser is therefore always the weaker one.

- [ ] **Step 1: Write the failing tests**

Extend `internal/server/dashboard_test.go` to assert the shipped page contains no `localStorage` at all, and that the only `sessionStorage` key it writes is the client token. Assert the admin prompt path exists and that the served HTML contains no admin token literal. These are static assertions on the embedded asset — they cannot prove runtime behaviour, so also state in your report what you verified by hand in a browser.

- [ ] **Step 2: Implement**

Add a kill control on a running job the pasted identity submitted, and a clear control on an `unhealthy` device. Kill uses the stored client token and sends the submitter the page already knows. Clear prompts for an admin token, sends one request, and drops the value immediately — do not keep it in a closure that survives.

Both actions confirm before firing, refresh state on success, and surface the server's error text on failure rather than a generic message: the two most likely failures are `not_job_owner` and `device_not_cleared`, and both tell the operator something specific and useful.

Keep every user-controlled string inserted as a text node, keep the page self-contained, and keep it working in both palettes.

- [ ] **Step 3: Verify in a browser and commit**

Start a controller and a worker on ports above 19000, open the page, and exercise both actions — including the refusal paths (killing someone else's job, clearing a healthy device). Paste what you observed into your report.

```bash
git add internal/server/dashboard/ internal/server/dashboard_test.go
git commit -m "feat: kill and clear from the dashboard, with the admin token never stored"
```

---

### Task 5: Per-source label preservation

**Files:**
- Modify: `internal/worker/probe.go`, `internal/worker/worker.go`
- Test: `internal/worker/probe_test.go`

**Interfaces:**
- Consumes: `ProbeResult`, `gatherLabels`, `labelsPayload`.
- Produces: `ProbeResult` carries per-source failure rather than one pass-wide `Failed` bool — the exact shape is yours, but a device must be preserved only when *its own* facts are unconfirmed.

**The defect this fixes**, from Stage 3's final review, measured live:

> With a second, unrelated probe broken, `disk_free_bytes` and `mem_total_bytes` froze at their last good values on both devices while `builtinLabels()` kept succeeding, because a device omitted from the payload never gets `ReplaceLabels` called at all. A buggy `/etc/rc/probe.d/50-custom.sh` on a full box leaves `disk_free_bytes=72G` advertised forever; `--select 'disk_free_bytes>=50G'` routes a job there and it dies on write.

Preservation must be **narrow**: a source that failed preserves the facts *that source* contributes; sources that succeeded still refresh. A host fact gathered successfully must reach the database even when an unrelated drop-in script is broken.

Keep the rule Stage 3 established and do not weaken it: if a probe source could not run at all — including `nvidia-smi` present at worker start and absent now — the facts it contributes are preserved rather than cleared, because wiping is fleet-wide while a stale label is one device.

- [ ] **Step 1: Write the failing test**

`TestOneBrokenDropInDoesNotFreezeFactsFromWorkingSources`: a probe directory with one script that reports `disk_free_bytes` and succeeds, and one that fails; assert the successful script's fact reaches the payload and refreshes when its value changes, while the failing script's contribution is preserved. Drive it through the real `gatherLabels`, not a synthetic `ProbeResult`. Confirm it fails against the current code and report what you saw.

- [ ] **Step 2: Implement, run, commit**

Run: `go clean -testcache && go test ./internal/worker/ -race -count=2`, then `go test ./... -race`

```bash
git add internal/worker/
git commit -m "fix: preserve only the facts whose source failed, not the whole host"
```

---

### Task 6: Server-computed label and sheet ages

**Files:**
- Modify: `internal/server/client_api.go`, `internal/cli/describe.go`
- Test: `internal/server/describe_api_test.go`, `internal/cli/describe_test.go`

**Interfaces:**
- Consumes: `DescribeResponse`, `model.Label`.
- Produces: `DescribeResponse` gains `LabelAgeSeconds map[string]int` keyed by `key+"/"+source`, and `SheetAgeSeconds int`; the CLI renders those instead of computing from timestamps.

**Why:** `rc describe` currently computes label and sheet ages on the CLI host against `time.Now()`, while heartbeat age is server-computed. A machine with a skewed clock reads a month-old label as fresh, and the existing `d < 0` clamp renders a future-stamped label as `0s ago`. Provenance freshness is the whole point of the feature, so it must not depend on the reader's clock. The dashboard already does this correctly and its comment explains why — follow it.

- [ ] **Step 1: Write the failing tests**

Server: with the fake clock advanced a known amount past a label's timestamp, assert `LabelAgeSeconds` carries that value for the right key and source, and that `SheetAgeSeconds` does the same. CLI: assert the rendered output uses the server's age even when the local clock disagrees — construct the stub response with an age that could not be derived from the timestamp, and assert the rendered text matches the server's number.

That last assertion is the one that matters, so make it fail first: with the CLI still computing locally, a server age that contradicts the timestamp will not appear in the output.

- [ ] **Step 2: Implement, run, commit**

Keep the absolute timestamps in the response as well — a machine consumer may want them — but the human rendering uses the server's ages.

Run: `go clean -testcache && go test ./... -race`

```bash
git add internal/server/client_api.go internal/cli/describe.go internal/server/describe_api_test.go internal/cli/describe_test.go
git commit -m "fix: compute label and sheet ages on the controller, not the reader's clock"
```

---

### Task 7: End-to-end coverage and documentation

**Files:**
- Create: `e2e/verify_test.go`, `e2e/notify_test.go`
- Modify: `README.md`, `examples/worker.yaml`

- [ ] **Step 1: End-to-end tests**

`e2e/verify_test.go`: a host with a verify script that fails; run a job; assert the job reaches its normal terminal state **and** the device ends `unhealthy` rather than `ready`, and that a subsequent submit for that device is refused. Then a passing script, and assert the device returns to `ready` as usual. Use the existing `newFleet` harness rather than building a second one.

`e2e/notify_test.go`: point the controller at an `httptest` webhook, trigger a watchdog trip with a short `max_runtime`, and assert the webhook receives a `watchdog_trip` event naming the device and job. Then point it at a server that never answers and assert jobs still schedule — the no-blocking guarantee, end to end.

`require.Eventually` with generous bounds. A flaky e2e test is worse than none; if one proves unstable, widen its bounds rather than deleting an assertion, and say what you widened.

- [ ] **Step 2: Documentation**

Update the README's "Not built yet" table — verify probes, webhook notifications and dashboard actions now exist; that empties the table, so replace it with a short statement of what the tool does not do (the spec's non-goals: no multi-controller replication, no fractional device sharing, no shipping code to hosts, no per-client or per-worker tokens, no TLS).

Document: the verify-probe contract (exit code is the result, stderr is the reason, a failure quarantines the device, off unless scripts exist, and that they run *before* the job's terminal report and why); the webhook (config, the JSON shape, the six event kinds, that delivery is best-effort with bounded retries and that a full queue drops rather than blocking); and the dashboard actions (kill your own job with the pasted client token, clear a device with an admin token prompted per click and never stored).

Update `examples/worker.yaml` with `verify_dir` and `verify_timeout`.

- [ ] **Step 3: Verify the documentation by running it**

Start a controller and a worker on ports above 19000 with a real verify script and a real webhook receiver. Run a job that fails verification, watch the device quarantine, clear it, and capture the webhook payloads. Paste the actual output into your report — every claim in the README must be one you watched happen.

- [ ] **Step 4: Run everything and commit**

Run: `go clean -testcache && go test ./e2e/ -race -count=2`, then `go test ./... -race`, then `go build ./... && go vet ./... && gofmt -l .`

```bash
git add e2e/ README.md examples/worker.yaml
git commit -m "test: end-to-end verify and notification coverage; docs: stage 4"
```

---

## Self-Review Notes

**Spec coverage:** verify probes with the stderr-as-reason contract and off-unless-configured (Task 1); the six event kinds with bounded retries and never blocking (Tasks 2, 3); dashboard actions with the admin token prompted per click and never stored (Task 4). The spec's non-goals move into the README in Task 7.

**Beyond the spec, both carried from Stage 3's final review as accepted costs that belong here:** per-source label preservation (Task 5) and server-computed ages (Task 6).

**A known weakness, stated so nobody mistakes it for an oversight:** Tasks 1, 2, 5 and 6 carry complete or near-complete test code; Tasks 3, 4 and 7 specify what to assert and leave the code to the implementer. That is a lower bar, and it is where a test that cannot fail has slipped in five times in this project — every one asserting on a substring another line in the same output also satisfied. Reviewers of those three tasks should break the code and watch the test fail rather than reading the assertions.

**Known risks:**

- Task 1 changes the order of operations at the end of every job. Getting it backwards puts a dirty device back in the pool, which is the failure the task exists to prevent — the e2e test in Task 7 is what proves the ordering, so it is not optional.
- Task 3 touches the sweep and the status handler, both of which several stages depend on. The events are additive, but an emit that panics or blocks would take the scheduler with it; the nil-notifier and never-block tests are the guard.
- Task 4 is the first code that lets a browser change state. The routes it calls are already authenticated and ownership-checked, so the risk is concentrated in the admin-token handling.

