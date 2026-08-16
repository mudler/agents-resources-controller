package worker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/worker"
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

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.True(t, res.OK)
	require.Empty(t, res.Reason)
}

func TestVerifyFailsAndCarriesTheScriptsStderr(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-vram.sh", `echo "72G still allocated" >&2; exit 1`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.False(t, res.OK)
	require.Contains(t, res.Reason, "72G still allocated",
		"the operator needs to know WHY the device was quarantined")
	require.Contains(t, res.Reason, "10-vram.sh", "and which script said so")
	// RULING C: a later task keys a verify_failed event on this exact
	// literal prefix over the free-text /v1/devices/{id}/fault endpoint. The
	// literal string is asserted here, not the package's own
	// verifyReasonPrefix constant, so a wording tidy-up that renames both
	// together still fails this test loudly instead of shipping a silent
	// cross-task contract break.
	require.True(t, strings.HasPrefix(res.Reason, "verify failed: "),
		"Reason must start with the literal prefix a later task keys off of")
}

// A later script must still run after an earlier one passes, and one failure
// is enough to fail the whole pass.
func TestVerifyRunsEveryScriptAndFailsIfAnyFails(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")
	writeVerify(t, dir, "10-bad.sh", `exit 1`)
	writeVerify(t, dir, "20-good.sh", `touch `+marker+`; exit 0`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.False(t, res.OK)
	require.FileExists(t, marker, "a failing script must not stop the rest of the pass")
}

// Two FAILING scripts, not one passing and one failing: the earlier
// multi-script test above leaves both "last failure wins" and "a failure
// stops the pass early" undetectable, since its second script passes. This
// one pins both: the SECOND script must still run (proving the loop does
// not break on the first failure), and the reason must stay the FIRST
// script's (proving a later failure does not overwrite it).
func TestVerifyFirstFailureReasonWinsButAllScriptsStillRun(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "ran-20")
	writeVerify(t, dir, "10-first-bad.sh", `echo "first complaint" >&2; exit 1`)
	writeVerify(t, dir, "20-second-bad.sh", `touch `+marker+`; echo "second complaint" >&2; exit 1`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.False(t, res.OK)
	require.FileExists(t, marker, "the second failing script must still run, not be skipped once the first fails")
	require.Contains(t, res.Reason, "10-first-bad.sh")
	require.Contains(t, res.Reason, "first complaint")
	require.NotContains(t, res.Reason, "20-second-bad.sh",
		"the first failure's reason must win over a later one")
	require.NotContains(t, res.Reason, "second complaint")
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
	res := w.RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.False(t, res.OK)
	require.Less(t, time.Since(start), 10*time.Second, "a hanging script must not stall the pass")
	require.Contains(t, res.Reason, "10-hang.sh")
}

// The feature is off unless a host ships scripts.
func TestVerifyPassesWhenThereAreNoScripts(t *testing.T) {
	res := newVerifyWorker(t, t.TempDir()).RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.True(t, res.OK)
}

func TestVerifyPassesWhenTheDirectoryDoesNotExist(t *testing.T) {
	res := newVerifyWorker(t, filepath.Join(t.TempDir(), "nope")).
		RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.True(t, res.OK)
}

// TestVerifyFailsWhenTheDirectoryCannotBeRead is the directory-level twin of
// the per-file stat test below, and exists because the two were NOT treated
// alike: an absent directory (the feature is off) and an unreadable one (the
// feature is configured and could not run) both returned OK, so a single
// `chmod 000` on verify.d silently disabled verification while recording
// every device as verified clean — with no fault, and therefore no event, to
// tell anyone.
//
// A device with no scripts at all is a device nobody promised anything
// about. A device whose scripts exist and could not be listed is a device
// somebody promised something about and nothing checked, which is the
// failure this whole feature exists to prevent, one level up from the file.
func TestVerifyFailsWhenTheDirectoryCannotBeRead(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so this case cannot be staged")
	}
	dir := t.TempDir()
	writeVerify(t, dir, "10-vram.sh", `exit 0`)
	require.NoError(t, os.Chmod(dir, 0o000))
	// Restored so t.TempDir's own cleanup can remove it.
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.False(t, res.OK,
		"an unreadable verify_dir must never be reported as a clean device: nothing ran, so nothing was proven")
	require.True(t, strings.HasPrefix(res.Reason, "verify failed: "),
		"the reason must carry the same prefix every other verify failure does, or the controller "+
			"reports it as a plain device_unhealthy instead of verify_failed")
	require.Contains(t, res.Reason, "permission denied", "the reason must say what actually went wrong")
}

func TestVerifyScriptSeesTheDeviceAndJob(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-env.sh",
		`[ "$RC_DEVICE" = "box:gpu0" ] && [ "$RC_JOB_ID" = "job1" ] && [ "$RC_SUBMITTER" = "agent-a" ] || exit 1`)

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.True(t, res.OK, "the script did not receive RC_DEVICE, RC_JOB_ID and RC_SUBMITTER")
}

// TestVerifyScriptSeesTheSubmitterFromTheAssignment pins the WIRING, not the
// environment builder: runVerify's own env test above would pass just as
// happily against the version of execute that handed it a hardcoded "", which
// is exactly the defect this closes. So this drives the real execute() path —
// Worker.Start against an httptest controller handing out an assignment with
// a submitter on it — and asserts the script sees that submitter and not an
// empty string.
//
// A verify script's whole reason to want it is to name whoever's run left the
// device dirty (page them, tag the quarantine, write it to a ticket); an
// empty RC_SUBMITTER fails silently rather than loudly, which is why this is
// worth a test of its own rather than trusting one argument at one call site.
func TestVerifyScriptSeesTheSubmitterFromTheAssignment(t *testing.T) {
	dir := t.TempDir()
	seen := filepath.Join(dir, "submitter.txt")
	// The script passes; what is under test is what it OBSERVED, so the
	// device's fate is beside the point here (the ordering tests above cover
	// that).
	writeVerify(t, dir, "10-who.sh", `printf '%s' "$RC_SUBMITTER" > `+seen+`; exit 0`)

	var (
		mu     sync.Mutex
		served bool
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/workers/register":
			json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})
		case "/v1/workers/w1/assignments":
			mu.Lock()
			first := !served
			served = true
			mu.Unlock()
			if !first {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"true"},
				"submitter": "alice@lab",
			}}})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Host:              "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          20 * time.Millisecond,
		VerifyDir:         dir,
		VerifyTimeout:     2 * time.Second,
	})
	go func() { _ = wk.Start(ctx) }()

	var got []byte
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(seen)
		if err != nil {
			return false
		}
		got = b
		return true
	}, 10*time.Second, 20*time.Millisecond, "the verify script never ran")

	require.Equal(t, "alice@lab", string(got),
		"a verify script must see the submitter of the job that just finished, not an empty RC_SUBMITTER")
}

// TestVerifyFailsOnAScriptItCannotStat closes a fail-open gap found in
// review: a dangling symlink (the realistic case is an operator symlinking
// 10-vram.sh -> /opt/rc/vram.sh, then a package upgrade removes the target)
// used to be logged at Warn and skipped, leaving OK=true — an operator who
// believes verification is running would never know it silently stopped
// checking anything. A script that cannot even be inspected must fail the
// pass exactly like one that ran and said no.
func TestVerifyFailsOnAScriptItCannotStat(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Symlink(filepath.Join(dir, "does-not-exist.sh"), filepath.Join(dir, "10-dangling.sh")))

	res := newVerifyWorker(t, dir).RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.False(t, res.OK, "a script that cannot be stat'ed must fail the pass, not silently pass it")
	require.True(t, strings.HasPrefix(res.Reason, "verify failed: "))
	require.Contains(t, res.Reason, "10-dangling.sh")
}

// TestVerifyFailsWhenThePassBudgetIsExceeded closes the other review gap:
// without a pass-wide budget, N scripts each capped at VerifyTimeout still
// serialise to N*VerifyTimeout, and runVerify runs on reportCtx — deliberately
// immune to worker shutdown — so nothing else would ever bound the total. Two
// scripts each individually allowed up to 10s (longer than the whole budget)
// must still be cut off, and cut off as a FAILURE naming the budget, not as
// a quiet partial pass.
func TestVerifyFailsWhenThePassBudgetIsExceeded(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-slow.sh", `sleep 5`)
	writeVerify(t, dir, "20-slow.sh", `sleep 5`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:          []worker.DeviceConfig{{Name: "gpu0"}},
		VerifyDir:        dir,
		VerifyTimeout:    10 * time.Second, // longer than the budget below, so the budget is what actually trips
		VerifyPassBudget: 500 * time.Millisecond,
	})

	start := time.Now()
	res := w.RunVerifyForTest(context.Background(), "box:gpu0", "job1", "agent-a")
	require.False(t, res.OK, "a pass that never finishes has proven nothing about the device and must not read as clean")
	require.Less(t, time.Since(start), 10*time.Second, "a stalled pass must not run past its own budget")
	require.Contains(t, res.Reason, "budget")
	require.True(t, strings.HasPrefix(res.Reason, "verify failed: "))
}

// TestVerifyFailureFaultsDeviceBeforeTerminalReport exercises the ordering
// that is this task's entire point through the REAL execute() path (via
// Worker.Start against a real httptest controller), not just RunVerifyForTest
// in isolation: a verify failure's fault report must reach the controller
// BEFORE the job's terminal report, because Store.Release only flips
// busy->ready and a device already unhealthy stays quarantined when the
// report lands — the other order leaves the device briefly schedulable
// while still dirty.
func TestVerifyFailureFaultsDeviceBeforeTerminalReport(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-dirty.sh", `echo "vram still pinned" >&2; exit 1`)

	var (
		mu     sync.Mutex
		events []string
		served bool
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/workers/register":
			json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

		case r.URL.Path == "/v1/workers/w1/assignments":
			mu.Lock()
			first := !served
			served = true
			mu.Unlock()
			if !first {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"true"},
			}}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/devices/gpubox:gpu0/fault":
			mu.Lock()
			events = append(events, "fault")
			mu.Unlock()
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body struct {
				State string `json:"state"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			if body.State == "succeeded" || body.State == "failed" || body.State == "killed" {
				mu.Lock()
				events = append(events, "terminal")
				mu.Unlock()
			}
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Host:              "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          20 * time.Millisecond,
		VerifyDir:         dir,
		VerifyTimeout:     2 * time.Second,
	})
	go func() { _ = wk.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) >= 2
	}, 10*time.Second, 20*time.Millisecond, "both the fault report and the terminal report must land")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"fault", "terminal"}, events,
		"the device fault must reach the controller BEFORE the terminal report, or Release could free a still-dirty device")
}

// TestVerifyFailureFaultsDeviceBeforeTerminalReportOnShutdown is the same
// ordering claim on the path the brief singles out as the reason runVerify
// is called with reportCtx (context.WithoutCancel), not the job's own
// cancellable ctx: a job ending DURING shutdown must still have its device
// verified, because that is exactly when a dirty device would otherwise be
// handed out again on the worker's restart.
func TestVerifyFailureFaultsDeviceBeforeTerminalReportOnShutdown(t *testing.T) {
	dir := t.TempDir()
	writeVerify(t, dir, "10-dirty.sh", `exit 1`)

	var (
		mu          sync.Mutex
		served      bool
		events      []string
		runningOnce sync.Once
	)
	runningSeen := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/workers/register":
			json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

		case r.URL.Path == "/v1/workers/w1/assignments":
			mu.Lock()
			first := !served
			served = true
			mu.Unlock()
			if !first {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"sleep", "5"},
			}}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/devices/gpubox:gpu0/fault":
			mu.Lock()
			events = append(events, "fault")
			mu.Unlock()
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body struct {
				State string `json:"state"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			if body.State == "succeeded" || body.State == "failed" || body.State == "killed" {
				events = append(events, "terminal")
			}
			mu.Unlock()
			if body.State == "running" {
				runningOnce.Do(func() { close(runningSeen) })
			}
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	defer cancelShutdown()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Host:              "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          50 * time.Millisecond,
		VerifyDir:         dir,
		VerifyTimeout:     2 * time.Second,
	})

	startReturned := make(chan struct{})
	go func() {
		_ = wk.Start(shutdownCtx)
		close(startReturned)
	}()

	select {
	case <-runningSeen:
	case <-time.After(10 * time.Second):
		t.Fatal("job never reported running")
	}

	cancelShutdown()

	select {
	case <-startReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after shutdown even though the job should die quickly under SIGTERM")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"fault", "terminal"}, events,
		"even on shutdown, the device fault must reach the controller BEFORE the terminal report — this is "+
			"exactly the moment a dirty device would otherwise be handed out on restart")
}
