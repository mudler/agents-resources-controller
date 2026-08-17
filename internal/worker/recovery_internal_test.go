package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/stretchr/testify/require"
)

// fakeScan replaces the /proc walk for the length of one test. It exists for
// the isolation cases only: a PID namespace containing exactly one process
// cannot be created without privileges the test runner does not have, so the
// DECISION is tested here and the SCAN itself is tested against real /proc by
// the survivor tests below, which can create a real, findable process.
func fakeScan(t *testing.T, procs []procStat, scanned bool) *int {
	t.Helper()
	calls := 0
	prev := procScan
	procScan = func(want func(procStat) bool) (bool, bool) {
		calls++
		for _, p := range procs {
			if want(p) {
				return true, scanned
			}
		}
		return false, scanned
	}
	t.Cleanup(func() { procScan = prev })
	return &calls
}

// PID 1 in a namespace holding nothing but itself: the container restart
// destroyed everything the previous job was running, so there is nothing
// left to hold a GPU.
func TestIsolatedWhenPIDOneIsAloneInItsNamespace(t *testing.T) {
	fakeScan(t, []procStat{{pid: 1, state: 'S'}}, true)
	require.True(t, isolatedPIDNamespace(1))
}

// PID 1 with company is not isolation: a pod with a shared process
// namespace, or an image that also runs an sshd, is PID 1 alongside
// processes that could include a job from before.
func TestNotIsolatedWhenSomethingElseIsAliveInTheNamespace(t *testing.T) {
	fakeScan(t, []procStat{{pid: 1, state: 'S'}, {pid: 42, state: 'S'}}, true)
	require.False(t, isolatedPIDNamespace(1))
}

// A worker that is not PID 1 is not the init of its own namespace, whatever
// else is true, and must not scan its way to a claim: on a host it would be
// looking at the whole machine.
func TestNotIsolatedWhenNotPIDOne(t *testing.T) {
	calls := fakeScan(t, nil, true)
	require.False(t, isolatedPIDNamespace(4242))
	require.Zero(t, *calls, "being PID 1 is the precondition; nothing should be scanned without it")
}

// "Found nothing" and "could not look" are opposite answers. A /proc that
// could not be read proves nothing at all.
func TestNotIsolatedWhenProcCouldNotBeRead(t *testing.T) {
	fakeScan(t, nil, false)
	require.False(t, isolatedPIDNamespace(1))
}

// The host case, against real /proc: a process carrying RC_JOB_ID is exactly
// what a job left behind by a dead worker looks like, and it must be found.
func TestSurvivorCheckFindsALiveJobProcess(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	cmd.Env = append(os.Environ(), "RC_JOB_ID=00000000-dead-beef-0000-000000000000")
	// Its own process group, the way execute() starts a job, so the survivor
	// is as far from this test process as a real one is from its worker.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
	})

	checked, found := survivorsFromPreviousRun(os.Getpid())
	require.True(t, checked, "the scan ran; this host's /proc is readable")
	require.True(t, found, "a live process carrying RC_JOB_ID is a survivor")
}

// The recovery case: once nothing carrying RC_JOB_ID is left alive, the check
// comes back clean — which is the proof that returns a device to the pool.
//
// The process SET is bounded here while the environ read stays real, unlike
// the test above. The whole-machine version of this assertion is not stable
// anywhere it would actually run: a development box (this one included) can
// legitimately be running an rc job of its own at the moment `go test` fires,
// and the check would then correctly find it and correctly report a survivor
// — a test that fails because the code was right about the room it was
// standing in.
func TestSurvivorCheckIsCleanOnceTheJobProcessIsGone(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	cmd.Env = append(os.Environ(), "RC_JOB_ID=00000000-dead-beef-0000-000000000000")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	gone := cmd.Process.Pid

	require.NoError(t, cmd.Process.Kill())
	_, err := cmd.Process.Wait() // reaped: not even a zombie is left
	require.NoError(t, err)

	fakeScan(t, []procStat{{pid: gone, state: 'S'}}, true)

	checked, found := survivorsFromPreviousRun(os.Getpid())
	require.True(t, checked)
	require.False(t, found, "nothing from the previous run is alive any more")
}

// A worker started from inside a job (`rc run rc worker`) inherits RC_JOB_ID.
// Finding itself would quarantine its whole host forever, so it is excluded
// by pid.
func TestSurvivorCheckIgnoresTheWorkersOwnProcess(t *testing.T) {
	// A real process carrying RC_JOB_ID, standing in for the worker itself:
	// asking it about its own pid must come back clean even though its
	// environment matches, which is precisely what a test using the test
	// binary's own (unmarked) pid could not tell apart from no match at all.
	cmd := exec.Command("sleep", "60")
	cmd.Env = append(os.Environ(), "RC_JOB_ID=00000000-dead-beef-0000-000000000000")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	self := cmd.Process.Pid
	fakeScan(t, []procStat{{pid: self, state: 'S'}}, true)

	checked, found := survivorsFromPreviousRun(self)
	require.True(t, checked)
	require.False(t, found, "a worker started from inside a job must not find itself and quarantine its own host forever")
}

// A process this worker cannot inspect — anything it does not own — is the
// difference between "not one of ours" and "I could not tell". It must
// produce no claim rather than a comforting empty answer.
func TestSurvivorCheckProvesNothingWhenAProcessCannotBeInspected(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root: every /proc/<pid>/environ is readable, so the blind case cannot be produced")
	}
	// PID 1 is init, owned by root, and its environ is mode 0400 — the
	// canonical "there are processes here I cannot see into".
	fakeScan(t, []procStat{{pid: 1, state: 'S'}}, true)

	checked, found := survivorsFromPreviousRun(os.Getpid())
	require.False(t, checked, "a scan that could not read part of the process table has established nothing")
	require.False(t, found)
}

// A /proc that could not be walked at all is the same answer, arrived at
// sooner.
func TestSurvivorCheckProvesNothingWhenProcCouldNotBeRead(t *testing.T) {
	fakeScan(t, nil, false)
	checked, found := survivorsFromPreviousRun(os.Getpid())
	require.False(t, checked)
	require.False(t, found)
}

// A lifecycle hook with no job behind it is run with RC_JOB_ID set to the
// empty string (see runStartupReleaseHooks). Counting one of those as a
// survivor would mean a worker's own startup hooks could keep its devices
// quarantined.
func TestEmptyJobIDIsNotASurvivor(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	cmd.Env = append(os.Environ(), "RC_JOB_ID=")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	// Give the child a moment to actually exist in /proc with its env.
	require.Eventually(t, func() bool {
		_, err := os.Stat("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/environ")
		return err == nil
	}, 2*time.Second, 10*time.Millisecond)

	match, err := hasJobEnv(cmd.Process.Pid)
	require.NoError(t, err)
	require.False(t, match, "an empty RC_JOB_ID marks a hook, not a job")
}

// A process that exits while /proc is being walked holds nothing, and its
// disappearance is not an error the caller has to interpret.
func TestExitedProcessIsNeitherSurvivorNorError(t *testing.T) {
	cmd := exec.Command("true")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid
	require.NoError(t, cmd.Wait())

	match, err := hasJobEnv(pid)
	require.NoError(t, err, "a process that has gone is not an error")
	require.False(t, match)
}

// The claim has to actually leave the machine. register() is where the proof
// is computed and the only place it is ever sent, so this pins the wire: the
// controller reads "recovery", and a worker that computed a proof and forgot
// to attach it would look exactly like a worker that could prove nothing.
func TestRegistrationCarriesTheProofOnTheWire(t *testing.T) {
	// A namespace containing only this process: the survivor check excludes
	// the worker itself, so it comes back clean without depending on what
	// else happens to be running on the machine running the test.
	fakeScan(t, []procStat{{pid: os.Getpid(), state: 'S'}}, true)

	var body []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/workers/register" {
			body, _ = io.ReadAll(r.Body)
			_ = json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	w := New(Config{
		ControllerURL: ts.URL, Host: "gpubox", Devices: []DeviceConfig{{Name: "gpu0"}},
		ProbeDir: t.TempDir(), VerifyDir: t.TempDir(), SheetDir: t.TempDir(),
	})
	require.NoError(t, w.register(context.Background()))

	var sent struct {
		Recovery model.RecoveryProof `json:"recovery"`
	}
	require.NoError(t, json.Unmarshal(body, &sent))
	require.Equal(t, model.RecoveryProof{SurvivorsChecked: true}, sent.Recovery)
	require.True(t, sent.Recovery.Proves())
}

// And the silenced worker must send a claim the controller reads as nothing
// at all — not a field the controller has to interpret, but an absent one.
func TestRegistrationSendsNoProofWhenManualClearIsRequired(t *testing.T) {
	fakeScan(t, []procStat{{pid: os.Getpid(), state: 'S'}}, true)

	var body []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/workers/register" {
			body, _ = io.ReadAll(r.Body)
			_ = json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	w := New(Config{
		ControllerURL: ts.URL, Host: "gpubox", Devices: []DeviceConfig{{Name: "gpu0"}},
		ProbeDir: t.TempDir(), VerifyDir: t.TempDir(), SheetDir: t.TempDir(),
		RequireManualClear: true,
	})
	require.NoError(t, w.register(context.Background()))

	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))
	require.NotContains(t, raw, "recovery", "a suppressed claim is an absent field, not an empty object to be interpreted")
}

// require_manual_clear is the one setting, and it can only ever be more
// conservative: with it set, this worker claims nothing even in the one
// situation where it could prove isolation outright.
func TestRequireManualClearSuppressesEvenAProvenIsolation(t *testing.T) {
	fakeScan(t, []procStat{{pid: 1, state: 'S'}}, true)

	proving := New(Config{ControllerURL: "http://c", Host: "gpubox"})
	require.Equal(t, model.RecoveryProof{Isolated: true}, proving.recoveryProofFor(1),
		"test is broken if this worker cannot prove isolation: the suppression below would prove nothing")

	silenced := New(Config{ControllerURL: "http://c", Host: "gpubox", RequireManualClear: true})
	require.Equal(t, model.RecoveryProof{}, silenced.recoveryProofFor(1))
	require.False(t, silenced.recoveryProofFor(1).Proves())
}
