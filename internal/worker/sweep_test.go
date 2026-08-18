package worker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

// escapeeJob is the shape of job this whole mechanism exists for: it starts a
// child through setsid(1), which calls setsid(2) and so leaves both the
// process group and the session the worker put the job in. The parent shell
// then exits 0 — an ordinary, successful job — while the child keeps running,
// keeps its inherited RC_JOB_ID, and (on a real box) keeps the GPU.
//
// The redirection is required, not cosmetic: a child holding the inherited
// stdout pipe keeps cmd.Wait() blocked, so without it Run would simply wait
// out the full sleep instead of returning to a live escapee. The wait loop is
// required too — the sweep runs inside Run, so the escapee has to be a real
// process in /proc BEFORE the leader exits, or the test would be racing the
// thing it is trying to observe.
func escapeeJob(t *testing.T, pidFile string) []string {
	t.Helper()
	_, err := exec.LookPath("setsid")
	require.NoError(t, err, "setsid(1) from util-linux is required: it is how this test produces a process the group kill genuinely cannot reach")
	return []string{"sh", "-c",
		"setsid sh -c 'echo $$ > " + pidFile + "; exec sleep 300' >/dev/null 2>&1 &\n" +
			"while [ ! -s " + pidFile + " ]; do sleep 0.01; done\n" +
			"exit 0"}
}

// readPIDFile waits for the escapee to have written its pid and returns it.
func readPIDFile(t *testing.T, path string) int {
	t.Helper()
	var pid int
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(path)
		if err != nil || len(strings.TrimSpace(string(data))) == 0 {
			return false
		}
		p, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			return false
		}
		pid = p
		return true
	}, 5*time.Second, 20*time.Millisecond, "the escapee never recorded its pid")
	return pid
}

// procPgidSid reads a live process's process-group and session ids straight
// out of /proc, so the test can state as fact — not as belief — that the
// escapee is outside the group the worker killed.
func procPgidSid(t *testing.T, pid int) (pgid, sid int) {
	t.Helper()
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	require.NoError(t, err)
	s := string(data)
	fields := strings.Fields(s[strings.LastIndexByte(s, ')')+2:])
	require.GreaterOrEqual(t, len(fields), 4)
	pgid, err = strconv.Atoi(fields[2])
	require.NoError(t, err)
	sid, err = strconv.Atoi(fields[3])
	require.NoError(t, err)
	return pgid, sid
}

// The whole point of the mechanism, in two halves that must both hold.
//
// First half: with the sweep off, a setsid'd child survives everything Run
// does. Run kills the job's process group and then sweeps that group for
// stragglers; a process that called setsid(2) is in neither the group nor the
// session any more, so both miss it entirely. On this worker that is worse
// than it sounds — jobs run inside the worker's own long-lived container, so
// nothing tears the survivor down later either: it holds the GPU until the
// pod restarts and the next job gets a dirty device.
//
// Second half: with the sweep on, the same job leaves nothing behind, because
// setsid(2) changes the process group and the session and does NOT change the
// environment, so RC_JOB_ID still names the job that spawned it.
func TestSweepReapsASetsidEscapeeThatTheGroupKillCannotReach(t *testing.T) {
	jobID := "sweep-escapee-" + strconv.Itoa(os.Getpid())

	// Half one: no SweepJobID, so nothing but the existing group teardown runs.
	unsweptFile := filepath.Join(t.TempDir(), "pid")
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command: escapeeJob(t, unsweptFile),
		Env:     map[string]string{"RC_JOB_ID": jobID},
	}, &out)
	require.NoError(t, res.Err)
	require.Equal(t, 0, res.ExitCode)

	unswept := readPIDFile(t, unsweptFile)
	t.Cleanup(func() {
		_ = syscall.Kill(unswept, syscall.SIGKILL)
	})
	require.True(t, processAlive(t, unswept),
		"a setsid'd child is supposed to survive the process-group kill: if it does not, this test is no longer testing the escape it was written for")
	pgid, sid := procPgidSid(t, unswept)
	require.Equal(t, unswept, pgid, "the escapee leads its own process group, which is why kill(-pgid) never reached it")
	require.Equal(t, unswept, sid, "the escapee leads its own session too")
	require.NoError(t, syscall.Kill(unswept, syscall.SIGKILL))

	// Half two: the same job, swept by RC_JOB_ID.
	sweptFile := filepath.Join(t.TempDir(), "pid")
	var out2 syncBuf
	res = worker.Run(context.Background(), worker.JobSpec{
		Command:    escapeeJob(t, sweptFile),
		Env:        map[string]string{"RC_JOB_ID": jobID},
		SweepJobID: jobID,
	}, &out2)
	require.NoError(t, res.Err)
	require.Equal(t, 0, res.ExitCode)

	swept := readPIDFile(t, sweptFile)
	t.Cleanup(func() { _ = syscall.Kill(swept, syscall.SIGKILL) })

	require.Contains(t, res.Sweep.Found, swept,
		"the escapee carries this job's RC_JOB_ID and must be found by the sweep")
	require.Empty(t, res.Sweep.Remaining, "the escapee had no reason to survive SIGTERM")
	require.False(t, processAlive(t, swept), "the sweep reported the escapee reaped, so it must actually be gone")
}

// The regression this feature is most likely to reintroduce, and the reason
// the sweep reports through its own field instead of through Killed.
//
// On 2026-08-17 a straggler sweep that set Killed on the clean-exit path made
// 40 out of 40 ordinary successful jobs report themselves killed (see 337e23d,
// "a zombie is not a straggler"). That is not cosmetic: worker.go maps Killed
// onto model.JobKilled, hooks.go treats a killed hook as a failed hook, and
// verify.go treats a killed probe as a failed probe and quarantines the
// device.
//
// A job that exited 0 and happened to leave a detached process behind is a
// SUCCESSFUL job. The survivor is worth reporting — it is in Sweep — but it is
// not a different outcome for the job.
func TestSuccessfulJobWithADetachedSurvivorIsStillReportedSuccessful(t *testing.T) {
	jobID := "sweep-success-" + strconv.Itoa(os.Getpid())
	pidFile := filepath.Join(t.TempDir(), "pid")

	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command:    escapeeJob(t, pidFile),
		Env:        map[string]string{"RC_JOB_ID": jobID},
		SweepJobID: jobID,
	}, &out)

	survivor := readPIDFile(t, pidFile)
	t.Cleanup(func() { _ = syscall.Kill(survivor, syscall.SIGKILL) })

	// The sweep really did have something to do — otherwise this test would
	// pass just as happily against a sweep that never runs at all.
	require.Contains(t, res.Sweep.Found, survivor)

	require.NoError(t, res.Err)
	require.Equal(t, 0, res.ExitCode)
	require.False(t, res.Killed,
		"a job that exited 0 and left a detached process behind is a successful job, not a killed one (%q)", res.Reason)
	require.Empty(t, res.Reason)
}

// Jobs run back to back on the same box, and two of them can be alive at once
// on a multi-GPU host. Killing another job's processes would be a far worse
// bug than the leak this sweep exists to fix, so the match is on the exact
// value of RC_JOB_ID: not a prefix, not a substring.
//
// The two bystanders are chosen to catch precisely those two mistakes — one
// carries an id this job's id is a prefix OF, the other carries an id that is
// a prefix of this job's.
func TestSweepLeavesOtherJobsAlone(t *testing.T) {
	jobID := "sweep-others-" + strconv.Itoa(os.Getpid())

	bystanders := map[string]int{}
	for _, otherID := range []string{jobID + "-later", jobID[:len(jobID)-3]} {
		cmd := exec.Command("sleep", "300")
		cmd.Env = append(os.Environ(), "RC_JOB_ID="+otherID)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		require.NoError(t, cmd.Start())
		pid := cmd.Process.Pid
		bystanders[otherID] = pid
		t.Cleanup(func() {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			_, _ = cmd.Process.Wait()
		})
		// In /proc with its environment before the sweep looks.
		require.Eventually(t, func() bool {
			data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/environ")
			return err == nil && strings.Contains(string(data), "RC_JOB_ID="+otherID)
		}, 5*time.Second, 20*time.Millisecond)
	}

	pidFile := filepath.Join(t.TempDir(), "pid")
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command:    escapeeJob(t, pidFile),
		Env:        map[string]string{"RC_JOB_ID": jobID},
		SweepJobID: jobID,
	}, &out)
	require.NoError(t, res.Err)

	ours := readPIDFile(t, pidFile)
	t.Cleanup(func() { _ = syscall.Kill(ours, syscall.SIGKILL) })
	require.False(t, processAlive(t, ours), "this job's own escapee must still be reaped")

	for otherID, pid := range bystanders {
		require.NotContains(t, res.Sweep.Found, pid, "a process belonging to job %q was picked up by job %q's sweep", otherID, jobID)
		require.True(t, processAlive(t, pid), "a process belonging to job %q was killed by job %q's sweep", otherID, jobID)
	}
}

// The mechanism has to actually be switched on for real jobs, and the outcome
// the CONTROLLER is told has to be unaffected by it. Everything above tests
// the sweep through Run; this drives a whole worker against a fake controller
// so it covers the one line in execute() that asks for the sweep and the
// state mapping that the 2026-08-17 regression corrupted — where a sweep that
// set Killed turned every ordinary success into "killed" on the wire.
func TestWorkerSweepsAJobsEscapeesAndStillReportsSuccess(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pid")
	command := escapeeJob(t, pidFile)

	var (
		mu        sync.Mutex
		states    []string
		exitCodes []int
		served    bool
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/workers/register":
			_ = json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})
		case "/v1/workers/w1/assignments":
			mu.Lock()
			first := !served
			served = true
			mu.Unlock()
			if !first {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{{
				"job_id":    "sweep-e2e-job",
				"device_id": "gpubox:gpu0",
				"command":   command,
			}}})
		case "/v1/jobs/sweep-e2e-job/status":
			var body struct {
				State    string `json:"state"`
				ExitCode *int   `json:"exit_code"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			states = append(states, body.State)
			if body.ExitCode != nil {
				exitCodes = append(exitCodes, *body.ExitCode)
			}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Host:              "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: 50 * time.Millisecond,
		PollWait:          100 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(exitCodes) == 1
	}, 15*time.Second, 50*time.Millisecond, "the job never reached a terminal report")

	escapee := readPIDFile(t, pidFile)
	t.Cleanup(func() { _ = syscall.Kill(escapee, syscall.SIGKILL) })
	require.False(t, processAlive(t, escapee),
		"a job's setsid'd child outlived the job on a worker that is supposed to sweep by RC_JOB_ID")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []int{0}, exitCodes)
	require.Equal(t, []string{"running", "succeeded"}, states,
		"reaping a survivor must not change what the job itself is reported as")
}
