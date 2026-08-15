package cli_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/cli"
	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/logstore"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

// TestHoldPrintsGrantedDeviceAndExpiry drives NewHoldCmd end to end against
// a stub controller that hands the hold straight to "assigned" (no queue
// wait) and asserts the granted message names the device and an expiry —
// exact fields, not a loose substring, so a message that happened to
// mention the device ID somewhere else could not satisfy it by accident.
func TestHoldPrintsGrantedDeviceAndExpiry(t *testing.T) {
	started := time.Now()
	job := model.Job{
		ID: "hold1", DeviceID: "gpubox:gpu0", State: model.JobAssigned,
		Kind: model.LeaseKindHold, Reason: "manual profiling", StartedAt: &started,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			var body server.SubmitRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			require.Equal(t, model.LeaseKindHold, body.Kind, "rc hold must submit kind=hold")
			require.Empty(t, body.Command, "rc hold must never submit a command")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/hold1":
			// WaitTerminal's poll: block forever so this test controls
			// exactly when the RunE call returns, via context cancellation.
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewHoldCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	stderr := &syncBuffer{}
	cmd.SetErr(stderr)
	cmd.SetArgs([]string{"gpubox:gpu0", "--ttl", "30m", "--reason", "manual profiling", "--as", "tester"})

	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	go func() {
		require.Eventually(t, func() bool {
			return len(stderr.String()) > 0
		}, 2*time.Second, 10*time.Millisecond)
		cancel()
	}()

	_ = cmd.Execute()

	out := stderr.String()
	require.Contains(t, out, "hold1 granted on gpubox:gpu0", "must name the job and the device it was granted on")
	require.Contains(t, out, "expires around", "must tell the caller when the hold expires")
}

// TestHoldCtrlCOnAGrantedHoldReleasesIt is the behaviour that sets rc hold
// apart from rc run: Ctrl-C on a job that is already granted (running) must
// release it — the opposite of rc run's detach-only handling of a running
// job — because a hold's whole purpose is that a human is present and
// leaving means they're done. This asserts the actual kill/release request
// reached the controller, not just that rc hold printed something.
func TestHoldCtrlCOnAGrantedHoldReleasesIt(t *testing.T) {
	job := model.Job{ID: "hold1", DeviceID: "gpubox:gpu0", State: model.JobRunning, Kind: model.LeaseKindHold}
	var releaseCalled atomic.Bool
	var releaseSubmitter atomic.Value
	polled := make(chan struct{}, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(job)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs/hold1":
			select {
			case polled <- struct{}{}:
			default:
			}
			_ = json.NewEncoder(w).Encode(server.JobView{Job: job})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/hold1/kill":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			releaseSubmitter.Store(body["submitter"])
			releaseCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	t.Setenv("RC_CONTROLLER", ts.URL)
	t.Setenv("RC_TOKEN", "tok")

	cmd := cli.NewHoldCmd()
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetErr(&syncBuffer{})
	cmd.SetArgs([]string{"gpubox:gpu0", "--ttl", "30m", "--as", "tester"})

	ctx, cancel := context.WithCancel(context.Background())
	cmd.SetContext(ctx)
	go func() {
		<-polled
		cancel()
	}()

	err := cmd.Execute()
	require.Error(t, err, "Ctrl-C must not report success")

	require.True(t, releaseCalled.Load(), "Ctrl-C on a granted hold must send a release (kill) request")
	require.Equal(t, "tester", releaseSubmitter.Load())
}

// TestHoldSubmissionCarryingCommandCwdOrEnvIsRefused proves the security
// property the whole design rests on end to end, against the REAL
// controller (not a stub echoing canned responses): the sleeper a hold
// runs, where it runs, and what environment it inherits are all the
// worker's choice, never the client's — not just the command. A submission
// that smuggles a payload through cwd or env (e.g. LD_PRELOAD) instead of
// command would let it run inside a process every view in the system labels
// "hold", exactly the "arbitrary code under a different label" the guard
// exists to prevent. rc hold itself has no flag to supply any of these, so
// this drives the client directly, the way a caller bypassing the CLI (or a
// future bug in it) could.
//
// The device is registered so a rejected case is distinguished from an
// accepted one by 400 vs 201, not by which error the store happens to
// return for a device it has never heard of — see the fix-round-1 review
// note this replaced a version of this test for.
func TestHoldSubmissionCarryingCommandCwdOrEnvIsRefused(t *testing.T) {
	dir := t.TempDir()
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))
	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, st.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1", State: model.DeviceReady}},
	))

	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"ctok": "client"},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	cl := client.New(ts.URL, "ctok")

	cases := []struct {
		name string
		opts client.SubmitOptions
	}{
		{"command", client.SubmitOptions{Command: []string{"true"}}},
		{"cwd", client.SubmitOptions{Cwd: "/attacker/cwd"}},
		{"env", client.SubmitOptions{Env: map[string]string{"LD_PRELOAD": "/tmp/evil.so"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := tc.opts
			opts.DeviceID = "gpubox:gpu0"
			opts.Submitter = "tester"
			opts.MaxRuntime = time.Minute
			opts.Kind = model.LeaseKindHold
			_, err := cl.Submit(context.Background(), opts)
			require.Error(t, err, "the controller must refuse a hold submission carrying "+tc.name)
			require.Contains(t, err.Error(), "bad_request")
		})
	}

	// A clean hold — none of command, cwd, or env — must still be accepted
	// (201): the guard must reject exactly the fields that matter, not holds
	// in general.
	job, err := cl.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "gpubox:gpu0", Submitter: "tester", MaxRuntime: time.Minute, Kind: model.LeaseKindHold,
	})
	require.NoError(t, err, "a hold with no command, cwd, or env must be accepted")
	require.NotNil(t, job)

	// And the sibling rule: a hold with no --ttl must also be refused,
	// rather than silently granted with no expiry.
	_, err = cl.Submit(context.Background(), client.SubmitOptions{
		DeviceID:  "gpubox:gpu0",
		Submitter: "tester",
		Kind:      model.LeaseKindHold,
	})
	require.Error(t, err, "the controller must refuse a hold submission with no --ttl")
	require.Contains(t, err.Error(), "bad_request")
}
