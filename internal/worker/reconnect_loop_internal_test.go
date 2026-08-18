package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/stretchr/testify/require"
)

// The other half of the 2026-08-18 defect, driven through the worker's real
// loops: autonomous recovery is evaluated at REGISTRATION, and a worker that
// registers once at startup never presents its proof to a controller that
// restarted underneath it. The device stays quarantined until a human clears
// it, which is what happened.

// registration is what one register call carried, decoded from the wire. The
// fields are the ones that make a registration worth anything: the proof the
// controller's AutoRecover reads, and the boot ID its reboot path reads.
type registration struct {
	Host     string              `json:"host"`
	BootID   string              `json:"boot_id"`
	Recovery model.RecoveryProof `json:"recovery"`
	Devices  []struct {
		Name string `json:"name"`
	} `json:"devices"`
}

// fakeController is a controller a test can restart (by changing the identity
// it reports) and kill (by dropping every request), and that records what was
// registered with it.
type fakeController struct {
	mu            sync.Mutex
	instance      string
	down          bool
	registrations []registration
	assignment    map[string]any // handed out once, on the first poll
	handedOut     bool
	running       chan struct{} // closed when the worker reports a job running
}

func newFakeController(t *testing.T) (*fakeController, *httptest.Server) {
	t.Helper()
	c := &fakeController{instance: "controller-a", running: make(chan struct{})}
	ts := httptest.NewServer(http.HandlerFunc(c.serve))
	t.Cleanup(ts.Close)
	return c, ts
}

func (c *fakeController) serve(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	down, instance := c.down, c.instance
	c.mu.Unlock()

	if down {
		// A controller that is not there does not answer: aborting the
		// handler drops the connection, which is what the worker's HTTP
		// client sees when the process is gone.
		panic(http.ErrAbortHandler)
	}
	w.Header().Set(controllerInstanceHeader, instance)

	switch {
	case r.URL.Path == "/v1/workers/register":
		var reg registration
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &reg)
		c.mu.Lock()
		c.registrations = append(c.registrations, reg)
		c.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

	case r.URL.Path == "/v1/workers/w1/assignments":
		c.mu.Lock()
		a := c.assignment
		first := a != nil && !c.handedOut
		c.handedOut = c.handedOut || first
		c.mu.Unlock()
		if !first {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"assignments": []map[string]any{a}})

	case strings.HasSuffix(r.URL.Path, "/status"):
		var body struct {
			State string `json:"state"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.State == string(model.JobRunning) {
			c.mu.Lock()
			select {
			case <-c.running:
			default:
				close(c.running)
			}
			c.mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)

	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (c *fakeController) restart(instance string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.instance = instance
}

func (c *fakeController) setDown(down bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.down = down
}

func (c *fakeController) registered() []registration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]registration(nil), c.registrations...)
}

// shortenDuration swaps in a test-only value for one of the reconnection
// thresholds and puts the shipped one back afterwards.
func shortenDuration(t *testing.T, target *time.Duration, d time.Duration) {
	t.Helper()
	original := *target
	*target = d
	t.Cleanup(func() { *target = original })
}

// startWorker runs a worker against a fake controller with the loop timings
// wound down, and stops it when the test ends.
func startWorker(t *testing.T, url string, cfg Config) *Worker {
	t.Helper()
	cfg.ControllerURL = url
	cfg.Token = "wtok"
	if cfg.Host == "" {
		cfg.Host = "gpubox"
	}
	if cfg.Devices == nil {
		cfg.Devices = []DeviceConfig{{Name: "gpu0"}}
	}
	cfg.HeartbeatInterval = 20 * time.Millisecond
	cfg.PollWait = 50 * time.Millisecond

	w := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the worker did not stop after its context was cancelled")
		}
	})
	return w
}

// The defect itself: the controller restarts, the worker keeps heartbeating
// at the new process, and until this change it never told that process
// anything it could use to return a quarantined device to the pool.
func TestAControllerRestartSendsTheWorkerBackThroughRegistration(t *testing.T) {
	shortenDuration(t, &reregisterJitter, 0)

	c, ts := newFakeController(t)
	startWorker(t, ts.URL, Config{})

	require.Eventually(t, func() bool { return len(c.registered()) == 1 },
		5*time.Second, 10*time.Millisecond, "the worker never registered at all")

	// The controller comes back as a different process, on the same address
	// and the same database — the restart of 2026-08-18.
	c.restart("controller-b")

	require.Eventually(t, func() bool { return len(c.registered()) >= 2 },
		5*time.Second, 10*time.Millisecond,
		"the worker never registered with the restarted controller, so its quarantined devices have no proof to be cleared by")

	// And what it sent is a full registration, not a lesser ping: the same
	// boot ID and the same recovery proof the first one carried, which are
	// the two things store.AutoRecover and the reboot path actually read.
	regs := c.registered()
	require.Equal(t, regs[0], regs[1],
		"a re-registration must carry everything a registration carries")
	require.Len(t, regs[1].Devices, 1)
	require.Equal(t, "gpubox", regs[1].Host)
}

// The storm guard. A dropped request, a restarted proxy, one missed
// heartbeat: contact resumes with the same controller almost immediately, and
// nothing about this worker's registration has gone stale. Registering here
// would be a fleet-wide stampede triggered by one packet loss — and every one
// of those registrations reaps whatever the controller believes is in flight.
func TestATransientFailureDoesNotSendTheWorkerBackThroughRegistration(t *testing.T) {
	shortenDuration(t, &reregisterJitter, 0)
	// The silence horizon stays at its shipped five minutes: the point is
	// that a blip is nowhere near it.

	c, ts := newFakeController(t)
	startWorker(t, ts.URL, Config{})

	require.Eventually(t, func() bool { return len(c.registered()) == 1 },
		5*time.Second, 10*time.Millisecond, "the worker never registered at all")

	// Out of contact for a moment — several heartbeats and polls fail — and
	// then back, same controller.
	c.setDown(true)
	time.Sleep(100 * time.Millisecond)
	c.setDown(false)

	// Many polls' worth of opportunity to do the wrong thing.
	time.Sleep(300 * time.Millisecond)
	require.Len(t, c.registered(), 1,
		"a blip must not send the worker back through registration")
}

// Silence long enough for the controller to have written this worker's
// devices off IS worth registering for, even though the controller never
// restarted: only a registration carries the proof that clears the
// quarantine that silence caused.
func TestSilencePastTheWriteOffHorizonSendsTheWorkerBackThroughRegistration(t *testing.T) {
	shortenDuration(t, &reregisterJitter, 0)
	shortenDuration(t, &reregisterAfterSilence, 150*time.Millisecond)

	c, ts := newFakeController(t)
	startWorker(t, ts.URL, Config{})

	require.Eventually(t, func() bool { return len(c.registered()) == 1 },
		5*time.Second, 10*time.Millisecond, "the worker never registered at all")

	c.setDown(true)
	time.Sleep(250 * time.Millisecond)
	c.setDown(false)

	require.Eventually(t, func() bool { return len(c.registered()) >= 2 },
		5*time.Second, 10*time.Millisecond,
		"a worker whose devices the controller has had time to write off must present its proof again")
}

// The guard that stops the cure being the disease. UpsertWorker treats a
// registration as a fresh process announcing it runs nothing: it marks this
// worker's in-flight jobs lost and quarantines the devices under them with
// reason "registration". A reconnection that fired while a job was running
// would therefore kill that job and quarantine its GPU — precisely the damage
// this whole change exists to stop the controller doing.
func TestAWorkerWithAJobInHandDoesNotReRegisterUntilItIsDone(t *testing.T) {
	shortenDuration(t, &reregisterJitter, 0)

	c, ts := newFakeController(t)
	c.assignment = map[string]any{
		"job_id":    "job1",
		"device_id": "gpubox:gpu0",
		"command":   []string{"sh", "-c", "sleep 0.6"},
	}
	startWorker(t, ts.URL, Config{})

	select {
	case <-c.running:
	case <-time.After(10 * time.Second):
		t.Fatal("the worker never started the assigned job")
	}
	require.Len(t, c.registered(), 1)

	// The controller restarts mid-job. The worker keeps polling — and keeps
	// its hands off registration for as long as it is supervising anything.
	c.restart("controller-b")
	time.Sleep(300 * time.Millisecond)
	require.Len(t, c.registered(), 1,
		"registering here would have reaped the job this worker is running")

	// Once the job is done, the reconnection it deferred still happens: the
	// device it may need to prove clean has not stopped being quarantined.
	require.Eventually(t, func() bool { return len(c.registered()) >= 2 },
		5*time.Second, 10*time.Millisecond,
		"the deferred re-registration must still happen once the worker is idle")
}
