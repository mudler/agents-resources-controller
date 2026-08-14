package worker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

// registerBody mirrors enough of server.RegisterRequest's wire shape for
// these tests to assert on.
type registerBody struct {
	Host           string                       `json:"host"`
	Labels         map[string]map[string]string `json:"labels"`
	DeclaredLabels map[string]map[string]string `json:"declared_labels"`
	Sheet          string                       `json:"sheet"`
	DeviceSheets   map[string]string            `json:"device_sheets"`
}

// TestRegisterCarriesDetectedDeclaredLabelsAndSheets wires together Task 5's
// gatherLabels, Task 6's readSheets, and DeviceConfig.Labels, and proves
// they actually reach the FIRST /v1/workers/register call, not just some
// later push: an operator watching a freshly started worker must see real
// data immediately, not an empty set that only fills in after the first
// ProbeInterval tick (5 minutes by default).
func TestRegisterCarriesDetectedDeclaredLabelsAndSheets(t *testing.T) {
	sheetDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(sheetDir, "host.md"), []byte("# gpubox"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(sheetDir, "host.d"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sheetDir, "host.d", "gpu0.md"), []byte("gpu0 notes"), 0o644))

	registered := make(chan registerBody, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/workers/register":
			var body registerBody
			_ = json.NewDecoder(r.Body).Decode(&body)
			select {
			case registered <- body:
			default:
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL: ts.URL, Token: "t", Host: "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0", Labels: map[string]string{"owner": "team-a"}}},
		SheetDir:          sheetDir,
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          50 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	var body registerBody
	select {
	case body = <-registered:
	case <-time.After(10 * time.Second):
		t.Fatal("register never reached the controller")
	}

	require.Equal(t, "gpubox", body.Host)
	require.Equal(t, "# gpubox", body.Sheet)
	require.Equal(t, "gpu0 notes", body.DeviceSheets["gpu0"])
	require.Equal(t, map[string]string{"owner": "team-a"}, body.DeclaredLabels["gpu0"])
	// Built-ins (at minimum "cpus", via runtime.NumCPU) always produce a
	// host-wide fact on a real host, so Labels must be non-nil.
	require.NotNil(t, body.Labels, "a normal probe pass must never be sent as nil")
	require.NotEmpty(t, body.Labels[""], "built-in host facts must be present")
}

// TestProbeLoopPushesLabelsOnInterval proves the OTHER half of "pushed at
// registration and again on the probe interval": once ProbeInterval
// elapses, a fresh pass is pushed to the dedicated labels route — not by
// calling /v1/workers/register again (see internal/server's
// handlePushLabels doc comment for why that would be dangerous).
func TestProbeLoopPushesLabelsOnInterval(t *testing.T) {
	pushed := make(chan map[string]any, 4)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/workers/register":
			_ = json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})
		case "/v1/workers/w1/labels":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			select {
			case pushed <- body:
			default:
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Token:             "t",
		Host:              "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          50 * time.Millisecond,
		ProbeInterval:     150 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	select {
	case body := <-pushed:
		require.Equal(t, "gpubox", body["host"])
		labels, _ := body["labels"].(map[string]any)
		require.NotEmpty(t, labels, "the periodic push must carry the freshly probed facts")
	case <-time.After(8 * time.Second):
		t.Fatal("no periodic label push reached the controller within the probe interval")
	}
}

// TestProbePassNeverBlocksHeartbeat is the design point spelled out in the
// task brief: "The probe pass must never block the heartbeat: run it on its
// own goroutine and push the result when it completes." A worker late to
// its own heartbeat has its devices marked unknown by the controller, so a
// slow probe (here, a deliberately sleeping drop-in) must not stall
// heartbeatLoop's own ticker.
func TestProbePassNeverBlocksHeartbeat(t *testing.T) {
	probeDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(probeDir, "10-slow.sh"),
		[]byte("#!/bin/sh\nsleep 1\necho '{\"vendor\":\"nvidia\"}'\n"), 0o755))

	var (
		mu         sync.Mutex
		heartbeats int
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/workers/register":
			_ = json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})
		case "/v1/workers/w1/heartbeat":
			mu.Lock()
			heartbeats++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	const window = 4 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Token:             "t",
		Host:              "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: 100 * time.Millisecond,
		PollWait:          50 * time.Millisecond,
		ProbeDir:          probeDir,
		ProbeTimeout:      5 * time.Second,
		ProbeInterval:     150 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	<-ctx.Done()
	time.Sleep(200 * time.Millisecond) // let any in-flight heartbeat land

	mu.Lock()
	n := heartbeats
	mu.Unlock()

	// register() itself runs one synchronous (slow) probe pass before the
	// heartbeat loop even starts, costing ~1s of the 4s window. A heartbeat
	// loop truly stalled behind the REPEATED 1s probe passes that follow
	// (registration, then every ~150ms probeLoop tick after) would tick only
	// a handful of times total; one that is never blocked by them ticks at
	// its own 100ms cadence throughout the remaining ~3s, comfortably over
	// this threshold.
	require.Greater(t, n, 15,
		"the heartbeat loop must keep ticking on its own goroutine through slow, back-to-back probe passes; got %d heartbeats", n)
}

// TestPushLabelsOmitsADeviceWhoseProbeStartsFailing is the fix-round-1
// Critical finding, driven through the REAL gatherLabels path end to end —
// not a hand-built ProbeResult — exactly as the review asked: a drop-in
// probe reports real gpu0 facts on its first run (captured in the
// /v1/workers/register body), then starts failing on every subsequent run
// (simulating "a driver upgrade broke nvidia-smi"). The next probe-interval
// push must OMIT gpu0 from its labels map entirely — not send it as {},
// which the controller's ReplaceLabels would still turn into a clear — even
// though the pass's built-in host facts (at minimum "cpus") keep flowing
// and make the pass as a whole very much non-empty. This is precisely the
// scenario the ORIGINAL pass-wide-only guard could never catch, because a
// real host's built-ins essentially never produce an empty ProbeResult.
func TestPushLabelsOmitsADeviceWhoseProbeStartsFailing(t *testing.T) {
	probeDir := t.TempDir()
	marker := filepath.Join(probeDir, ".ran-once")
	script := "#!/bin/sh\n" +
		"if [ -f " + marker + " ]; then\n" +
		"  echo 'nvidia-smi: Failed to initialize NVML' >&2\n" +
		"  exit 1\n" +
		"fi\n" +
		"touch " + marker + "\n" +
		`echo '{"gpu0.vram":"80G","gpu0.model":"a100"}'` + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(probeDir, "10-gpu.sh"), []byte(script), 0o755))

	registered := make(chan registerBody, 1)
	pushed := make(chan map[string]any, 4)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/workers/register":
			var body registerBody
			_ = json.NewDecoder(r.Body).Decode(&body)
			select {
			case registered <- body:
			default:
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})
		case "/v1/workers/w1/labels":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			select {
			case pushed <- body:
			default:
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Token:             "t",
		Host:              "gpubox",
		Devices:           []worker.DeviceConfig{{Name: "gpu0"}},
		HeartbeatInterval: 500 * time.Millisecond,
		PollWait:          50 * time.Millisecond,
		ProbeDir:          probeDir,
		ProbeInterval:     150 * time.Millisecond,
	})
	go func() { _ = wk.Start(ctx) }()

	// The FIRST pass (inside register()) must have captured gpu0's real
	// facts — proving the probe script and the pipeline both work before
	// asserting on the interesting (failure) case.
	var regBody registerBody
	select {
	case regBody = <-registered:
	case <-time.After(10 * time.Second):
		t.Fatal("register never reached the controller")
	}
	require.Equal(t, "80G", regBody.Labels["gpu0"]["vram"], "sanity: the first pass must have captured gpu0's real facts")

	// A subsequent probe-interval push must have run the (now failing)
	// probe again and omitted gpu0 entirely.
	var pushBody map[string]any
	select {
	case pushBody = <-pushed:
	case <-time.After(10 * time.Second):
		t.Fatal("no periodic label push reached the controller")
	}
	labels, _ := pushBody["labels"].(map[string]any)
	require.NotNil(t, labels, "host-wide built-ins (at minimum cpus) must still be present")
	_, hasHostKey := labels[""]
	require.True(t, hasHostKey, "host-wide facts must be unaffected by gpu0's probe failure")
	_, hasGPU0Key := labels["gpu0"]
	require.False(t, hasGPU0Key,
		"a device whose probe started failing must be OMITTED from the labels push, not sent as {} — "+
			"sending {} would have the controller clear its previously detected vram/model facts")
}
