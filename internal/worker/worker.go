package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
)

type assignment struct {
	JobID              string            `json:"job_id"`
	DeviceID           string            `json:"device_id"`
	Command            []string          `json:"command"`
	Cwd                string            `json:"cwd"`
	Env                map[string]string `json:"env"`
	MaxRuntimeSeconds  int               `json:"max_runtime_seconds,omitempty"`
	IdleTimeoutSeconds int               `json:"idle_timeout_seconds,omitempty"`
	// Submitter identifies who submitted this job, so a lifecycle hook's
	// RC_SUBMITTER can name them.
	Submitter string `json:"submitter,omitempty"`
}

// deviceSpec is what this worker declares about one of its devices at
// registration: its name and, if the operator configured one, the runtime
// ceiling the controller should enforce against any job scheduled onto it.
type deviceSpec struct {
	Name              string `json:"name"`
	MaxRuntimeSeconds int    `json:"max_runtime_seconds,omitempty"`
}

// registerRequest mirrors server.RegisterRequest: what this worker posts
// once at startup. Labels and DeclaredLabels are deliberately NOT
// `omitempty` — see labelsPayload for why a nil map (omitted from the wire
// entirely) must mean something different from a non-nil, empty one. Sheet
// is a pointer for the identical reason, fixed in fix round 1: readSheets
// can fail to read host.md for a reason other than "it doesn't exist"
// (permission denied, ...), and a plain string could not tell that case
// apart from "the host genuinely has no sheet" — which would let a
// transient chmod erase a previously good, stored sheet the next time this
// worker registers or pushes. nil means "leave the stored host sheet
// alone"; a non-nil pointer, even to "", is an explicit, trustworthy report.
type registerRequest struct {
	Host           string                       `json:"host"`
	BootID         string                       `json:"boot_id,omitempty"`
	Devices        []deviceSpec                 `json:"devices"`
	Labels         map[string]map[string]string `json:"labels"`
	DeclaredLabels map[string]map[string]string `json:"declared_labels"`
	Sheet          *string                      `json:"sheet,omitempty"`
	DeviceSheets   map[string]string            `json:"device_sheets,omitempty"`
}

// labelsPushRequest mirrors server.LabelsPushRequest: what this worker posts
// on every probe-interval pass AFTER its initial registration. It carries no
// boot_id and no ceiling — see pushLabels and handlePushLabels's own doc
// comment for why this is a route separate from register() entirely, not a
// repeated call to it. Sheet has the same nil-vs-empty contract as
// registerRequest.Sheet.
type labelsPushRequest struct {
	Host           string                       `json:"host"`
	Devices        []string                     `json:"devices"`
	Labels         map[string]map[string]string `json:"labels"`
	DeclaredLabels map[string]map[string]string `json:"declared_labels"`
	Sheet          *string                      `json:"sheet,omitempty"`
	DeviceSheets   map[string]string            `json:"device_sheets,omitempty"`
}

// pollResponse mirrors server.PollResponse: an envelope carrying both new
// work and kill requests, so a kill reaches the worker as fast as an
// assignment does rather than waiting on a separate channel.
type pollResponse struct {
	Assignments []assignment `json:"assignments"`
	Kills       []string     `json:"kills,omitempty"`
}

// heartbeatRequest mirrors server.HeartbeatRequest: the jobs this worker is
// actually supervising. The controller renews the lease of each named job and
// nothing else, so this list — not the mere fact that the heartbeat arrived —
// is what keeps a device's lease alive.
type heartbeatRequest struct {
	RunningJobIDs []string `json:"running_job_ids,omitempty"`
}

type Worker struct {
	cfg      Config
	http     *http.Client
	workerID string

	mu      sync.Mutex
	running map[string]context.CancelFunc // jobs currently in flight on this worker
	started map[string]struct{}           // every job ID this process has ever started; never deleted

	wg sync.WaitGroup // one entry per in-flight job goroutine; Start waits on this before returning

	// hooks is this worker's resolved lifecycle-hook configuration, keyed
	// by full device ID ("host:name"), built once at New() from cfg.
	hooks map[string]hookSpec
	// hooksMu guards deviceHooks (map insert/lookup only); each entry's own
	// mutex guards everything else about that one device's held/linger
	// state, so devices never contend with each other.
	hooksMu     sync.Mutex
	deviceHooks map[string]*deviceHookState
}

func New(cfg Config) *Worker {
	// Defaults are applied here, not only in LoadConfig: a Config assembled
	// in code reaches this constructor without ever passing through the file
	// loader, and a zero hook timeout would mean no MaxRuntime — a hook that
	// wedges then runs forever and takes the worker's startup with it. See
	// Config.withDefaults.
	cfg = cfg.withDefaults()

	hooks := make(map[string]hookSpec, len(cfg.Devices))
	for _, d := range cfg.Devices {
		hooks[cfg.Host+":"+d.Name] = hookSpec{
			onAcquire:     d.OnAcquire,
			onRelease:     d.OnRelease,
			timeout:       d.HookTimeout,
			releaseLinger: d.ReleaseLinger,
		}
	}
	return &Worker{
		cfg:         cfg,
		http:        &http.Client{Timeout: 2 * time.Minute},
		running:     map[string]context.CancelFunc{},
		started:     map[string]struct{}{},
		hooks:       hooks,
		deviceHooks: map[string]*deviceHookState{},
	}
}

// shutdownGrace bounds how long Start waits, once ctx is cancelled, for jobs
// already running to actually finish and report before giving up on them. It
// must comfortably cover Run's own worst case (SIGTERM, up to GraceCeiling —
// default 10s — then SIGKILL, then up to a couple of seconds for a stubborn
// process-group straggler: ~12s total) plus the terminal report's retry
// budget — bounded to ~28s worst case by terminalReportAttemptTimeout, see
// that constant for the arithmetic — for ~40s total, or a routine shutdown
// would itself trigger the "abandoned job" path this exists to avoid. That
// retry budget is only a real bound because each attempt has its own
// timeout: without terminalReportAttemptTimeout, a blackholed controller
// (connection accepted, nothing ever comes back) could let a single attempt
// run for w's full 2-minute http.Client timeout, and five of those blow
// through this 45s many times over.
//
// One path can still exceed it: a job whose acquire hook failed makes two
// retried calls back to back (its terminal report, then the device fault
// report), so an unreachable controller costs up to ~56s there. That trade
// is deliberate — a device the hook has just called unusable must not stay
// schedulable because we were in a hurry to exit — and it costs a log line,
// not correctness: the job in question never started, so there is no
// process to abandon.
const shutdownGrace = 45 * time.Second

// startupReleaseBudget bounds the whole startup reconciliation pass — every
// device's release hook together, not each one — so a slow hook delays this
// worker's first poll by a known amount rather than an unbounded one. See
// runStartupReleaseHooks for why the sum, not the per-hook timeout, is the
// number that matters.
//
// A var, not a const, so tests can shrink it.
var startupReleaseBudget = 2 * time.Minute

func (w *Worker) Start(ctx context.Context) error {
	if err := w.register(ctx); err != nil {
		return err
	}
	// The heartbeat starts BEFORE the startup reconciliation pass, not after
	// it. The pass runs operator scripts of unknown duration (a stop-LocalAI
	// hook is allowed a full hook timeout each), and while it ran the worker
	// used to be silent: past the controller's device-freshness window, this
	// worker's own devices get marked unknown before it has polled even
	// once. Heartbeating through the pass costs nothing — the heartbeat
	// names the jobs this worker is supervising, and during the pass that
	// list is correctly empty, so no lease is renewed by starting it early.
	go w.heartbeatLoop(ctx)

	// The probe loop, like the heartbeat, runs on its own goroutine from
	// here on: a probe pass can take up to probePassBudget (30s, and a
	// single wedged nvidia-smi or drop-in probe could in principle push
	// close to it), and it must never delay a heartbeat tick on the other
	// goroutine — a worker late to its own heartbeat has its devices marked
	// unknown. The first pass already ran synchronously inside register()
	// above; this only fires again once ProbeInterval has actually elapsed.
	go w.probeLoop(ctx)

	// Before this worker ever polls for work, run the release hook of every
	// device it declares one for. A worker that crashed (kill -9, an OOM,
	// a crashed host) never got to run its normal release-on-idle path for
	// whatever device it last held, and a clean shutdown followed by a
	// restart lands here too — so this single pass, run unconditionally on
	// every start, heals both cases without needing to tell them apart.
	// This is exactly why the README requires the operator's release hook
	// to be idempotent: this call has no idea whether the service it
	// targets is already stopped.
	w.runStartupReleaseHooks(ctx)
	w.pollLoop(ctx)

	// ctx is done: stop waiting for new work, but a job already running here
	// must not be abandoned. Its process group survives this process exiting
	// (Run starts it with Setpgid), so returning now would orphan it on the
	// device with nobody left to drive the SIGTERM -> grace -> SIGKILL
	// sequence, and a restarted worker's register call would hand the same
	// device out to a second job while the first is still sitting on it.
	waitDone := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(shutdownGrace):
		w.mu.Lock()
		abandoned := make([]string, 0, len(w.running))
		for id := range w.running {
			abandoned = append(abandoned, id)
		}
		w.mu.Unlock()
		if len(abandoned) > 0 {
			slog.Error("shutdown grace period expired with jobs still running; abandoning them", "jobs", abandoned)
		}
	}

	// Jobs are done (or given up on), so any release those jobs armed is now
	// sitting on a bare timer goroutine nothing waits for. Fire them here,
	// bounded: returning without doing so is what leaves a cleanly stopped
	// node with the operator's inference server still stopped and no job
	// running — a state nothing but a restart heals.
	w.drainPendingReleases(releaseDrainBudget)
	return ctx.Err()
}

// cudaDeviceIndex derives a CUDA_VISIBLE_DEVICES value from a device ID of
// the form "host:name" by taking the trailing run of digits off the NAME —
// "gpubox:gpu1" yields ("1", true). A name with no trailing digits (or a
// malformed ID with no ":") yields ("", false): silently setting a wrong
// index would be worse than leaving the variable unset.
func cudaDeviceIndex(deviceID string) (string, bool) {
	_, name, ok := strings.Cut(deviceID, ":")
	if !ok || name == "" {
		return "", false
	}
	i := len(name)
	for i > 0 && name[i-1] >= '0' && name[i-1] <= '9' {
		i--
	}
	if i == len(name) {
		return "", false // no trailing digits
	}
	digits := name[i:]
	if _, err := strconv.Atoi(digits); err != nil {
		return "", false
	}
	return digits, true
}

func (w *Worker) do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, w.cfg.ControllerURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+w.cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	return w.http.Do(req)
}

func (w *Worker) register(ctx context.Context) error {
	devices := make([]deviceSpec, 0, len(w.cfg.Devices))
	for _, d := range w.cfg.Devices {
		devices = append(devices, deviceSpec{
			Name:              d.Name,
			MaxRuntimeSeconds: int(d.MaxRuntime.Seconds()),
		})
	}

	// The very first probe pass and sheet read run synchronously, right
	// here, so registration carries real data on the worker's first
	// contact with the controller rather than an empty set that only fills
	// in once the probe loop's first tick fires (up to ProbeInterval — 5
	// minutes by default — later). This can add up to probePassBudget
	// (30s) to Start before the first heartbeat, which is acceptable for
	// the same reason runStartupReleaseHooks already can: it happens once,
	// before this worker's devices are relied on for anything.
	res := w.gatherLabels(ctx)
	labels := labelsPayload(res)
	if labels == nil {
		slog.Error("initial probe pass produced no facts at all; registering without detected labels",
			"host", w.cfg.Host)
	}
	declared := declaredLabelsPayload(w.cfg.Devices)
	sheet, deviceSheets := sheetPayload(w.cfg.SheetDir, w.cfg.Devices)

	payload, err := json.Marshal(registerRequest{
		Host:           w.cfg.Host,
		BootID:         BootID(),
		Devices:        devices,
		Labels:         labels,
		DeclaredLabels: declared,
		Sheet:          sheet,
		DeviceSheets:   deviceSheets,
	})
	if err != nil {
		return err
	}
	resp, err := w.do(ctx, http.MethodPost, "/v1/workers/register", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("register: controller returned %s", resp.Status)
	}
	var out struct {
		WorkerID string `json:"worker_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	w.workerID = out.WorkerID
	slog.Info("registered", "worker_id", w.workerID, "host", w.cfg.Host, "devices", w.cfg.Devices)
	return nil
}

// runStartupReleaseHooks runs the on_release hook of every device this
// worker declares one for, sequentially, before Start's first poll. It
// carries no job identity — RC_JOB_ID and RC_SUBMITTER are empty, exactly as
// they are for any release this worker did not itself trigger — and it does
// not touch this device's held state either way: a fresh process has never
// held anything, and the whole point of this pass is to be safe to run
// whether or not the previous process's job actually still had it acquired.
// A failure here is logged the same way any other release-hook failure is:
// loudly, but never as a reason to refuse to start.
func (w *Worker) runStartupReleaseHooks(ctx context.Context) {
	// The pass as a whole is bounded, not just each hook in it. Per-device
	// timeouts alone make the worst case the SUM over every device this host
	// declares, which on an 8-GPU box with the default 60s timeout is eight
	// minutes of a worker that has registered but never polled — and a
	// controller that has long since marked its devices unknown. A budget
	// here turns "startup may be delayed by an unknown amount" into "startup
	// is delayed by at most this". Whatever the budget cuts short is exactly
	// what the NEXT start's pass will attempt again, and a release hook is
	// required to be idempotent precisely so that is safe.
	ctx, cancel := context.WithTimeout(ctx, startupReleaseBudget)
	defer cancel()

	for deviceID, spec := range w.hooks {
		if spec.onRelease == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			slog.Error("startup release pass exceeded its budget; remaining devices skipped",
				"device", deviceID, "budget", startupReleaseBudget)
			return
		}
		out, err := w.runHook(ctx, spec.onRelease, "release", deviceID, "", "", spec.timeout)
		if err != nil {
			slog.Error("startup release hook failed", "device", deviceID, "err", err, "output_tail", tailOutput(out))
			continue
		}
		slog.Debug("startup release hook ran", "device", deviceID)
	}
}

// reportFault tells the controller a device's on_acquire hook failed, so it
// quarantines the device unhealthy with quarantine reason "fault" — the one
// kind a proven reboot does not clear, because a reboot proves no process
// survived but nothing about the hardware (or, here, the service the hook
// couldn't stop).
//
// It is retried on exactly the same budget as the terminal job report,
// because it carries the same weight: it is the only thing standing between
// a device the hook has just said is unusable and the next job the
// controller schedules onto it. A single dropped attempt used to leave the
// device schedulable while this worker logged nothing but one line — the
// next job lands on a GPU whose VRAM is still held, which is the OOM this
// project exists to prevent.
//
// A 4xx is not retried: an unknown device (404), a rejected token (401/403)
// or a malformed body (400) will answer the same way five times over. Only
// transport failures and 5xx — the transient kinds — are worth another
// attempt.
func (w *Worker) reportFault(ctx context.Context, deviceID, reason string) {
	payload, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		slog.Error("marshal fault report", "device", deviceID, "err", err)
		return
	}

	backoff := 200 * time.Millisecond
	for attempt := 1; attempt <= terminalReportAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, terminalReportAttemptTimeout)
		ok, retryable := w.postFault(attemptCtx, deviceID, payload)
		cancel()
		if ok {
			return
		}
		if !retryable || attempt == terminalReportAttempts {
			break
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	slog.Error("device fault report failed; the device stays schedulable despite a failed acquire hook",
		"device", deviceID)
}

// postFault makes one fault-report attempt. It reports whether the
// controller accepted it and, if not, whether trying again could plausibly
// help.
func (w *Worker) postFault(ctx context.Context, deviceID string, payload []byte) (ok, retryable bool) {
	resp, err := w.do(ctx, http.MethodPost, "/v1/devices/"+deviceID+"/fault", bytes.NewReader(payload))
	if err != nil {
		slog.Warn("report device fault failed", "device", deviceID, "err", err)
		return false, true
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		return true, false
	}
	slog.Error("report device fault rejected", "device", deviceID, "status", resp.Status)
	return false, resp.StatusCode/100 == 5
}

// supervisedJobIDs is what this worker tells the controller it is actually
// running: the jobs it currently has a live supervision goroutine for. A job
// stays in this set until its terminal report has been made (or definitively
// given up on), because that is precisely the window during which its device
// is genuinely occupied.
//
// It is deliberately NOT "every job the controller assigned me". The
// controller renews only the leases of the jobs named here (see
// store.RecordHeartbeat), so naming a job this process has no handle on would
// re-create the bug that motivated this: a lease renewed forever for a
// process that does not exist.
func (w *Worker) supervisedJobIDs() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := make([]string, 0, len(w.running))
	for id := range w.running {
		ids = append(ids, id)
	}
	return ids
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			payload, err := json.Marshal(heartbeatRequest{RunningJobIDs: w.supervisedJobIDs()})
			if err != nil {
				slog.Warn("heartbeat marshal failed", "err", err)
				continue
			}
			resp, err := w.do(ctx, http.MethodPost,
				"/v1/workers/"+w.workerID+"/heartbeat", bytes.NewReader(payload))
			if err != nil {
				slog.Warn("heartbeat failed", "err", err)
				continue
			}
			resp.Body.Close()
		}
	}
}

// probeLoop re-runs a full probe pass every ProbeInterval and pushes
// whatever it found to the controller. It runs on its own goroutine (see
// Start), so a slow pass here never delays a heartbeat tick on the other
// one: pushLabels runs synchronously within THIS loop, not spawned
// per-tick, so two probe passes are never running concurrently against the
// same host (wasteful — nvidia-smi and any drop-in probes would be invoked
// twice at once for no benefit) — the cost of that choice is simply that a
// pass overrunning ProbeInterval delays the next tick's pass, which is
// exactly as acceptable as gatherLabels' own probePassBudget comment
// already argues: whatever one pass misses, the next one picks up.
func (w *Worker) probeLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.ProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.pushLabels(ctx)
		}
	}
}

// pushLabels runs one probe pass and pushes its result, plus this host's
// current usage sheets, to the controller via the dedicated labels route —
// NOT by calling register() again. See handlePushLabels's doc comment
// (internal/server/worker_api.go) for why: re-registering a worker that is
// still alive is indistinguishable, server-side, from a fresh process that
// cannot possibly be running anything, and would reap every job this worker
// is actually supervising right now.
func (w *Worker) pushLabels(ctx context.Context) {
	res := w.gatherLabels(ctx)
	labels := labelsPayload(res)
	if labels == nil {
		slog.Error("probe pass produced no facts at all; pushing without a labels field so previously detected labels are not wiped",
			"host", w.cfg.Host)
	}
	declared := declaredLabelsPayload(w.cfg.Devices)
	sheet, deviceSheets := sheetPayload(w.cfg.SheetDir, w.cfg.Devices)

	names := make([]string, 0, len(w.cfg.Devices))
	for _, d := range w.cfg.Devices {
		names = append(names, d.Name)
	}

	payload, err := json.Marshal(labelsPushRequest{
		Host:           w.cfg.Host,
		Devices:        names,
		Labels:         labels,
		DeclaredLabels: declared,
		Sheet:          sheet,
		DeviceSheets:   deviceSheets,
	})
	if err != nil {
		slog.Error("marshal labels push", "err", err)
		return
	}
	resp, err := w.do(ctx, http.MethodPost, "/v1/workers/"+w.workerID+"/labels", bytes.NewReader(payload))
	if err != nil {
		slog.Warn("push labels failed", "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		slog.Warn("push labels rejected", "status", resp.Status)
	}
}

// labelsPayload turns one gatherLabels pass into the wire shape a register
// or labels-push request carries: "" for host-wide facts, a device's bare
// name for everything scoped to it.
//
// This is the guard standing between a probe outage and a device's detected
// labels being silently wiped, and it is applied PER DEVICE, not once for
// the whole pass — a fix-round-1 review finding caught the original,
// pass-wide version of this guard being inert in production:
// builtinLabels() always yields at least "cpus" from runtime.NumCPU(), so
// res.Host is non-empty on virtually every real host, which made the
// pass-wide "everything is empty" check unreachable outside a contrived
// unit test. The actual danger it needs to prevent is narrower and more
// common: nvidia-smi (present on PATH but broken — the realistic shape of
// "a driver upgrade broke it", see nvidiaLabels) or a drop-in probe failing
// while the rest of the pass — built-ins included — keeps succeeding. That
// must not turn a device's LAST KNOWN GPU/VRAM facts into nothing just
// because host-wide facts happened to still come through.
//
// The controller's ReplaceLabels makes the stored set for one device and one
// source exactly what it is given, so an empty submap means "clear" —
// correct for a device whose probes ran and legitimately found nothing (a
// card removed), wrong for a device whose probes simply failed to run this
// pass. Those two cases produce an identical empty per-device map, so the
// only place left to distinguish them is res.Failed: when nothing capable of
// producing device-scoped facts failed anywhere this pass, an empty device
// is trustworthy and is sent as an explicit clear; when something did fail,
// every device that came back empty is OMITTED from the returned map
// entirely (its key is simply absent, not present as {}) rather than sent
// as a clear — see applyDeviceFacts (server-side) for why omission, not an
// empty map, is what actually protects it: only a present key gets
// ReplaceLabels called on it at all.
//
// A nil return — meaning "say nothing about detected labels at all" — is
// reserved for the one case where there is nothing to say about anything:
// no host facts and no device facts whatsoever.
func labelsPayload(res ProbeResult) map[string]map[string]string {
	allEmpty := len(res.Host) == 0
	if allEmpty {
		for _, m := range res.Device {
			if len(m) > 0 {
				allEmpty = false
				break
			}
		}
	}
	if allEmpty {
		return nil
	}

	out := make(map[string]map[string]string, len(res.Device)+1)
	if len(res.Host) > 0 {
		out[""] = res.Host
	}
	for name, m := range res.Device {
		if len(m) > 0 {
			out[name] = m
			continue
		}
		if !res.Failed {
			// Confirmed clean: this device's probes ran (or there simply
			// were none configured) and nothing failed anywhere this
			// pass, so an empty result here is trustworthy.
			out[name] = m
		}
		// else: something that can produce device-scoped facts failed
		// this pass, so this device's empty result proves nothing about
		// it — omit it so applyDeviceFacts leaves it untouched.
	}
	return out
}

// declaredLabelsPayload turns this worker's configured, operator-asserted
// device labels into the same wire shape labelsPayload produces. Unlike
// detected labels there is no probe to fail here: worker.yaml IS the
// declaration of intent, so this always sends the config verbatim,
// including devices that declare none — an operator who removes a declared
// label and restarts must see it actually cleared, the same way an unset
// max_runtime clears a previously-declared ceiling (see register's own
// ceiling-clearing behaviour, server-side).
func declaredLabelsPayload(devices []DeviceConfig) map[string]map[string]string {
	out := make(map[string]map[string]string, len(devices))
	for _, d := range devices {
		out[d.Name] = d.Labels
	}
	return out
}

// sheetPayload wraps readSheets into the wire shape a register or
// labels-push request carries. A nil sheet return means "host.md could not
// be read this round (for a reason other than it simply not existing);
// leave whatever the controller already has for this host's sheet alone" —
// see registerRequest.Sheet's own doc comment for why a plain string cannot
// carry that distinction. deviceSheets already carries the equivalent
// per-device signal on its own: readSheets omits a device's key entirely
// when ITS sheet could not be read, which is exactly the shape
// applyDeviceFacts (server-side) already expects.
func sheetPayload(dir string, devices []DeviceConfig) (sheet *string, deviceSheets map[string]string) {
	host, perDevice, err := readSheets(dir, devices)
	if err != nil {
		slog.Warn("read host usage sheet; leaving the previously stored copy alone", "err", err)
		return nil, perDevice
	}
	return &host, perDevice
}

// minPollInterval floors the gap between successive polls when the previous
// one returned no work and no error. A conforming controller already blocks
// for close to PollWait before answering empty-handed, so this is normally a
// no-op. It exists for a non-conforming controller or a proxy in front of it
// that answers the long-poll instantly: without a floor here, pollLoop would
// spin as fast as the network round trip allows — thousands of requests per
// second at the controller from a single worker, enough for one misbehaving
// proxy to stall scheduling fleet-wide rather than just for this one worker.
const minPollInterval = 250 * time.Millisecond

// pollLoop long-polls for work. A failed poll is retried: the controller being
// down must never abandon jobs already running here.
func (w *Worker) pollLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		resp, err := w.poll(ctx)
		if err != nil {
			slog.Warn("poll failed", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		// A kill for a job already tracked in w.running (registration happens
		// inside execute(), in the goroutine spawned below, NOT synchronously
		// here) reaches it via cancelRunning. But a job can also arrive with
		// a kill for its OWN job ID in this very same response — e.g. `rc
		// kill` landing between ScheduleOnce assigning it and this poll —
		// and at that moment it is not in w.running yet: nothing has spawned
		// it. cancelRunning alone would silently no-op and the assignment
		// loop below would start it anyway. killSet lets the assignment loop
		// recognise and drop that case before it ever calls execute().
		killSet := make(map[string]struct{}, len(resp.Kills))
		for _, jobID := range resp.Kills {
			killSet[jobID] = struct{}{}
			w.cancelRunning(jobID)
		}

		var toStart []assignment
		for _, a := range resp.Assignments {
			if _, killed := killSet[a.JobID]; killed {
				// The controller has already been told to kill this job:
				// starting it now would allocate VRAM on a device someone
				// may already be waiting for, and the user who typed
				// `rc kill` has every reason to believe nothing ran. Report
				// it terminally instead of spawning it, so the controller
				// frees the device and the waiting client learns the
				// outcome rather than watching the job sit assigned.
				w.wg.Add(1)
				go func(jobID string) {
					defer w.wg.Done()
					w.reportPreKilled(ctx, jobID)
				}(a.JobID)
				continue
			}
			toStart = append(toStart, a)
		}

		if len(toStart) == 0 {
			// Whether this poll carried only kills (delivered on their own,
			// or dropped above alongside their own assignment), or nothing
			// at all, there is no new work to start locally: any kill just
			// delivered was already actioned above (idempotently, if
			// re-delivered), so the same floor applies as the empty case —
			// no reason to hammer the controller faster than that.
			select {
			case <-ctx.Done():
				return
			case <-time.After(minPollInterval):
			}
			continue
		}
		for _, a := range toStart {
			// Registered here, synchronously in the poll loop, so that by the
			// time pollLoop returns (ctx already done) every goroutine it has
			// ever started is already accounted for in wg — Start's shutdown
			// wait can never race a wg.Add happening after it started
			// wg.Wait(). (w.running itself is only populated once execute()
			// runs, inside the spawned goroutine below — see the killSet
			// comment above for why that distinction matters.)
			w.wg.Add(1)
			go func(a assignment) {
				defer w.wg.Done()
				w.execute(ctx, a)
			}(a)
		}
	}
}

func (w *Worker) poll(ctx context.Context) (pollResponse, error) {
	path := fmt.Sprintf("/v1/workers/%s/assignments?wait=%s", w.workerID, w.cfg.PollWait)
	resp, err := w.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return pollResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return pollResponse{}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return pollResponse{}, fmt.Errorf("controller returned %s", resp.Status)
	}
	var out pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return pollResponse{}, err
	}
	return out, nil
}

// cancelRunning cancels the context backing a job this worker currently has
// running, so Run's existing SIGTERM -> grace -> SIGKILL path tears down its
// process group — the same path a shutdown or the job's own watchdog uses,
// not a second one. A job ID this worker is not (or no longer) running is
// silently ignored: a kill flag with nothing local left to act on is not an
// error, and calling an already-fired CancelFunc again is a harmless no-op,
// so a kill request re-delivered on a later poll costs nothing.
func (w *Worker) cancelRunning(jobID string) {
	w.mu.Lock()
	cancel, ok := w.running[jobID]
	w.mu.Unlock()
	if ok {
		cancel()
	}
}

// reportPreKilled handles a job that arrived in the same poll response as a
// kill request for its own job ID: it must never be spawned, but it still
// needs a terminal report — otherwise the controller leaves its device
// leased and a client waiting on it never learns the outcome. w.started
// marks it the same way execute() does, so this worker process never later
// starts it either (e.g. if the same job is re-delivered on a subsequent
// poll before the controller's own state catches up) and a re-delivered
// kill for it here is a harmless no-op.
func (w *Worker) reportPreKilled(ctx context.Context, jobID string) {
	w.mu.Lock()
	if _, done := w.started[jobID]; done {
		w.mu.Unlock()
		return
	}
	w.started[jobID] = struct{}{}
	w.mu.Unlock()

	// Detached from ctx for the same reason execute()'s reportCtx is: a
	// shutdown racing this must not silently drop the report a client may be
	// waiting on.
	reportCtx := context.WithoutCancel(ctx)
	body := map[string]any{
		"state":     model.JobKilled,
		"worker_id": w.workerID,
		"reason":    "killed before this worker started it",
	}
	w.reportTerminalWithRetry(reportCtx, jobID, body)
}

func (w *Worker) execute(ctx context.Context, a assignment) {
	w.mu.Lock()
	if _, busy := w.running[a.JobID]; busy {
		w.mu.Unlock()
		return // a duplicate poll result must not start the job twice, concurrently
	}
	if _, done := w.started[a.JobID]; done {
		w.mu.Unlock()
		return // this worker process already ran this job ID to completion; never again
	}
	w.started[a.JobID] = struct{}{}
	jobCtx, cancel := context.WithCancel(ctx)
	w.running[a.JobID] = cancel
	w.mu.Unlock()

	defer func() {
		cancel()
		w.mu.Lock()
		delete(w.running, a.JobID)
		w.mu.Unlock()
	}()

	// Detached from ctx: shutdown cancels ctx to signal Run (via jobCtx below)
	// to SIGTERM the job, but the HTTP calls that record the outcome — both
	// status reports and the final log flush — must survive that same
	// cancellation, or a job that ran to completion during shutdown still has
	// its result silently discarded. This is the same reasoning that already
	// has the log sink use the parent context rather than the job's; it now
	// covers shutdown too.
	reportCtx := context.WithoutCancel(ctx)

	// Ensure the device is held — running its on_acquire hook, unless an
	// earlier job in this same back-to-back run already did and the device
	// was never released — before this job ever touches it. spec is looked
	// up once here and reused below for scheduleRelease, so both halves of
	// this job's hook lifecycle agree on the same timeout/linger.
	// jobCtx, not ctx: the hook is supervised like the job it is running for,
	// so `rc kill` and a worker shutdown both reach it — and, crucially, so
	// the failure below can tell "the hook says this GPU isn't free" from
	// "we interrupted the hook ourselves".
	spec := w.hooks[a.DeviceID]
	acq := w.acquireDevice(jobCtx, a.DeviceID, spec, a.JobID, a.Submitter)
	if acq.err != nil {
		// A hook we cancelled ourselves — a shutdown, or a kill for this very
		// job — is not evidence of anything about the device. Reporting a
		// fault here quarantines the GPU with the one quarantine reason a
		// reboot deliberately does not clear, so a `systemctl restart
		// rc-worker` landing mid-hook would brick the device until an admin
		// noticed and ran `rc clear`. Report the job through the same
		// terminal path a kill uses and leave the device alone.
		if jobCtx.Err() != nil {
			slog.Warn("acquire hook cancelled; job not started, device not faulted",
				"job", a.JobID, "device", a.DeviceID, "err", acq.err)
			w.reportTerminalWithRetry(reportCtx, a.JobID, map[string]any{
				"state": model.JobKilled, "worker_id": w.workerID,
				"reason": "cancelled during acquire hook: " + acq.err.Error(),
			})
			return
		}
		// The acquire hook failed (non-zero exit or timeout): the job never
		// runs at all, it is reported failed with the hook's own output as
		// the reason so the operator sees why from `rc ps`, and the device
		// is quarantined as a fault rather than handed out again next poll.
		slog.Error("acquire hook failed; job will not run", "job", a.JobID, "device", a.DeviceID, "err", acq.err)
		reason := "acquire hook failed: " + acq.err.Error()
		if tail := tailOutput(acq.output); tail != "" {
			reason += "\n" + tail
		}
		w.reportTerminalWithRetry(reportCtx, a.JobID, map[string]any{
			"state": model.JobFailed, "worker_id": w.workerID, "reason": reason,
		})
		w.reportFault(reportCtx, a.DeviceID, reason)
		return
	}

	// worker_id is required: the controller rejects a status report for a job
	// it did not assign to this worker, so one worker cannot free another's
	// device out from under a running process. (The controller already
	// transitioned this job to "running" the moment it handed out the
	// assignment, so this call is normally a harmless no-op; it is kept as
	// belt-and-suspenders in case that ever changes.)
	w.report(reportCtx, a.JobID, map[string]any{"state": model.JobRunning, "worker_id": w.workerID})

	env := map[string]string{}
	for k, v := range a.Env {
		env[k] = v
	}
	env["RC_JOB_ID"] = a.JobID
	env["RC_DEVICE"] = a.DeviceID
	// Scheduling a device is meaningless if the process is free to touch
	// every GPU on the box (the exact OOM this project exists to prevent on
	// a multi-GPU host). The convention, documented in the README: the index
	// is the trailing integer of the device NAME, the part after "host:" —
	// "gpubox:gpu1" yields "1". A name with no trailing integer sets
	// nothing rather than guess wrong, and an operator-supplied value that
	// already arrived in the job's env (checked above via a.Env) is never
	// overridden.
	if _, set := env["CUDA_VISIBLE_DEVICES"]; !set {
		if idx, ok := cudaDeviceIndex(a.DeviceID); ok {
			env["CUDA_VISIBLE_DEVICES"] = idx
		}
	}

	sink := &logSink{w: w, jobID: a.JobID, ctx: reportCtx}
	res := Run(jobCtx, JobSpec{
		Command:     a.Command,
		Cwd:         a.Cwd,
		Env:         env,
		MaxRuntime:  time.Duration(a.MaxRuntimeSeconds) * time.Second,
		IdleTimeout: time.Duration(a.IdleTimeoutSeconds) * time.Second,
	}, sink)
	sink.Flush()

	state := model.JobSucceeded
	switch {
	case res.Killed:
		state = model.JobKilled
	case res.Err != nil || res.ExitCode != 0:
		state = model.JobFailed
	}
	body := map[string]any{"state": state, "exit_code": res.ExitCode, "worker_id": w.workerID}
	if res.Reason != "" {
		body["reason"] = res.Reason
	}
	if res.Err != nil {
		body["reason"] = res.Err.Error()
	}
	// The terminal report is the call that frees the device's lease: unlike
	// the "running" report above, dropping it strands the device until the
	// reaper notices, so it gets retried.
	w.reportTerminalWithRetry(reportCtx, a.JobID, body)

	// The job ended — success, failure, watchdog, kill, or shutdown, it
	// makes no difference here. Arm the release linger; if another
	// assignment for this device arrives before it fires, acquireDevice
	// cancels it and this job's release never runs at all, which is exactly
	// the point: the device was never actually idle in between.
	//
	// acq.epoch is this job's proof that it still owns the device. The
	// terminal report above can take up to ~28s when its response is lost,
	// and the controller frees the device the moment the report lands — so
	// by the time this line runs, a later job may already hold it. Passing
	// the epoch is what makes that case a no-op instead of a release firing
	// on top of a running job.
	w.scheduleRelease(a.DeviceID, spec, a.JobID, a.Submitter, acq.epoch)
}

// terminalReportAttempts bounds how many times reportTerminalWithRetry will
// try before giving up and logging. Retrying forever would leave a goroutine
// spinning after a permanent failure (e.g. a revoked token); this trades a
// bounded, logged failure for that risk.
const terminalReportAttempts = 5

// terminalReportAttemptTimeout bounds a SINGLE terminal-report HTTP call.
// w's http.Client has a 2-minute Timeout (see New) sized for ordinary log
// streaming, but a blackholed controller — TCP accepted, nothing ever comes
// back, unlike a refused connection which fails in milliseconds — would let
// one stuck attempt consume up to that full 2 minutes on its own. With 5
// attempts that is up to 10 minutes, which does not fit inside Start's 45s
// shutdownGrace no matter how the backoff is tuned. Capping each attempt
// here, not the retry loop as a whole, is what actually makes shutdownGrace's
// comment about covering "the terminal report's retry budget" true: worst
// case is terminalReportAttempts*terminalReportAttemptTimeout plus the
// backoff between them (5*5s + ~3s ≈ 28s), comfortably inside 45s alongside
// Run's own worst-case kill sequence (~12s).
//
// A var, not a const, solely so an internal test can shrink it and prove the
// bound is real without actually waiting on a blackholed connection.
var terminalReportAttemptTimeout = 5 * time.Second

func (w *Worker) reportTerminalWithRetry(ctx context.Context, jobID string, body map[string]any) {
	backoff := 200 * time.Millisecond
	for attempt := 1; attempt <= terminalReportAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, terminalReportAttemptTimeout)
		ok := w.report(attemptCtx, jobID, body)
		cancel()
		if ok {
			return
		}
		if attempt == terminalReportAttempts {
			break
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	slog.Error("terminal status report failed after retries; device lease will strand until the reaper reclaims it",
		"job", jobID)
}

// report sends a single status update and reports whether the controller
// accepted it. Both a transport error and a non-2xx response are logged by
// name here — a silently swallowed rejection (e.g. 403 not_job_owner, or a
// 400 from a typoed state) previously left no trace of a dropped report.
func (w *Worker) report(ctx context.Context, jobID string, body map[string]any) bool {
	payload, err := json.Marshal(body)
	if err != nil {
		slog.Error("marshal status", "job", jobID, "err", err)
		return false
	}
	resp, err := w.do(ctx, http.MethodPost, "/v1/jobs/"+jobID+"/status", bytes.NewReader(payload))
	if err != nil {
		slog.Warn("report status", "job", jobID, "err", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		slog.Warn("report status rejected", "job", jobID, "status", resp.Status)
		return false
	}
	return true
}

// logSink batches output and ships it to the controller, flushing on size or
// time so a live `rc run` sees output while the job is still going.
type logSink struct {
	w     *Worker
	jobID string
	ctx   context.Context

	mu  sync.Mutex // guards buf and the flush timer
	buf bytes.Buffer
	t   *time.Timer

	// sendMu serialises Flush end-to-end (extraction through the POST), not
	// just the POST itself. Two Flush calls can be triggered concurrently
	// (the size threshold inside Write, and the timer's AfterFunc); without
	// this, whichever one happens to finish its POST first can land its
	// chunk at the controller ahead of an earlier chunk that was extracted
	// first but got descheduled before sending. Serialising the whole
	// operation makes extraction order and send order the same thing.
	sendMu sync.Mutex
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.buf.Write(p)
	full := s.buf.Len() >= 64*1024
	if s.t == nil {
		s.t = time.AfterFunc(time.Second, s.Flush)
	}
	s.mu.Unlock()

	if full {
		s.Flush()
	}
	return len(p), nil
}

func (s *logSink) Flush() {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	s.mu.Lock()
	if s.t != nil {
		s.t.Stop()
		s.t = nil
	}
	if s.buf.Len() == 0 {
		s.mu.Unlock()
		return
	}
	chunk := make([]byte, s.buf.Len())
	copy(chunk, s.buf.Bytes())
	s.buf.Reset()
	s.mu.Unlock() // released before the network call: Write must keep accepting output while a send is in flight

	resp, err := s.w.do(s.ctx, http.MethodPost, "/v1/jobs/"+s.jobID+"/logs", bytes.NewReader(chunk))
	if err != nil {
		slog.Warn("ship logs", "job", s.jobID, "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		// The chunk is already gone from buf and cannot be recovered here;
		// this at least makes the loss visible instead of silent.
		slog.Warn("ship logs rejected; chunk lost", "job", s.jobID, "status", resp.Status)
	}
}
