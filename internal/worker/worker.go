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
}

// deviceSpec is what this worker declares about one of its devices at
// registration: its name and, if the operator configured one, the runtime
// ceiling the controller should enforce against any job scheduled onto it.
type deviceSpec struct {
	Name              string `json:"name"`
	MaxRuntimeSeconds int    `json:"max_runtime_seconds,omitempty"`
}

// pollResponse mirrors server.PollResponse: an envelope carrying both new
// work and kill requests, so a kill reaches the worker as fast as an
// assignment does rather than waiting on a separate channel.
type pollResponse struct {
	Assignments []assignment `json:"assignments"`
	Kills       []string     `json:"kills,omitempty"`
}

type Worker struct {
	cfg      Config
	http     *http.Client
	workerID string

	mu      sync.Mutex
	running map[string]context.CancelFunc // jobs currently in flight on this worker
	started map[string]struct{}           // every job ID this process has ever started; never deleted

	wg sync.WaitGroup // one entry per in-flight job goroutine; Start waits on this before returning
}

func New(cfg Config) *Worker {
	return &Worker{
		cfg:     cfg,
		http:    &http.Client{Timeout: 2 * time.Minute},
		running: map[string]context.CancelFunc{},
		started: map[string]struct{}{},
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
const shutdownGrace = 45 * time.Second

func (w *Worker) Start(ctx context.Context) error {
	if err := w.register(ctx); err != nil {
		return err
	}
	go w.heartbeatLoop(ctx)
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
	payload, err := json.Marshal(map[string]any{
		"host":    w.cfg.Host,
		"boot_id": BootID(),
		"devices": devices,
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

func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			resp, err := w.do(ctx, http.MethodPost, "/v1/workers/"+w.workerID+"/heartbeat", nil)
			if err != nil {
				slog.Warn("heartbeat failed", "err", err)
				continue
			}
			resp.Body.Close()
		}
	}
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
