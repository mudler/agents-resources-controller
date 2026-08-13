package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/mudler/resource-controller/internal/model"
)

type assignment struct {
	JobID    string            `json:"job_id"`
	DeviceID string            `json:"device_id"`
	Command  []string          `json:"command"`
	Cwd      string            `json:"cwd"`
	Env      map[string]string `json:"env"`
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
// process-group straggler) plus the terminal report's retry budget, or a
// routine shutdown would itself trigger the "abandoned job" path this exists
// to avoid.
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
	payload, err := json.Marshal(map[string]any{"host": w.cfg.Host, "devices": w.cfg.Devices})
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

// pollLoop long-polls for work. A failed poll is retried: the controller being
// down must never abandon jobs already running here.
func (w *Worker) pollLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		assignments, err := w.poll(ctx)
		if err != nil {
			slog.Warn("poll failed", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		for _, a := range assignments {
			// Registered here, synchronously in the poll loop, so that by the
			// time pollLoop returns (ctx already done) every goroutine it has
			// ever started is already accounted for in wg — Start's shutdown
			// wait can never race a wg.Add happening after it started
			// wg.Wait().
			w.wg.Add(1)
			go func(a assignment) {
				defer w.wg.Done()
				w.execute(ctx, a)
			}(a)
		}
	}
}

func (w *Worker) poll(ctx context.Context) ([]assignment, error) {
	path := fmt.Sprintf("/v1/workers/%s/assignments?wait=%s", w.workerID, w.cfg.PollWait)
	resp, err := w.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("controller returned %s", resp.Status)
	}
	var out []assignment
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
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

	sink := &logSink{w: w, jobID: a.JobID, ctx: reportCtx}
	res := Run(jobCtx, JobSpec{Command: a.Command, Cwd: a.Cwd, Env: env}, sink)
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

func (w *Worker) reportTerminalWithRetry(ctx context.Context, jobID string, body map[string]any) {
	backoff := 200 * time.Millisecond
	for attempt := 1; attempt <= terminalReportAttempts; attempt++ {
		if w.report(ctx, jobID, body) {
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
