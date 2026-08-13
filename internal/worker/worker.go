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
	running map[string]context.CancelFunc
}

func New(cfg Config) *Worker {
	return &Worker{
		cfg:     cfg,
		http:    &http.Client{Timeout: 2 * time.Minute},
		running: map[string]context.CancelFunc{},
	}
}

func (w *Worker) Start(ctx context.Context) error {
	if err := w.register(ctx); err != nil {
		return err
	}
	go w.heartbeatLoop(ctx)
	w.pollLoop(ctx)
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
			go w.execute(ctx, a)
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
		return // a duplicate poll result must not start the job twice
	}
	jobCtx, cancel := context.WithCancel(ctx)
	w.running[a.JobID] = cancel
	w.mu.Unlock()

	defer func() {
		cancel()
		w.mu.Lock()
		delete(w.running, a.JobID)
		w.mu.Unlock()
	}()

	// worker_id is required: the controller rejects a status report for a job
	// it did not assign to this worker, so one worker cannot free another's
	// device out from under a running process.
	w.report(ctx, a.JobID, map[string]any{"state": "running", "worker_id": w.workerID})

	env := map[string]string{}
	for k, v := range a.Env {
		env[k] = v
	}
	env["RC_JOB_ID"] = a.JobID
	env["RC_DEVICE"] = a.DeviceID

	sink := &logSink{w: w, jobID: a.JobID, ctx: ctx}
	res := Run(jobCtx, JobSpec{Command: a.Command, Cwd: a.Cwd, Env: env}, sink)
	sink.Flush()

	state := "succeeded"
	switch {
	case res.Killed:
		state = "killed"
	case res.Err != nil || res.ExitCode != 0:
		state = "failed"
	}
	body := map[string]any{"state": state, "exit_code": res.ExitCode, "worker_id": w.workerID}
	if res.Reason != "" {
		body["reason"] = res.Reason
	}
	if res.Err != nil {
		body["reason"] = res.Err.Error()
	}
	w.report(ctx, a.JobID, body)
}

func (w *Worker) report(ctx context.Context, jobID string, body map[string]any) {
	payload, err := json.Marshal(body)
	if err != nil {
		slog.Error("marshal status", "err", err)
		return
	}
	resp, err := w.do(ctx, http.MethodPost, "/v1/jobs/"+jobID+"/status", bytes.NewReader(payload))
	if err != nil {
		slog.Error("report status", "job", jobID, "err", err)
		return
	}
	resp.Body.Close()
}

// logSink batches output and ships it to the controller, flushing on size or
// time so a live `rc run` sees output while the job is still going.
type logSink struct {
	w     *Worker
	jobID string
	ctx   context.Context

	mu  sync.Mutex
	buf bytes.Buffer
	t   *time.Timer
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
	s.mu.Unlock()

	resp, err := s.w.do(s.ctx, http.MethodPost, "/v1/jobs/"+s.jobID+"/logs", bytes.NewReader(chunk))
	if err != nil {
		slog.Warn("ship logs", "job", s.jobID, "err", err)
		return
	}
	resp.Body.Close()
}
