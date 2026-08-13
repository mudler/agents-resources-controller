package server

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
)

type SubmitRequest struct {
	DeviceID        string            `json:"device_id"`
	Command         []string          `json:"command"`
	Cwd             string            `json:"cwd,omitempty"`
	Env             map[string]string `json:"env,omitempty"`
	Submitter       string            `json:"submitter"`
	IdempotencyKey  string            `json:"idempotency_key,omitempty"`
	LeaseTTLSeconds int               `json:"lease_ttl_seconds,omitempty"`
}

type DeviceView struct {
	Device              model.Device `json:"device"`
	Holder              string       `json:"holder,omitempty"`
	JobID               string       `json:"job_id,omitempty"`
	Command             []string     `json:"command,omitempty"`
	ElapsedSeconds      int          `json:"elapsed_seconds"`
	HeartbeatAgeSeconds int          `json:"heartbeat_age_seconds"`
}

type StateResponse struct {
	Devices []DeviceView `json:"devices"`
	Jobs    []model.Job  `json:"jobs"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req SubmitRequest
	if !decode(w, r, &req) {
		return
	}
	if len(req.Command) == 0 {
		writeErr(w, http.StatusBadRequest, "bad_request", "command required")
		return
	}
	if req.DeviceID == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "device_id required (selectors arrive in stage 2)")
		return
	}
	if req.Submitter == "" {
		// A blank holder on a busy device defeats the entire point of this
		// system: answering "who has gpu0 right now".
		writeErr(w, http.StatusBadRequest, "bad_request", "submitter required")
		return
	}

	ttl := time.Duration(req.LeaseTTLSeconds) * time.Second
	job, err := s.cfg.Store.Allocate(store.AllocateRequest{
		DeviceID: req.DeviceID, Command: req.Command, Cwd: req.Cwd, Env: req.Env,
		Submitter: req.Submitter, IdempotencyKey: req.IdempotencyKey, LeaseTTL: ttl,
	})
	if errors.Is(err, store.ErrNoDevice) {
		// No queue in stage 1: say so immediately rather than block.
		writeErr(w, http.StatusConflict, "no_device_available",
			fmt.Sprintf("%s is not free", req.DeviceID))
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}

	s.notify.poke(job.WorkerID)
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.cfg.Store.Job(r.PathValue("id"))
	if err != nil {
		writeJobLookupError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// writeJobLookupError maps a Store.Job error to the right client-facing
// status. A missing job is 404 with a stable message; anything else (a
// closed database, a driver error) must NOT also become 404 — a live job
// reported as "not found" during an infrastructure failure would make a
// client conclude the job vanished and resubmit onto a device that is
// actually still busy. Anything other than sql.ErrNoRows is a 500 with a
// generic message; the raw driver error never reaches the client.
func writeJobLookupError(w http.ResponseWriter, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		writeErr(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	writeErr(w, http.StatusInternalServerError, "store_error", "failed to look up job")
}

func (s *Server) handleDevices(w http.ResponseWriter, r *http.Request) {
	views, err := s.deviceViews()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	views, err := s.deviceViews()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	jobs, err := s.cfg.Store.ActiveJobs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, StateResponse{Devices: views, Jobs: jobs})
}

// deviceViews joins devices to their live lease so a caller sees who holds
// what and how stale the information is — the thing a lock file cannot say.
func (s *Server) deviceViews() ([]DeviceView, error) {
	devices, err := s.cfg.Store.Devices()
	if err != nil {
		return nil, err
	}
	leases, err := s.cfg.Store.Leases()
	if err != nil {
		return nil, err
	}
	byDevice := map[string]struct {
		holder string
		jobID  string
		since  time.Time
	}{}
	for _, l := range leases {
		byDevice[l.DeviceID] = struct {
			holder string
			jobID  string
			since  time.Time
		}{l.Holder, l.JobID, l.AcquiredAt}
	}

	now := s.cfg.Clock.Now()
	out := make([]DeviceView, 0, len(devices))
	for _, d := range devices {
		v := DeviceView{
			Device:              d,
			HeartbeatAgeSeconds: int(now.Sub(d.LastHeartbeatAt).Seconds()),
		}
		if l, ok := byDevice[d.ID]; ok {
			v.Holder = l.holder
			v.JobID = l.jobID
			v.ElapsedSeconds = int(now.Sub(l.since).Seconds())
			if job, err := s.cfg.Store.Job(l.jobID); err == nil {
				v.Command = job.Command
			}
		}
		out = append(out, v)
	}
	return out, nil
}

func (s *Server) handleClearDevice(w http.ResponseWriter, r *http.Request) {
	cleared, err := s.cfg.Store.ClearDevice(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", "failed to clear device")
		return
	}
	if !cleared {
		// ClearDevice refuses to contradict the lease table. Answering 200
		// here would tell the operator the opposite of what happened on the
		// one manual override that exists for a stuck GPU.
		writeErr(w, http.StatusConflict, "device_not_cleared", "device has a live lease and was not cleared")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleStreamLogs streams a job's output as newline-delimited chunks and
// ends when the job reaches a terminal state.
func (s *Server) handleStreamLogs(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "unsupported", "streaming unsupported")
		return
	}

	// The job must exist before anything is started: Follow would otherwise
	// O_CREATE a log file for an unknown ID and the watcher below would spin
	// forever since Job() never succeeds, leaking a goroutine and an fd per
	// request to any client token.
	if _, err := s.cfg.Store.Job(jobID); err != nil {
		writeJobLookupError(w, err)
		return
	}

	done := make(chan struct{})
	chunks, err := s.cfg.Logs.Follow(r.Context(), jobID, done)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	// Close the follower once the job finishes.
	go func() {
		defer close(done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				job, err := s.cfg.Store.Job(jobID)
				if err == nil && job.State.Terminal() {
					time.Sleep(200 * time.Millisecond) // let trailing chunks land
					return
				}
			}
		}
	}()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	// Flush the status line and headers immediately: WriteHeader only
	// buffers them. Without this a client blocks with no response at all
	// until the first log chunk arrives, which may be never for a job that
	// has not produced output yet.
	flusher.Flush()

	for chunk := range chunks {
		if _, err := w.Write(chunk); err != nil {
			return
		}
		flusher.Flush()
	}
}
