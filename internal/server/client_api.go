package server

import (
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
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job)
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
	if err := s.cfg.Store.ClearDevice(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
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

	for chunk := range chunks {
		if _, err := w.Write(chunk); err != nil {
			return
		}
		flusher.Flush()
	}
}
