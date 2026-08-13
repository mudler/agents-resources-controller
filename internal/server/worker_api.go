package server

import (
	"database/sql"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/mudler/agents-resources-controller/internal/model"
)

// maxLogChunk bounds a single log upload. A worker whose chunk exceeds this
// is told so explicitly (413) and must split and retry: silently truncating
// a job's output would lose data without telling anyone.
const maxLogChunk = 1 << 20

type RegisterRequest struct {
	Host    string   `json:"host"`
	Devices []string `json:"devices"`
}

type RegisterResponse struct {
	WorkerID string `json:"worker_id"`
}

type Assignment struct {
	JobID    string            `json:"job_id"`
	DeviceID string            `json:"device_id"`
	Command  []string          `json:"command"`
	Cwd      string            `json:"cwd,omitempty"`
	Env      map[string]string `json:"env,omitempty"`
}

type StatusRequest struct {
	WorkerID string         `json:"worker_id"`
	State    model.JobState `json:"state"`
	ExitCode *int           `json:"exit_code,omitempty"`
	Reason   string         `json:"reason,omitempty"`
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Host == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "host required")
		return
	}

	now := s.cfg.Clock.Now()
	// The worker ID is derived from the host so a restarted worker resumes its
	// own devices instead of orphaning them.
	workerID := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(req.Host)).String()

	devices := make([]model.Device, 0, len(req.Devices))
	for _, name := range req.Devices {
		devices = append(devices, model.Device{
			ID: req.Host + ":" + name, Host: req.Host, Name: name,
			WorkerID: workerID, State: model.DeviceReady, LastHeartbeatAt: now,
		})
	}

	if err := s.cfg.Store.UpsertWorker(
		model.Worker{ID: workerID, Host: req.Host, LastHeartbeatAt: now}, devices); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, RegisterResponse{WorkerID: workerID})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if err := s.cfg.Store.RecordHeartbeat(r.PathValue("id"), s.cfg.Clock.Now()); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleAssignments long-polls: it returns as soon as this worker has work,
// otherwise 204 after the wait window so the worker simply calls again.
func (s *Server) handleAssignments(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")

	wait := 30 * time.Second
	if v := r.URL.Query().Get("wait"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 && d <= time.Minute {
			wait = d
		}
	}

	woken, cancel := s.notify.wait(workerID)
	defer cancel()

	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	for {
		jobs, err := s.cfg.Store.AssignedJobsFor(workerID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
		if len(jobs) > 0 {
			// Handing out an assignment is a state transition, not just a
			// query: mark each job running immediately, before it is ever
			// written to the response. Otherwise the job stays "assigned"
			// until the worker's own "running" report happens to land, and a
			// second poll landing in that window (or a retried poll after a
			// dropped response) sees it as "assigned" again and hands it out
			// — and therefore starts it — a second time.
			//
			// This makes started_at mark handout rather than the moment the
			// worker actually spawns the process, which is a deliberate,
			// documented shift: if the worker dies between handout and spawn,
			// the job simply sits in "running" until the reaper marks it
			// lost, which is the correct outcome since nobody actually knows
			// whether it started. The worker's own "running" report becomes a
			// harmless no-op — MarkRunning only transitions assigned->running,
			// so calling it again once already running affects no rows.
			now := s.cfg.Clock.Now()
			out := make([]Assignment, 0, len(jobs))
			for _, j := range jobs {
				if err := s.cfg.Store.MarkRunning(j.ID, now); err != nil {
					writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
					return
				}
				out = append(out, Assignment{
					JobID: j.ID, DeviceID: j.DeviceID, Command: j.Command, Cwd: j.Cwd, Env: j.Env,
				})
			}
			writeJSON(w, http.StatusOK, out)
			return
		}
		select {
		case <-woken:
		case <-deadline.C:
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) handleAppendLogs(w http.ResponseWriter, r *http.Request) {
	chunk, err := io.ReadAll(io.LimitReader(r.Body, maxLogChunk+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if len(chunk) > maxLogChunk {
		writeErr(w, http.StatusRequestEntityTooLarge, "chunk_too_large", "log chunk exceeds 1MB; split and retry")
		return
	}
	if err := s.cfg.Logs.Append(r.PathValue("id"), chunk); err != nil {
		writeErr(w, http.StatusInternalServerError, "log_error", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	var req StatusRequest
	if !decode(w, r, &req) {
		return
	}
	jobID := r.PathValue("id")

	job, err := s.cfg.Store.Job(jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "not_found", "job not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	// A job belongs to the worker it was assigned to. Without this check any
	// holder of a worker token could report status for someone else's job and
	// have Release() hand its device to a second job while the first is still
	// running on it — exactly the race this system exists to prevent.
	if req.WorkerID == "" || req.WorkerID != job.WorkerID {
		writeErr(w, http.StatusForbidden, "not_job_owner", "job is not assigned to this worker")
		return
	}

	if req.State == model.JobRunning {
		if err := s.cfg.Store.MarkRunning(jobID, s.cfg.Clock.Now()); err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if !req.State.Terminal() {
		writeErr(w, http.StatusBadRequest, "bad_request", "state must be running or terminal")
		return
	}
	if err := s.cfg.Store.Release(jobID, req.State, req.ExitCode, req.Reason); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}
