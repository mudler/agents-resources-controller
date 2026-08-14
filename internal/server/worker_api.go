package server

import (
	"bytes"
	"database/sql"
	"encoding/json"
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

// maxHeartbeatBody bounds a heartbeat's job list. A worker supervises a
// handful of jobs at most (one per device), so anything approaching this is
// either a bug or an attempt to make the controller allocate on demand.
const maxHeartbeatBody = 64 << 10

// DeviceSpec is one device a worker declares at registration: its name and,
// if the operator configured one, the runtime ceiling the controller should
// enforce against any job scheduled onto it.
type DeviceSpec struct {
	Name              string `json:"name"`
	MaxRuntimeSeconds int    `json:"max_runtime_seconds,omitempty"`
}

type RegisterRequest struct {
	Host    string       `json:"host"`
	BootID  string       `json:"boot_id,omitempty"`
	Devices []DeviceSpec `json:"devices"`
}

type RegisterResponse struct {
	WorkerID string `json:"worker_id"`
}

type Assignment struct {
	JobID              string            `json:"job_id"`
	DeviceID           string            `json:"device_id"`
	Command            []string          `json:"command"`
	Cwd                string            `json:"cwd,omitempty"`
	Env                map[string]string `json:"env,omitempty"`
	MaxRuntimeSeconds  int               `json:"max_runtime_seconds,omitempty"`
	IdleTimeoutSeconds int               `json:"idle_timeout_seconds,omitempty"`
}

// PollResponse is the envelope handleAssignments answers a long-poll with: it
// carries both newly handed-out assignments and job IDs the controller wants
// killed, so a kill reaches the worker as fast as an assignment does rather
// than waiting on a separate channel.
type PollResponse struct {
	Assignments []Assignment `json:"assignments"`
	Kills       []string     `json:"kills,omitempty"`
}

// HeartbeatRequest is what a worker says on every heartbeat: the IDs of the
// jobs it is actually supervising right now. The controller renews the leases
// of exactly those jobs and no others, so a job the worker has no process for
// — one whose assignment response never arrived, say — stops being renewed
// and falls to lease expiry rather than being kept alive forever by an
// unrelated liveness signal.
type HeartbeatRequest struct {
	RunningJobIDs []string `json:"running_job_ids,omitempty"`
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
	for _, dev := range req.Devices {
		devices = append(devices, model.Device{
			ID: req.Host + ":" + dev.Name, Host: req.Host, Name: dev.Name,
			WorkerID: workerID, State: model.DeviceReady, LastHeartbeatAt: now,
		})
	}

	if err := s.cfg.Store.UpsertWorker(
		model.Worker{ID: workerID, Host: req.Host, BootID: req.BootID, LastHeartbeatAt: now}, devices); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}

	// UpsertWorker creates/updates the device rows themselves but does not
	// touch their runtime ceiling; that is set here, once each device row is
	// known to exist. worker.yaml is the declaration of intent, so
	// registration must make the stored ceiling match it exactly — including
	// when the declaration is "no ceiling": a device that previously
	// declared max_runtime and now omits it must actually be cleared, or an
	// operator who deletes it and restarts sees no change and can only fix
	// it by hand-editing the database. Every device in this registration
	// gets its ceiling written, zero included, not just the ones with a
	// positive value.
	for _, dev := range req.Devices {
		sec := dev.MaxRuntimeSeconds
		if sec < 0 {
			sec = 0
		}
		id := req.Host + ":" + dev.Name
		if err := s.cfg.Store.SetDeviceMaxRuntime(id, time.Duration(sec)*time.Second); err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
	}
	s.publishDevices()
	writeJSON(w, http.StatusOK, RegisterResponse{WorkerID: workerID})
}

// handleHeartbeat renews the leases of the jobs the worker names, and only
// those. See store.RecordHeartbeat for why: a worker being alive says nothing
// about whether it is running any particular job, and renewing on liveness
// alone made lease expiry unreachable for a job whose assignment response was
// lost.
//
// A body is optional so a heartbeat from a worker with nothing running can
// stay a bare POST. An absent or empty body means "I am running nothing",
// which is the honest reading: a worker that cannot name a job is not
// evidence that the job is alive.
func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxHeartbeatBody+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if len(body) > maxHeartbeatBody {
		writeErr(w, http.StatusRequestEntityTooLarge, "body_too_large", "heartbeat body exceeds 64KB")
		return
	}
	var req HeartbeatRequest
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
	}

	if err := s.cfg.Store.RecordHeartbeat(
		r.PathValue("id"), s.cfg.Clock.Now(), req.RunningJobIDs); err != nil {
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
		// A kill must reach the worker as fast as an assignment does, so it
		// rides the same long-poll response rather than a separate channel:
		// checked on every wake, and on its own enough to end the wait.
		//
		// This is a take: each flag handed out here is stamped delivered and
		// stays quiet for killRedeliverInterval. Ending the poll early is
		// exactly what a kill is for, but a flag on a job no worker is
		// actually running would otherwise end EVERY poll instantly and the
		// worker would re-poll at its floor interval forever. Spacing
		// re-delivery keeps the fast path fast and bounds the pathological
		// one.
		kills, err := s.cfg.Store.TakeKillRequests(workerID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
		if len(jobs) > 0 || len(kills) > 0 {
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
					MaxRuntimeSeconds: j.MaxRuntimeSeconds, IdleTimeoutSeconds: j.IdleTimeoutSeconds,
				})
			}
			writeJSON(w, http.StatusOK, PollResponse{Assignments: out, Kills: kills})
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
		s.publishJob(jobID, req.State)
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
	s.publishJob(jobID, req.State)
	w.WriteHeader(http.StatusOK)
}
