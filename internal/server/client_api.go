package server

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/store"
)

// SubmitRequest deliberately has no lease-TTL field: stage 1 enforces no
// expiry (watchdogs arrive in a later stage — see the design doc's staging
// section), and the store's leases.expires_at column is written with an
// internal default that nothing reads yet. Advertising a lease_ttl_seconds
// knob on the wire that silently did nothing was worse than not having the
// knob; the internal column and default stay, ready for the stage that
// actually enforces them.
type SubmitRequest struct {
	DeviceID           string            `json:"device_id"`
	Command            []string          `json:"command"`
	Cwd                string            `json:"cwd,omitempty"`
	Env                map[string]string `json:"env,omitempty"`
	Submitter          string            `json:"submitter"`
	IdempotencyKey     string            `json:"idempotency_key,omitempty"`
	Priority           int               `json:"priority,omitempty"`
	MaxRuntimeSeconds  int               `json:"max_runtime_seconds,omitempty"`
	IdleTimeoutSeconds int               `json:"idle_timeout_seconds,omitempty"`
	NoWait             bool              `json:"no_wait,omitempty"`
}

// JobView is a job plus the queue position a client needs to show progress.
type JobView struct {
	Job           model.Job `json:"job"`
	QueuePosition int       `json:"queue_position,omitempty"`
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
	Queued  []model.Job  `json:"queued"`
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
		writeErr(w, http.StatusBadRequest, "bad_request", "device_id required (device selectors are not implemented yet; address a device by its exact ID)")
		return
	}
	if req.Submitter == "" {
		// A blank holder on a busy device defeats the entire point of this
		// system: answering "who has gpu0 right now".
		writeErr(w, http.StatusBadRequest, "bad_request", "submitter required")
		return
	}

	// Submitting always enqueues: ScheduleOnce below is the only place a
	// device changes hands, so there is no check-then-act race between
	// "is this device free?" and grabbing it.
	job, err := s.cfg.Store.Enqueue(store.EnqueueRequest{
		DeviceID: req.DeviceID, Command: req.Command, Cwd: req.Cwd, Env: req.Env,
		Submitter: req.Submitter, IdempotencyKey: req.IdempotencyKey,
		Priority:    req.Priority,
		MaxRuntime:  time.Duration(req.MaxRuntimeSeconds) * time.Second,
		IdleTimeout: time.Duration(req.IdleTimeoutSeconds) * time.Second,
	})
	if errors.Is(err, store.ErrRuntimeAboveCeiling) {
		writeErr(w, http.StatusBadRequest, "runtime_above_ceiling", err.Error())
		return
	}
	if errors.Is(err, store.ErrUnknownDevice) {
		writeErr(w, http.StatusBadRequest, "unknown_device", err.Error())
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", "could not queue job")
		return
	}

	// Make one scheduling pass so a free device starts immediately rather
	// than waiting for the loop's next tick.
	assigned, err := s.cfg.Store.ScheduleOnce()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error",
			s.abandonQueuedJob(job.ID, "submit failed: could not schedule", "could not schedule"))
		return
	}
	for _, a := range assigned {
		s.notify.poke(a.WorkerID)
	}

	current, err := s.cfg.Store.Job(job.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error",
			s.abandonQueuedJob(job.ID, "submit failed: could not read job", "could not read job"))
		return
	}

	// --no-wait keeps stage 1's behaviour: never sit in a queue.
	if req.NoWait && current.State == model.JobQueued {
		cancelled, err := s.cfg.Store.CancelQueued(current.ID, "no-wait: device busy")
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", "could not cancel")
			return
		}
		if !cancelled {
			// Lost the race: something — this request's own ScheduleOnce
			// pass above, a concurrent submit, or rc serve's ticking
			// scheduler loop — assigned the device to this exact job
			// between the Job() read above and this cancel attempt.
			// NoWait asked not to sit in a queue; it did not ask to have a
			// job that is now actually running hidden behind a 409 while
			// it holds a GPU with no client watching it. Report it exactly
			// as an ordinary submit would.
			reloaded, err := s.cfg.Store.Job(current.ID)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, "store_error", "could not read job")
				return
			}
			s.publishJob(reloaded.ID, reloaded.State)
			writeJSON(w, http.StatusCreated, reloaded)
			return
		}
		s.publishJob(current.ID, model.JobKilled)
		writeErr(w, http.StatusConflict, "no_device_available",
			fmt.Sprintf("%s is not free", req.DeviceID))
		return
	}

	s.publishJob(current.ID, current.State)
	writeJSON(w, http.StatusCreated, current)
}

// abandonQueuedJob is called from the submit error paths that run after
// Enqueue has already created a queued job: it makes a best-effort attempt
// to cancel that job before the caller sees an error. Without this, a
// transient failure from a later step (ScheduleOnce, the read-back) — which
// the client sees as a failed submit it may reasonably retry elsewhere —
// would leave the job sitting in the queue to be picked up and run later by
// rc serve's scheduler loop with no client ever attached to it. It returns
// the message to report: the original failure alone, or, if the cancel
// itself also failed, both failures rather than letting the cancel error
// swallow the original one.
func (s *Server) abandonQueuedJob(jobID, cancelReason, failureMsg string) string {
	if _, err := s.cfg.Store.CancelQueued(jobID, cancelReason); err != nil {
		return fmt.Sprintf("%s, and could not cancel the queued job: %v", failureMsg, err)
	}
	return failureMsg
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.cfg.Store.Job(r.PathValue("id"))
	if err != nil {
		writeJobLookupError(w, err)
		return
	}
	pos, err := s.cfg.Store.QueuePosition(job.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", "could not read queue position")
		return
	}
	writeJSON(w, http.StatusOK, JobView{Job: *job, QueuePosition: pos})
}

type KillRequest struct {
	Submitter string `json:"submitter"`
}

// handleKill cancels a queued job outright, or asks the worker to terminate a
// running one by flagging it and letting the worker's poll observe the kill
// flag (task 5). Ownership is checked against the submitter unless the caller
// holds an admin token.
func (s *Server) handleKill(w http.ResponseWriter, r *http.Request) {
	var req KillRequest
	if !decode(w, r, &req) {
		return
	}
	jobID := r.PathValue("id")

	job, err := s.cfg.Store.Job(jobID)
	if err != nil {
		writeJobLookupError(w, err)
		return
	}
	if !isAdmin(r) && job.Submitter != req.Submitter {
		writeErr(w, http.StatusForbidden, "not_job_owner",
			"only the submitter or an admin may kill this job")
		return
	}

	// An admin token may kill a job it does not own — that is what "admin"
	// means here — but the recorded reason must say so plainly rather than
	// crediting an empty or unrelated req.Submitter value, which would read
	// back as a mystery to whoever investigates the job later.
	reason := "killed by " + req.Submitter
	if isAdmin(r) {
		reason = "killed by admin override"
		if req.Submitter != "" {
			reason += " (as " + req.Submitter + ")"
		}
	}

	switch job.State {
	case model.JobQueued:
		ok, err := s.cfg.Store.CancelQueued(jobID, reason)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", "could not cancel")
			return
		}
		if !ok {
			writeErr(w, http.StatusConflict, "not_cancellable", "job already started")
			return
		}
		s.publishJob(jobID, model.JobKilled)
	case model.JobAssigned, model.JobRunning:
		flagged, err := s.cfg.Store.RequestKill(jobID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", "could not request kill")
			return
		}
		if !flagged {
			writeErr(w, http.StatusConflict, "not_cancellable", "job already finished")
			return
		}
		s.notify.poke(job.WorkerID)
		s.publishJob(jobID, job.State)
	default:
		writeErr(w, http.StatusConflict, "not_cancellable", "job already finished")
		return
	}
	w.WriteHeader(http.StatusOK)
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
	queued, err := s.cfg.Store.QueuedJobs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, StateResponse{Devices: views, Jobs: jobs, Queued: queued})
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
	s.publishDevices()
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
