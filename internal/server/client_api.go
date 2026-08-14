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
// MinPriority and MaxPriority bound SubmitRequest.Priority. The spec is
// explicit that priority is "bounded to a small range so it stays a nudge
// rather than a scheduling language": unbounded values invite callers to
// encode a policy in the number (1000 for "production", 9999 for "really
// production"), and since there is no preemption, a big number buys nothing
// a small one does not — it only makes every later submitter bid higher.
// Out-of-range submissions are rejected rather than clamped, so a caller who
// asked for 500 is never quietly given 10 and left believing otherwise.
const (
	MinPriority = -10
	MaxPriority = 10
)

type SubmitRequest struct {
	DeviceID string `json:"device_id,omitempty"`
	// Selector picks a device by its labels instead of by exact ID — give
	// exactly one of DeviceID or Selector, never both. See
	// store.MatchingDevices for the matching rules.
	Selector           string            `json:"selector,omitempty"`
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

// describeRecentJobs bounds how much job history `rc describe` shows: five
// is enough to tell "this box just failed the last three runs" from "this
// box is fine", without turning describe into a second `rc ps`.
const describeRecentJobs = 5

// DescribeResponse is everything an agent needs to trust (or distrust) a
// device before writing commands for it: what it is, who holds it now, every
// label with its provenance and age, the humans' own usage notes and how
// stale THEY are, and its recent job history.
type DescribeResponse struct {
	Device              model.Device  `json:"device"`
	Holder              string        `json:"holder,omitempty"`
	JobID               string        `json:"job_id,omitempty"`
	ElapsedSeconds      int           `json:"elapsed_seconds"`
	HeartbeatAgeSeconds int           `json:"heartbeat_age_seconds"`
	Labels              []model.Label `json:"labels,omitempty"`
	Sheet               string        `json:"sheet,omitempty"`
	SheetUpdatedAt      time.Time     `json:"sheet_updated_at,omitempty"`
	RecentJobs          []model.Job   `json:"recent_jobs,omitempty"`
}

// ExplainResponse answers "if I submitted this selector right now, what
// would happen" without actually submitting anything: which devices match,
// which of those are free this instant, and how backed up the ones that
// aren't free already are.
type ExplainResponse struct {
	Selector   string   `json:"selector"`
	Matching   []string `json:"matching"`
	Free       []string `json:"free"`
	QueueDepth int      `json:"queue_depth"`
}

// handleDescribe answers `rc describe`: everything the routes above answer
// piecemeal (device state, labels, sheet, history), joined for one device so
// an agent can learn what a box is before it writes commands for it.
func (s *Server) handleDescribe(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	views, err := s.deviceViews()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	var view *DeviceView
	for i := range views {
		if views[i].Device.ID == id {
			view = &views[i]
			break
		}
	}
	if view == nil {
		writeErr(w, http.StatusNotFound, "not_found", "device not found")
		return
	}

	labels, err := s.cfg.Store.LabelsFor(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}

	// Prefer the device's own sheet; a device with none of its own still
	// gets the host-wide one, so describe never goes silent about
	// documentation that exists just because it lives one level up.
	sheet, sheetAt, err := s.cfg.Store.HostDoc(view.Device.Host, id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	if sheet == "" && sheetAt.IsZero() {
		sheet, sheetAt, err = s.cfg.Store.HostDoc(view.Device.Host, "")
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
	}

	recent, err := s.cfg.Store.RecentJobsForDevice(id, describeRecentJobs)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, DescribeResponse{
		Device:              view.Device,
		Holder:              view.Holder,
		JobID:               view.JobID,
		ElapsedSeconds:      view.ElapsedSeconds,
		HeartbeatAgeSeconds: view.HeartbeatAgeSeconds,
		Labels:              labels,
		Sheet:               sheet,
		SheetUpdatedAt:      sheetAt,
		RecentJobs:          recent,
	})
}

// handleExplain answers `rc run --explain`: it runs exactly the matching
// logic a real submit would (store.MatchingDevices — the same function
// Enqueue and ScheduleOnce use, so explain can never disagree with what
// actually happens), then reports device state and queue backlog, and
// submits nothing.
func (s *Server) handleExplain(w http.ResponseWriter, r *http.Request) {
	sel := r.URL.Query().Get("selector")
	if sel == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "selector query parameter required")
		return
	}

	matching, err := s.cfg.Store.MatchingDevices(sel)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_selector", err.Error())
		return
	}

	devices, err := s.cfg.Store.Devices()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	stateByID := make(map[string]model.DeviceState, len(devices))
	for _, d := range devices {
		stateByID[d.ID] = d.State
	}

	matchSet := make(map[string]bool, len(matching))
	free := make([]string, 0, len(matching))
	for _, id := range matching {
		matchSet[id] = true
		if stateByID[id] == model.DeviceReady {
			free = append(free, id)
		}
	}

	queued, err := s.cfg.Store.QueuedJobs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	depth := 0
	for _, j := range queued {
		if j.DeviceID != "" {
			if matchSet[j.DeviceID] {
				depth++
			}
			continue
		}
		if j.Selector == "" {
			continue
		}
		// A queued selector job could land on any of ITS candidates; it
		// counts against this explain's depth if the two candidate sets
		// overlap at all — reusing MatchingDevices again rather than a
		// second notion of "could this land here".
		candidates, err := s.cfg.Store.MatchingDevices(j.Selector)
		if err != nil {
			continue
		}
		for _, c := range candidates {
			if matchSet[c] {
				depth++
				break
			}
		}
	}

	writeJSON(w, http.StatusOK, ExplainResponse{
		Selector: sel, Matching: matching, Free: free, QueueDepth: depth,
	})
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
	switch {
	case req.DeviceID != "" && req.Selector != "":
		writeErr(w, http.StatusBadRequest, "bad_request", "give either device_id or selector, not both")
		return
	case req.DeviceID == "" && req.Selector == "":
		writeErr(w, http.StatusBadRequest, "bad_request", "device_id or selector required")
		return
	}
	if req.Submitter == "" {
		// A blank holder on a busy device defeats the entire point of this
		// system: answering "who has gpu0 right now".
		writeErr(w, http.StatusBadRequest, "bad_request", "submitter required")
		return
	}
	if req.Priority < MinPriority || req.Priority > MaxPriority {
		writeErr(w, http.StatusBadRequest, "priority_out_of_range",
			fmt.Sprintf("priority %d is out of range: it must be between %d and %d — priority is a nudge within one device's queue, not a scheduling language",
				req.Priority, MinPriority, MaxPriority))
		return
	}

	// Submitting always enqueues: ScheduleOnce below is the only place a
	// device changes hands, so there is no check-then-act race between
	// "is this device free?" and grabbing it.
	job, err := s.cfg.Store.Enqueue(store.EnqueueRequest{
		DeviceID: req.DeviceID, Selector: req.Selector, Command: req.Command, Cwd: req.Cwd, Env: req.Env,
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
	if errors.Is(err, store.ErrNoMatchingDevice) {
		writeErr(w, http.StatusBadRequest, "no_matching_device", err.Error())
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

// logWriteTimeout bounds every write to a log stream, mirroring
// eventWriteTimeout on the SSE side. A log reader that cannot accept a chunk
// within this is wedged, not merely slow: the alternative to disconnecting it
// is holding its subscriber, goroutine and fd open forever.
//
// A var, not a const, solely so an internal test can shrink it and prove the
// bound is real without a ten-second test — the same trick, for the same
// reason, as internal/worker's terminalReportAttemptTimeout.
var logWriteTimeout = 10 * time.Second

// handleStreamLogs streams a job's output as newline-delimited chunks and
// ends when the job reaches a terminal state.
func (s *Server) handleStreamLogs(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")

	if _, ok := w.(http.Flusher); !ok {
		writeErr(w, http.StatusInternalServerError, "unsupported", "streaming unsupported")
		return
	}
	rc := http.NewResponseController(w)

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
	if !flushLogs(rc) {
		return
	}

	for chunk := range chunks {
		// Every write is bounded, for the same reason the SSE stream's are
		// (see writeSSE): a wedged peer — a suspended laptop, a blackholed
		// route — blocks w.Write forever, and r.Context().Done() cannot
		// preempt a write already in flight. The handler would sit in that
		// write for the life of the process, so the deferred unsubscribe in
		// logstore never runs and the subscriber, its goroutine and its fd
		// leak. `rc attach` makes long-lived log streams routine, so this is
		// an ordinary case rather than an exotic one.
		if err := rc.SetWriteDeadline(time.Now().Add(logWriteTimeout)); err != nil {
			return
		}
		if _, err := w.Write(chunk); err != nil {
			return
		}
		if !flushLogs(rc) {
			return
		}
	}
}

// flushLogs flushes under the same bounded deadline every log write gets, so
// the header flush cannot wedge either.
func flushLogs(rc *http.ResponseController) bool {
	if err := rc.SetWriteDeadline(time.Now().Add(logWriteTimeout)); err != nil {
		return false
	}
	return rc.Flush() == nil
}
