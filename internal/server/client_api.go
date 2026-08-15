package server

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/selector"
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
	// Kind is model.LeaseKindJob or model.LeaseKindHold; empty means job.
	// A hold ("rc hold") is a job whose command the worker chooses for
	// itself, never the submitter — see handleSubmit, which rejects a hold
	// submission that carries one.
	Kind string `json:"kind,omitempty"`
	// Reason is why a hold was taken (e.g. "manual profiling"), surfaced by
	// rc devices and the dashboard via the lease it is copied onto. Only
	// meaningful for a hold.
	Reason string `json:"reason,omitempty"`
}

// holdCommand is the fixed, meaningless-by-design command recorded on a
// hold's job row. It is display-only — what a real worker actually runs
// for a hold is chosen by the worker itself (internal/worker's execute),
// never by what is stored here — so even if this value ever reached a
// process, nothing a submitter supplied could run through it: a hold
// submission that carries its own command is rejected outright below,
// before this is ever used.
var holdCommand = []string{"hold"}

// JobView is a job plus the queue position a client needs to show progress.
type JobView struct {
	Job           model.Job `json:"job"`
	QueuePosition int       `json:"queue_position,omitempty"`
}

type DeviceView struct {
	Device model.Device `json:"device"`
	Holder string       `json:"holder,omitempty"`
	JobID  string       `json:"job_id,omitempty"`
	// Kind is the holding lease's kind (model.LeaseKindJob or
	// model.LeaseKindHold), empty when nothing holds the device. Read
	// straight off the lease row, which is where task 8 labels a hold —
	// see the design note in internal/store/allocate.go's assignQueued.
	Kind                string   `json:"kind,omitempty"`
	Reason              string   `json:"reason,omitempty"`
	Command             []string `json:"command,omitempty"`
	ElapsedSeconds      int      `json:"elapsed_seconds"`
	HeartbeatAgeSeconds int      `json:"heartbeat_age_seconds"`
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
	// LabelAgeSeconds is how long ago each label in Labels was last
	// confirmed, computed by the controller against its own clock (see
	// deviceViews' HeartbeatAgeSeconds, which does the same for the same
	// reason: an agent deciding whether to trust a fact must not depend on
	// the reader's own clock, which can be skewed — a CLI-side
	// time.Now().Sub(UpdatedAt) would let a machine with a fast clock read a
	// month-old label as fresh). The absolute timestamp stays on each Label
	// too, for a machine consumer that wants it.
	//
	// Keyed by key+"/"+source. That join is unambiguous even though a label
	// KEY may itself contain "/" (labels come from a worker's YAML config or
	// a probe script, neither of which constrains the key's charset):
	// store.ReplaceLabels rejects any source other than the two literal,
	// equal-length, distinct strings model.SourceDetected ("detected") and
	// model.SourceDeclared ("declared"), so for two entries to collide on
	// this join their combined strings would need equal length AND matching
	// characters in the final 9 bytes ("/detected" vs "/declared", which
	// differ) — impossible regardless of what the key contains. A future
	// third source value must preserve this (equal length, and not a suffix
	// of another source) or switch the join to an unambiguous delimiter.
	LabelAgeSeconds map[string]int `json:"label_age_seconds,omitempty"`
	Sheet           string         `json:"sheet,omitempty"`
	SheetUpdatedAt  time.Time      `json:"sheet_updated_at,omitempty"`
	// SheetAgeSeconds is SheetUpdatedAt's age, computed by the controller
	// for the same clock-skew reason as LabelAgeSeconds above. Zero when
	// there is no sheet at all (SheetUpdatedAt is the zero time) rather than
	// the multi-decade figure a naive now.Sub of the zero time would give.
	SheetAgeSeconds int `json:"sheet_age_seconds"`
	// SheetIsHostWide is true when Sheet fell back to the host-wide note
	// because this device has none of its own — an agent reading "don't run
	// more than two jobs here" needs to know whether that applies to the
	// whole box or just this card.
	SheetIsHostWide bool        `json:"sheet_is_host_wide,omitempty"`
	RecentJobs      []model.Job `json:"recent_jobs,omitempty"`
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
	// documentation that exists just because it lives one level up. Which
	// one actually landed is reported back (sheetIsHostWide) so the
	// rendering — and the agent reading it — can tell "applies to this
	// card" from "applies to the whole box".
	//
	// The fallback triggers on an empty body alone, NOT "empty body and a
	// zero timestamp" as an earlier version of this check required. A real
	// worker's readSheets always sends an explicit (if empty) per-device
	// entry for every device it declares — a missing host.d/<name>.md reads
	// the same as an empty one — so applyDeviceFacts stores a real,
	// non-zero-timestamped row for a device that has never actually had its
	// own sheet. Requiring a zero timestamp on top of an empty body meant
	// that row's mere existence, from the device's first registration
	// onward, permanently defeated this fallback: host.md — the single most
	// common way to document a box — never surfaced for any of its devices.
	// An empty body has nothing to show either way, so falling back on it
	// alone is correct: there is no useful distinction left to make between
	// "never registered a sheet" and "registered an empty one".
	sheet, sheetAt, err := s.cfg.Store.HostDoc(view.Device.Host, id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	sheetIsHostWide := false
	if sheet == "" {
		sheet, sheetAt, err = s.cfg.Store.HostDoc(view.Device.Host, "")
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
		sheetIsHostWide = true
	}

	recent, err := s.cfg.Store.RecentJobsForDevice(id, describeRecentJobs)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}

	// Ages are computed here, once, against the controller's own clock —
	// see DescribeResponse.LabelAgeSeconds's doc comment for why this must
	// not be left to the CLI to compute against the reader's clock. Deliberately
	// not clamped at zero: a label or sheet stamped AHEAD of the controller's
	// clock (a worker with a fast clock) produces a negative age here, and
	// that is reported as-is rather than floored to 0 — flooring it would
	// render the least trustworthy reading (a fact from the future) as if it
	// were the freshest possible one. formatAge on the CLI side is what
	// turns a negative value into an explicit "in the future" warning.
	now := s.cfg.Clock.Now()
	labelAges := make(map[string]int, len(labels))
	for _, l := range labels {
		labelAges[l.Key+"/"+l.Source] = int(now.Sub(l.UpdatedAt).Seconds())
	}
	sheetAge := 0
	if !sheetAt.IsZero() {
		sheetAge = int(now.Sub(sheetAt).Seconds())
	}

	writeJSON(w, http.StatusOK, DescribeResponse{
		Device:              view.Device,
		Holder:              view.Holder,
		JobID:               view.JobID,
		ElapsedSeconds:      view.ElapsedSeconds,
		HeartbeatAgeSeconds: view.HeartbeatAgeSeconds,
		Labels:              labels,
		LabelAgeSeconds:     labelAges,
		Sheet:               sheet,
		SheetUpdatedAt:      sheetAt,
		SheetAgeSeconds:     sheetAge,
		SheetIsHostWide:     sheetIsHostWide,
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
	parsed, err := selector.Parse(sel)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_selector", err.Error())
		return
	}

	// The label snapshot and device list are fetched exactly ONCE for this
	// whole request and matched against locally — for the requested
	// selector AND for every queued selector job below — rather than
	// calling store.MatchingDevices (its own LabelSnapshot + Devices pair)
	// once per queued job. At MaxOpenConns(1) every store query serialises
	// against the scheduler and the heartbeat handler, so a deep selector
	// queue behind a per-job round trip would let a diagnostic command —
	// exactly the kind someone re-runs repeatedly while debugging — stall
	// job scheduling fleet-wide. The matching logic itself is unchanged:
	// this calls the same selector.Selector.Match that MatchingDevices
	// calls internally, just against a shared snapshot instead of a fresh
	// one per selector.
	snapshot, err := s.cfg.Store.LabelSnapshot()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	devices, err := s.cfg.Store.Devices()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}

	// Devices() is already ordered by ID, so appending matches in that
	// order keeps Matching sorted the same way store.MatchingDevices
	// guarantees.
	matching := make([]string, 0, len(devices))
	free := make([]string, 0, len(devices))
	matchSet := make(map[string]bool, len(devices))
	for _, d := range devices {
		if !parsed.Match(snapshot[d.ID]) {
			continue
		}
		matching = append(matching, d.ID)
		matchSet[d.ID] = true
		if d.State == model.DeviceReady {
			free = append(free, d.ID)
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
		// overlap at all, matched against the same shared snapshot rather
		// than a fresh store round trip per job.
		jobSel, err := selector.Parse(j.Selector)
		if err != nil {
			continue
		}
		for _, d := range devices {
			if matchSet[d.ID] && jobSel.Match(snapshot[d.ID]) {
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

	kind := req.Kind
	if kind == "" {
		kind = model.LeaseKindJob
	}
	switch kind {
	case model.LeaseKindJob:
		if len(req.Command) == 0 {
			writeErr(w, http.StatusBadRequest, "bad_request", "command required")
			return
		}
	case model.LeaseKindHold:
		// The sleeper a hold runs — its command, its working directory, and
		// its environment — is the worker's choice, never the submitter's
		// (see internal/worker's execute). This is the one place that
		// guarantee is actually enforced: a hold submission carrying any of
		// the three is refused outright, rejected rather than silently
		// blanked, so "--kind hold" can never become a way to run arbitrary
		// code (or point it at an attacker-chosen cwd, or hand it
		// LD_PRELOAD via env) labelled as something else. Blanking instead
		// of rejecting was considered and rejected: a client sending these
		// for a hold is confused about what a hold is, and silently
		// discarding two thirds of its request is worse than telling it so.
		if len(req.Command) != 0 {
			writeErr(w, http.StatusBadRequest, "bad_request",
				"a hold may not specify a command: the worker supplies its own")
			return
		}
		if req.Cwd != "" {
			writeErr(w, http.StatusBadRequest, "bad_request",
				"a hold may not specify a cwd: the worker's sleeper does not use one")
			return
		}
		if len(req.Env) != 0 {
			writeErr(w, http.StatusBadRequest, "bad_request",
				"a hold may not specify env: the worker's sleeper does not use it")
			return
		}
		if req.IdleTimeoutSeconds != 0 {
			writeErr(w, http.StatusBadRequest, "bad_request",
				"a hold may not specify idle_timeout_seconds: the worker's sleeper produces no output, so an idle timeout would kill the hold almost immediately")
			return
		}
		// --ttl is required for a hold, capped by the device's max_runtime
		// exactly as a job's is (checked below via the ordinary ceiling
		// path once MaxRuntimeSeconds is known to be positive) — rejected,
		// never clamped, never silently defaulted to "no expiry".
		if req.MaxRuntimeSeconds <= 0 {
			writeErr(w, http.StatusBadRequest, "bad_request", "a hold requires --ttl")
			return
		}
		req.Command = holdCommand
	default:
		writeErr(w, http.StatusBadRequest, "bad_request",
			fmt.Sprintf("unknown kind %q: must be %q or %q", req.Kind, model.LeaseKindJob, model.LeaseKindHold))
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
	// A malformed selector is validated here, before Enqueue, so it maps to
	// the same 400 bad_selector handleExplain already gives — not a 500.
	// store.Enqueue wraps selector.Parse's error as "selector: %w" with no
	// sentinel to test for, so without this pre-check it falls through to
	// the generic store_error 500 below: a typo'd selector would read as
	// "the controller is broken" instead of naming the bad term.
	if req.Selector != "" {
		if _, err := selector.Parse(req.Selector); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_selector", err.Error())
			return
		}
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
		Kind:        kind,
		Reason:      req.Reason,
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
	// One query for every device's labels, not one per device — see
	// AllLabels's own doc comment for why deviceViews in particular must
	// avoid that N+1 shape.
	labels, err := s.cfg.Store.AllLabels()
	if err != nil {
		return nil, err
	}
	byDevice := map[string]struct {
		holder string
		jobID  string
		kind   string
		reason string
		since  time.Time
	}{}
	for _, l := range leases {
		byDevice[l.DeviceID] = struct {
			holder string
			jobID  string
			kind   string
			reason string
			since  time.Time
		}{l.Holder, l.JobID, l.Kind, l.Reason, l.AcquiredAt}
	}

	now := s.cfg.Clock.Now()
	out := make([]DeviceView, 0, len(devices))
	for _, d := range devices {
		d.Labels = labels[d.ID]
		v := DeviceView{
			Device:              d,
			HeartbeatAgeSeconds: int(now.Sub(d.LastHeartbeatAt).Seconds()),
		}
		if l, ok := byDevice[d.ID]; ok {
			v.Holder = l.holder
			v.JobID = l.jobID
			v.Kind = l.kind
			v.Reason = l.reason
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
