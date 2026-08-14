package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/selector"
)

// ErrRuntimeAboveCeiling means the job asked for more wall clock than the
// device tolerates. We reject rather than clamp: a silently shortened budget
// produces a run whose submitter believes it had longer.
var ErrRuntimeAboveCeiling = errors.New("requested runtime exceeds the device ceiling")

// ErrUnknownDevice means the caller named a device_id that no worker has ever
// registered. Distinct from ErrNoDevice (the device exists but is busy) and
// ErrRuntimeAboveCeiling (the device exists but the requested runtime is too
// long) so a caller — and the HTTP layer — can answer each differently.
var ErrUnknownDevice = errors.New("unknown device")

// ErrNoMatchingDevice means the selector matches no device that exists. We
// reject at submit rather than queue forever: a selector matching nothing is
// far more often a typo than a bet on a host registering later.
var ErrNoMatchingDevice = errors.New("no device matches the selector")

type EnqueueRequest struct {
	DeviceID       string
	Selector       string
	Command        []string
	Cwd            string
	Env            map[string]string
	Submitter      string
	IdempotencyKey string
	Priority       int
	MaxRuntime     time.Duration
	IdleTimeout    time.Duration
}

// SetDeviceMaxRuntime records the ceiling a host declares for one of its
// devices. Called during registration. The column is seconds by design, so a
// sub-second duration truncates — that's intentional, not a bug: runtime
// ceilings are not meant to be enforced to sub-second precision.
func (s *Store) SetDeviceMaxRuntime(deviceID string, d time.Duration) error {
	res, err := s.db.Exec(`UPDATE devices SET max_runtime = ? WHERE id = ?`,
		int64(d.Seconds()), deviceID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: %q", ErrUnknownDevice, deviceID)
	}
	return nil
}

// Enqueue records a job in state queued. It never assigns: ScheduleOnce does
// that, so there is exactly one place where a device changes hands.
func (s *Store) Enqueue(req EnqueueRequest) (*model.Job, error) {
	if len(req.Command) == 0 {
		return nil, errors.New("command required")
	}
	if req.Submitter == "" {
		return nil, errors.New("submitter required")
	}

	switch {
	case req.DeviceID != "" && req.Selector != "":
		return nil, errors.New("give either device_id or selector, not both")
	case req.DeviceID == "" && req.Selector == "":
		return nil, errors.New("device_id or selector required")
	}

	// Selector resolution runs before the transaction opens: MatchingDevices
	// issues its own queries against s.db, and with MaxOpenConns(1) doing
	// that while a tx already holds the only connection would deadlock.
	if req.Selector != "" {
		sel, err := selector.Parse(req.Selector)
		if err != nil {
			return nil, fmt.Errorf("selector: %w", err)
		}
		matches, err := s.MatchingDevices(sel.String())
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("%w: %s", ErrNoMatchingDevice, sel.String())
		}

		// A pinned job is rejected below for asking more than its one named
		// device tolerates. A selector job has no single device yet, but the
		// same rule has to hold: if not one of today's matches can carry the
		// requested runtime, queueing it would either wait forever (no
		// candidate will ever qualify without a ceiling change) or, worse,
		// land on whichever candidate ScheduleOnce reaches first regardless
		// of ceiling. So this is rejected here too, not left to surface as a
		// silent stall or a bypassed ceiling later.
		if req.MaxRuntime > 0 {
			within, err := s.ceilingFilter(matches, req.MaxRuntime)
			if err != nil {
				return nil, err
			}
			if len(within) == 0 {
				ceiling, err := s.deviceCeilings()
				if err != nil {
					return nil, err
				}
				var best time.Duration
				for _, id := range matches {
					if c := ceiling[id]; c > best {
						best = c
					}
				}
				return nil, fmt.Errorf("%w: no device matching %q tolerates %s (highest declared ceiling among matches: %s)",
					ErrRuntimeAboveCeiling, sel.String(), req.MaxRuntime, best)
			}
		}

		req.Selector = sel.String() // store the normalised form
	}

	now := s.clock.Now()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if req.IdempotencyKey != "" {
		var existing string
		err := tx.QueryRow(`SELECT id FROM jobs WHERE idempotency_key = ?`, req.IdempotencyKey).Scan(&existing)
		switch {
		case err == nil:
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			return s.Job(existing)
		case !errors.Is(err, sql.ErrNoRows):
			return nil, fmt.Errorf("idempotency lookup: %w", err)
		}
	}

	// A selector job's ceiling is resolved at assignment time instead: it
	// inherits whatever device it lands on, which is not known yet.
	if req.DeviceID != "" {
		var ceiling int64
		err = tx.QueryRow(`SELECT max_runtime FROM devices WHERE id = ?`, req.DeviceID).Scan(&ceiling)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: %q", ErrUnknownDevice, req.DeviceID)
		}
		if err != nil {
			return nil, err
		}
		if ceiling > 0 {
			limit := time.Duration(ceiling) * time.Second
			if req.MaxRuntime == 0 || req.MaxRuntime > limit {
				if req.MaxRuntime > limit {
					return nil, fmt.Errorf("%w: %s allows at most %s",
						ErrRuntimeAboveCeiling, req.DeviceID, limit)
				}
				// No request: inherit the device's ceiling.
				req.MaxRuntime = limit
			}
		}
	}

	cmdJSON, err := json.Marshal(req.Command)
	if err != nil {
		return nil, err
	}
	envJSON, err := json.Marshal(req.Env)
	if err != nil {
		return nil, err
	}

	var key any
	if req.IdempotencyKey != "" {
		key = req.IdempotencyKey
	}

	id := uuid.NewString()
	if _, err := tx.Exec(
		`INSERT INTO jobs (id, selector, command, cwd, env, submitter, idempotency_key,
		                   state, device_id, worker_id, submitted_at, queued_at,
		                   priority, max_runtime, idle_timeout)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?)`,
		id, req.Selector, string(cmdJSON), req.Cwd, string(envJSON), req.Submitter, key,
		string(model.JobQueued), req.DeviceID, now.Unix(), now.Unix(),
		req.Priority, int64(req.MaxRuntime.Seconds()), int64(req.IdleTimeout.Seconds()),
	); err != nil {
		return nil, fmt.Errorf("insert queued job: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.Job(id)
}

// MatchingDevices returns the device IDs whose effective labels satisfy the
// selector, sorted by ID so scheduling is deterministic.
func (s *Store) MatchingDevices(sel string) ([]string, error) {
	parsed, err := selector.Parse(sel)
	if err != nil {
		return nil, fmt.Errorf("selector: %w", err)
	}
	snap, err := s.LabelSnapshot()
	if err != nil {
		return nil, err
	}
	devices, err := s.Devices()
	if err != nil {
		return nil, err
	}

	var out []string
	for _, d := range devices {
		if parsed.Match(snap[d.ID]) {
			out = append(out, d.ID)
		}
	}
	sort.Strings(out)
	return out, nil
}

// deviceCeilings returns every device's declared runtime ceiling, keyed by
// ID; 0 means none declared. It reads the column directly rather than going
// through Devices(), which deliberately does not surface it (see
// TestRegisterAppliesDeviceRuntimeCeilings).
func (s *Store) deviceCeilings() (map[string]time.Duration, error) {
	rows, err := s.db.Query(`SELECT id, max_runtime FROM devices`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]time.Duration{}
	for rows.Next() {
		var id string
		var sec int64
		if err := rows.Scan(&id, &sec); err != nil {
			return nil, err
		}
		out[id] = time.Duration(sec) * time.Second
	}
	return out, rows.Err()
}

// ceilingFilter narrows matches to the devices whose declared runtime
// ceiling can accommodate requested. A device with no declared ceiling (0)
// always qualifies. requested <= 0 means the job asked for no runtime of its
// own, so nothing is filtered here — it inherits whatever ceiling the
// device it lands on declares, same as it always has.
//
// This backs both Enqueue's submit-time validation and ScheduleOnce's
// per-pass candidate list, so the two can never disagree: a device Enqueue
// would have refused a job for is never later handed to that job by the
// scheduler routing around the label match instead.
func (s *Store) ceilingFilter(matches []string, requested time.Duration) ([]string, error) {
	if requested <= 0 || len(matches) == 0 {
		return matches, nil
	}
	ceiling, err := s.deviceCeilings()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(matches))
	for _, id := range matches {
		if c := ceiling[id]; c == 0 || c >= requested {
			out = append(out, id)
		}
	}
	return out, nil
}

// ScheduleOnce makes one scheduling pass: for each queued job in priority then
// FIFO order, assign it if its device is free, otherwise reserve that device
// so nothing behind it can take it first. Returns the jobs assigned.
func (s *Store) ScheduleOnce() ([]model.Job, error) {
	queued, err := s.QueuedJobs()
	if err != nil {
		return nil, err
	}
	if len(queued) == 0 {
		return nil, s.clearReservations()
	}

	// Reservations are rebuilt from scratch each pass: the head job for a
	// device holds it, and a job that has gone away releases it implicitly.
	if err := s.clearReservations(); err != nil {
		return nil, err
	}

	var assigned []model.Job
	reserved := map[string]bool{} // device -> a queued job ahead of us holds it

	for _, job := range queued {
		candidates := []string{job.DeviceID}
		if job.Selector != "" {
			matches, err := s.MatchingDevices(job.Selector)
			if err != nil {
				// One bad row (e.g. a malformed selector that somehow got
				// past submit-time validation) must not wedge the whole
				// fleet: log it and let the rest of the pass proceed.
				slog.Error("scheduling: selector match failed for a queued job; skipping it this pass",
					"job", job.ID, "selector", job.Selector, "err", err)
				continue
			}
			candidates, err = s.ceilingFilter(matches, time.Duration(job.MaxRuntimeSeconds)*time.Second)
			if err != nil {
				slog.Error("scheduling: device ceiling lookup failed for a queued job; skipping it this pass",
					"job", job.ID, "selector", job.Selector, "err", err)
				continue
			}
			if len(candidates) == 0 {
				// Visible rather than silent: without this the job just
				// sits in state queued forever with no reservation and no
				// trace of why, indistinguishable from a hung scheduler.
				slog.Warn("scheduling: selector job matches no device that also tolerates its runtime this pass",
					"job", job.ID, "selector", job.Selector)
				continue
			}
		}

		// Prune only the candidates someone ahead of us in the queue is
		// already holding — not the whole job. Skipping the job entirely
		// whenever *any* candidate was reserved let a job behind it take a
		// candidate nobody had actually claimed, defeating the reservation
		// and starving a broad selector job behind a stream of narrower
		// ones. Note `reserved` is a map[string]bool — each queued job is
		// visited once per pass, so a job can never encounter its own
		// reservation and no owner needs recording.
		available := make([]string, 0, len(candidates))
		for _, deviceID := range candidates {
			if !reserved[deviceID] {
				available = append(available, deviceID)
			}
		}
		if len(available) == 0 {
			continue // every candidate is already held by a job ahead of us
		}

		placed := false
		for _, deviceID := range available {
			out, err := s.assignQueued(job.ID, deviceID)
			switch {
			case err == nil:
				assigned = append(assigned, *out)
				placed = true
			case errors.Is(err, ErrNoDevice):
				continue // this one is busy; try the next candidate
			case errors.Is(err, errJobNoLongerQueued):
				placed = true // vanished; reserve nothing on its behalf
			default:
				return nil, err
			}
			if placed {
				break
			}
		}
		if placed {
			continue
		}

		// Nothing free among what nobody ahead of us had already claimed:
		// hold all of it for this job so later jobs cannot jump ahead of it
		// either.
		for _, deviceID := range available {
			reserved[deviceID] = true
			if err := s.reserve(job.ID, deviceID); err != nil {
				return nil, err
			}
		}
	}
	return assigned, nil
}

// QueuedJobs returns queued jobs in scheduling order: priority DESC, then
// oldest first. The tie-break is SQLite's implicit rowid rather than
// queued_at or id: queued_at has one-second resolution (two jobs enqueued in
// the same second, or under a frozen test clock, tie on it), and id is a
// random UUID unrelated to submission order. rowid is strictly monotonic
// with insertion order, so it is the only column that reliably preserves
// FIFO when priority and queued_at both tie.
func (s *Store) QueuedJobs() ([]model.Job, error) {
	rows, err := s.db.Query(
		`SELECT id FROM jobs WHERE state = ? ORDER BY priority DESC, queued_at, rowid`,
		string(model.JobQueued))
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]model.Job, 0, len(ids))
	for _, id := range ids {
		j, err := s.Job(id)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, nil
}

// QueuePosition is 1-based. For a pinned job it counts jobs ahead of it
// waiting for the same device. A selector job's candidate set can change
// between scheduling passes (labels change, devices come and go), so it
// counts every queued job ahead of it in scheduling order instead — an
// upper bound on its wait, not an exact slot. 0 means the job is not queued.
func (s *Store) QueuePosition(jobID string) (int, error) {
	job, err := s.Job(jobID)
	if err != nil {
		return 0, err
	}
	if job.State != model.JobQueued {
		return 0, nil
	}
	var queuedAt int64
	if job.QueuedAt != nil {
		queuedAt = job.QueuedAt.Unix()
	}
	// Tie-break on rowid, not id: see the comment on QueuedJobs for why.
	deviceFilter := "device_id = ?"
	args := []any{string(model.JobQueued)}
	if job.Selector != "" {
		deviceFilter = "1 = 1"
	} else {
		args = append(args, job.DeviceID)
	}
	args = append(args, job.Priority, job.Priority, queuedAt, queuedAt, job.ID)

	var ahead int
	err = s.db.QueryRow(
		`SELECT COUNT(*) FROM jobs
		 WHERE state = ? AND `+deviceFilter+`
		   AND (priority > ? OR (priority = ? AND (queued_at < ? OR (queued_at = ? AND rowid < (SELECT rowid FROM jobs WHERE id = ?)))))`,
		args...,
	).Scan(&ahead)
	if err != nil {
		return 0, err
	}
	return ahead + 1, nil
}

// CancelQueued removes a job that has not started. It reports false when the
// job is already assigned or running — that is rc kill's job, not this one,
// because a running job owns a device and a live process.
func (s *Store) CancelQueued(jobID, reason string) (bool, error) {
	now := s.clock.Now()
	res, err := s.db.Exec(
		`UPDATE jobs SET state = ?, kill_reason = ?, finished_at = ?
		 WHERE id = ? AND state = ?`,
		string(model.JobKilled), reason, now.Unix(), jobID, string(model.JobQueued))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	_, err = s.db.Exec(`DELETE FROM reservations WHERE job_id = ?`, jobID)
	return true, err
}

// RequestKill flags a running job for termination. The worker sees the flag on
// its next poll and terminates the process group; the terminal report then
// arrives through the normal path, so there is exactly one place where a job
// ends. It reports whether it actually flagged anything: a job that has
// already left assigned/running (finished, or raced onto a terminal state
// between the caller's lookup and this call) flags nothing, and the caller
// must not report success for a kill that did not land on anything live.
func (s *Store) RequestKill(jobID string) (bool, error) {
	// kill_delivered_at is reset so an operator re-issuing `rc kill` gets the
	// flag re-offered on the next poll rather than waiting out
	// killRedeliverInterval from an earlier delivery.
	res, err := s.db.Exec(
		`UPDATE jobs SET kill_requested = 1, kill_delivered_at = 0 WHERE id = ? AND state IN (?, ?)`,
		jobID, string(model.JobAssigned), string(model.JobRunning))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// killRedeliverInterval is how long a kill flag stays quiet after it has been
// handed to a worker before it is offered again.
//
// Re-delivery has to exist: a poll response can be lost (a dropped
// connection, a proxy, a controller restart mid-write), and a kill that
// reached nobody must still reach the worker eventually. But re-delivering it
// on *every* poll is what turns an unactionable flag into a hot loop —
// handleAssignments ends its long poll the instant there is a kill to report,
// so a flag on a job no worker is actually running (the lost-assignment case
// this stage's heartbeat/lease fix is about) makes every poll return in
// milliseconds and the worker re-poll at its minimum interval, forever.
// Spacing re-delivery out keeps both properties: a real running job still
// gets its kill within one interval of a lost response, and a flag nobody can
// action costs one extra wake every interval instead of thousands.
const killRedeliverInterval = 30 * time.Second

// TakeKillRequests lists the jobs on a worker whose kill flag is due for
// delivery, and stamps them as delivered in the same transaction. It is a
// take, not a read: the stamp is what bounds re-delivery (see
// killRedeliverInterval), so a caller that reads without stamping would
// reintroduce the hot loop.
func (s *Store) TakeKillRequests(workerID string) ([]string, error) {
	now := s.clock.Now()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.Query(
		`SELECT id FROM jobs
		 WHERE worker_id = ? AND kill_requested = 1 AND state IN (?, ?)
		   AND kill_delivered_at <= ?`,
		workerID, string(model.JobAssigned), string(model.JobRunning),
		now.Add(-killRedeliverInterval).Unix())
	if err != nil {
		return nil, err
	}
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Rows are fully drained above before this write: the pool is capped at
	// one connection, so a write issued while the cursor is still open would
	// deadlock.
	for _, id := range out {
		if _, err := tx.Exec(
			`UPDATE jobs SET kill_delivered_at = ? WHERE id = ?`, now.Unix(), id); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) reserve(jobID, deviceID string) error {
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO reservations (job_id, device_id, created_at) VALUES (?, ?, ?)`,
		jobID, deviceID, s.clock.Now().Unix())
	return err
}

func (s *Store) clearReservations() error {
	_, err := s.db.Exec(`DELETE FROM reservations`)
	return err
}
