package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mudler/agents-resources-controller/internal/model"
)

// ErrRuntimeAboveCeiling means the job asked for more wall clock than the
// device tolerates. We reject rather than clamp: a silently shortened budget
// produces a run whose submitter believes it had longer.
var ErrRuntimeAboveCeiling = errors.New("requested runtime exceeds the device ceiling")

type EnqueueRequest struct {
	DeviceID       string
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
// devices. Called during registration.
func (s *Store) SetDeviceMaxRuntime(deviceID string, d time.Duration) error {
	_, err := s.db.Exec(`UPDATE devices SET max_runtime = ? WHERE id = ?`,
		int64(d.Seconds()), deviceID)
	return err
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

	var ceiling int64
	err = tx.QueryRow(`SELECT max_runtime FROM devices WHERE id = ?`, req.DeviceID).Scan(&ceiling)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("unknown device %q", req.DeviceID)
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
		 VALUES (?, '', ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?)`,
		id, string(cmdJSON), req.Cwd, string(envJSON), req.Submitter, key,
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
	reserved := map[string]string{} // device -> job that holds the reservation

	for _, job := range queued {
		if holder, taken := reserved[job.DeviceID]; taken && holder != job.ID {
			continue // someone ahead of us is waiting for this device
		}

		out, err := s.assignQueued(job.ID, job.DeviceID)
		switch {
		case err == nil:
			assigned = append(assigned, *out)
		case errors.Is(err, ErrNoDevice):
			// Not free: hold it for this job so later jobs cannot jump ahead.
			reserved[job.DeviceID] = job.ID
			if err := s.reserve(job.ID, job.DeviceID); err != nil {
				return nil, err
			}
		default:
			return nil, err
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

// QueuePosition is 1-based among jobs waiting for the same device; 0 means the
// job is not queued.
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
	var ahead int
	err = s.db.QueryRow(
		`SELECT COUNT(*) FROM jobs
		 WHERE state = ? AND device_id = ?
		   AND (priority > ? OR (priority = ? AND (queued_at < ? OR (queued_at = ? AND rowid < (SELECT rowid FROM jobs WHERE id = ?)))))`,
		string(model.JobQueued), job.DeviceID,
		job.Priority, job.Priority, queuedAt, queuedAt, job.ID,
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
