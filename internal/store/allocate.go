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

// ErrNoDevice means nothing matching the request was free. Stage 1 has no
// queue, so the caller is told immediately rather than parked.
var ErrNoDevice = errors.New("no device available")

type AllocateRequest struct {
	DeviceID       string // Stage 1: exact device ID. Selectors arrive in Stage 2.
	Command        []string
	Cwd            string
	Env            map[string]string
	Submitter      string
	IdempotencyKey string
	LeaseTTL       time.Duration
}

// Allocate claims a device and creates its job in ONE transaction. Either the
// device flips ready -> busy and the job and lease exist, or nothing happened.
func (s *Store) Allocate(req AllocateRequest) (*model.Job, error) {
	if len(req.Command) == 0 {
		return nil, errors.New("command required")
	}
	if req.LeaseTTL <= 0 {
		req.LeaseTTL = 5 * time.Minute
	}

	now := s.clock.Now()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// A retried submit must not claim a second device.
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

	var deviceID, workerID string
	err = tx.QueryRow(
		`SELECT id, worker_id FROM devices WHERE id = ? AND state = ? LIMIT 1`,
		req.DeviceID, string(model.DeviceReady),
	).Scan(&deviceID, &workerID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoDevice
	}
	if err != nil {
		return nil, fmt.Errorf("select device: %w", err)
	}

	if _, err := tx.Exec(
		`UPDATE devices SET state = ? WHERE id = ? AND state = ?`,
		string(model.DeviceBusy), deviceID, string(model.DeviceReady),
	); err != nil {
		return nil, fmt.Errorf("mark device busy: %w", err)
	}

	envJSON, err := json.Marshal(req.Env)
	if err != nil {
		return nil, err
	}
	cmdJSON, err := json.Marshal(req.Command)
	if err != nil {
		return nil, err
	}

	job := &model.Job{
		ID:             uuid.NewString(),
		Command:        req.Command,
		Cwd:            req.Cwd,
		Env:            req.Env,
		Submitter:      req.Submitter,
		IdempotencyKey: req.IdempotencyKey,
		State:          model.JobAssigned,
		DeviceID:       deviceID,
		WorkerID:       workerID,
		SubmittedAt:    now,
	}

	var key any
	if req.IdempotencyKey != "" {
		key = req.IdempotencyKey
	}
	if _, err := tx.Exec(
		`INSERT INTO jobs (id, selector, command, cwd, env, submitter, idempotency_key,
		                   state, device_id, worker_id, submitted_at)
		 VALUES (?, '', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.ID, string(cmdJSON), job.Cwd, string(envJSON), job.Submitter, key,
		string(job.State), job.DeviceID, job.WorkerID, now.Unix(),
	); err != nil {
		return nil, fmt.Errorf("insert job: %w", err)
	}

	// The partial unique index makes a second live lease impossible even if
	// the state machine above were ever wrong.
	if _, err := tx.Exec(
		`INSERT INTO leases (id, device_id, holder, job_id, acquired_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		uuid.NewString(), deviceID, req.Submitter, job.ID, now.Unix(), now.Add(req.LeaseTTL).Unix(),
	); err != nil {
		return nil, fmt.Errorf("insert lease: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return job, nil
}

// Release ends a job and returns its device to the pool.
func (s *Store) Release(jobID string, state model.JobState, exitCode *int, reason string) error {
	if !state.Terminal() {
		return fmt.Errorf("release requires a terminal state, got %q", state)
	}
	now := s.clock.Now()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var deviceID string
	var current model.JobState
	if err := tx.QueryRow(`SELECT device_id, state FROM jobs WHERE id = ?`, jobID).
		Scan(&deviceID, &current); err != nil {
		return fmt.Errorf("load job: %w", err)
	}
	if current.Terminal() {
		return tx.Commit() // Already released; releasing twice is not an error.
	}

	if _, err := tx.Exec(
		`UPDATE jobs SET state = ?, exit_code = ?, kill_reason = ?, finished_at = ? WHERE id = ?`,
		string(state), exitCode, reason, now.Unix(), jobID,
	); err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	if _, err := tx.Exec(
		`UPDATE leases SET released_at = ? WHERE job_id = ? AND released_at IS NULL`,
		now.Unix(), jobID,
	); err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	// Only a busy device returns to ready: an unhealthy device stays out of
	// the pool until explicitly cleared.
	if _, err := tx.Exec(
		`UPDATE devices SET state = ? WHERE id = ? AND state = ?`,
		string(model.DeviceReady), deviceID, string(model.DeviceBusy),
	); err != nil {
		return fmt.Errorf("free device: %w", err)
	}
	return tx.Commit()
}

func (s *Store) Job(id string) (*model.Job, error) {
	var (
		j                 model.Job
		cmdJSON, envJSON  string
		started, finished sql.NullInt64
		exitCode          sql.NullInt64
		idem              sql.NullString
		submitted         int64
	)
	err := s.db.QueryRow(
		`SELECT id, selector, command, cwd, env, submitter, idempotency_key, state,
		        device_id, worker_id, exit_code, kill_reason, submitted_at, started_at, finished_at
		 FROM jobs WHERE id = ?`, id,
	).Scan(&j.ID, &j.Selector, &cmdJSON, &j.Cwd, &envJSON, &j.Submitter, &idem, &j.State,
		&j.DeviceID, &j.WorkerID, &exitCode, &j.KillReason, &submitted, &started, &finished)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(cmdJSON), &j.Command); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(envJSON), &j.Env); err != nil {
		return nil, err
	}
	j.IdempotencyKey = idem.String
	j.SubmittedAt = time.Unix(submitted, 0).UTC()
	if exitCode.Valid {
		c := int(exitCode.Int64)
		j.ExitCode = &c
	}
	if started.Valid {
		t := time.Unix(started.Int64, 0).UTC()
		j.StartedAt = &t
	}
	if finished.Valid {
		t := time.Unix(finished.Int64, 0).UTC()
		j.FinishedAt = &t
	}
	return &j, nil
}

// MarkRunning records that the worker has actually spawned the process.
func (s *Store) MarkRunning(jobID string, at time.Time) error {
	_, err := s.db.Exec(
		`UPDATE jobs SET state = ?, started_at = ? WHERE id = ? AND state = ?`,
		string(model.JobRunning), at.Unix(), jobID, string(model.JobAssigned),
	)
	return err
}
