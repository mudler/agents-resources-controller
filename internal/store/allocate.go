package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mudler/resource-controller/internal/model"
)

// ErrNoDevice means nothing matching the request was free. Stage 1 has no
// queue, so the caller is told immediately rather than parked.
var ErrNoDevice = errors.New("no device available")

// errJobNoLongerQueued means the job assignQueued was asked to assign is no
// longer in state queued (e.g. cancelled between the scheduling pass reading
// its snapshot and reaching this job). It is distinct from ErrNoDevice: the
// device may well be free, and the caller must not reserve it for a job that
// no longer exists, nor let it block whoever is behind it in the queue.
var errJobNoLongerQueued = errors.New("job no longer queued")

const (
	defaultLeaseTTL       = 5 * time.Minute
	leaseGraceOverRuntime = 10 * time.Minute
)

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
//
// It bypasses the queue entirely: there is no queued state, no priority, no
// reservation, and no interaction with ScheduleOnce. That made it stage 1's
// only allocation path; since stage 2, production code must go through
// Enqueue followed by ScheduleOnce instead, which is the only route that
// keeps "who gets a device next" consistent with what a client seeing its
// queue position was told. Allocate is not deleted because the store's own
// tests still use it to set up a device already busy without needing a
// scheduling pass — but a second, queue-bypassing way to hand out a device
// is exactly the kind of thing that turns into a race if production code
// ever calls it again, so: do not wire this into any handler.
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
		j                                           model.Job
		cmdJSON, envJSON                            string
		started, finished                           sql.NullInt64
		exitCode                                    sql.NullInt64
		idem                                        sql.NullString
		submitted                                   int64
		priority, maxRuntime, idleTimeout, queuedAt int64
	)
	err := s.db.QueryRow(
		`SELECT id, selector, command, cwd, env, submitter, idempotency_key, state,
		        device_id, worker_id, exit_code, kill_reason, submitted_at, started_at, finished_at,
		        priority, max_runtime, idle_timeout, queued_at, kind, reason, stdio
		 FROM jobs WHERE id = ?`, id,
	).Scan(&j.ID, &j.Selector, &cmdJSON, &j.Cwd, &envJSON, &j.Submitter, &idem, &j.State,
		&j.DeviceID, &j.WorkerID, &exitCode, &j.KillReason, &submitted, &started, &finished,
		&priority, &maxRuntime, &idleTimeout, &queuedAt, &j.Kind, &j.Reason, &j.Stdio)
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
	j.Priority = int(priority)
	j.MaxRuntimeSeconds = int(maxRuntime)
	j.IdleTimeoutSeconds = int(idleTimeout)
	if queuedAt > 0 {
		t := time.Unix(queuedAt, 0).UTC()
		j.QueuedAt = &t
	}
	return &j, nil
}

// assignQueued moves a queued job onto its device in ONE transaction:
// device ready -> busy, job queued -> assigned, lease inserted. Returns
// ErrNoDevice when the device is not free, leaving the job queued so it
// blocks whoever is behind it — and errJobNoLongerQueued when the job itself
// has moved on (e.g. cancelled) between the caller's snapshot and now, which
// must not block anyone since there is no job left to reserve the device for.
func (s *Store) assignQueued(jobID, deviceID string) (*model.Job, error) {
	now := s.clock.Now()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var workerID string
	var deviceCeiling int64
	err = tx.QueryRow(
		`SELECT worker_id, max_runtime FROM devices WHERE id = ? AND state = ?`,
		deviceID, string(model.DeviceReady)).Scan(&workerID, &deviceCeiling)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoDevice
	}
	if err != nil {
		return nil, fmt.Errorf("select device: %w", err)
	}

	var submitter, kind, reason string
	var ttl int64
	if err := tx.QueryRow(
		`SELECT submitter, max_runtime, kind, reason FROM jobs WHERE id = ? AND state = ?`,
		jobID, string(model.JobQueued)).Scan(&submitter, &ttl, &kind, &reason); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errJobNoLongerQueued
		}
		return nil, err
	}

	// A pinned job already had the device's ceiling applied at submit time
	// in Enqueue. A selector job could not know which device it would land
	// on, so it inherits the landing device's ceiling only now, and only if
	// it asked for none of its own.
	if ttl == 0 && deviceCeiling > 0 {
		ttl = deviceCeiling
	}

	if _, err := tx.Exec(
		`UPDATE devices SET state = ? WHERE id = ? AND state = ?`,
		string(model.DeviceBusy), deviceID, string(model.DeviceReady)); err != nil {
		return nil, fmt.Errorf("mark device busy: %w", err)
	}
	// device_id is set here (not only at Enqueue) because a selector job's
	// row carries no device_id until it lands on one.
	if _, err := tx.Exec(
		`UPDATE jobs SET state = ?, device_id = ?, worker_id = ?, max_runtime = ? WHERE id = ?`,
		string(model.JobAssigned), deviceID, workerID, ttl, jobID); err != nil {
		return nil, fmt.Errorf("assign job: %w", err)
	}

	// A job lease outlives its runtime ceiling by a margin so the worker's own
	// watchdog fires first; expiry is the backstop for a worker that vanishes.
	expiry := now.Add(defaultLeaseTTL)
	if ttl > 0 {
		expiry = now.Add(time.Duration(ttl)*time.Second + leaseGraceOverRuntime)
	}
	// kind and reason are copied straight from the job row onto the lease
	// it is granted: for a hold this is what lets rc devices and the
	// dashboard label the holder as a hold with its reason, rather than a
	// mysterious sleep, without a join back to jobs.
	if _, err := tx.Exec(
		`INSERT INTO leases (id, device_id, holder, job_id, acquired_at, expires_at, kind, reason)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		uuid.NewString(), deviceID, submitter, jobID, now.Unix(), expiry.Unix(), kind, reason); err != nil {
		return nil, fmt.Errorf("insert lease: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM reservations WHERE device_id = ?`, deviceID); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.Job(jobID)
}

// MarkRunning records that the worker has actually spawned the process.
func (s *Store) MarkRunning(jobID string, at time.Time) error {
	_, err := s.db.Exec(
		`UPDATE jobs SET state = ?, started_at = ? WHERE id = ? AND state = ?`,
		string(model.JobRunning), at.Unix(), jobID, string(model.JobAssigned),
	)
	return err
}
