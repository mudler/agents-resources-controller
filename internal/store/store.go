// Package store owns all controller state. The allocation transaction here is
// the mutex that replaces flock: it cannot be bypassed or pointed at a
// divergent path.
package store

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/model"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

type Store struct {
	db    *sql.DB
	clock clock.Clock
}

func Open(path string, c clock.Clock) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// One writer: SQLite serializes writes anyway, and this makes the
	// allocation transaction's behaviour obvious rather than emergent.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, clock: c}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// UpsertWorker registers a worker and its declared devices. A worker that
// registers is a fresh process announcing it has no running jobs — nothing
// a brand-new process could be supervising survives a restart — so
// registration must reconcile whatever the previous process left in flight
// before it touches device state at all. Without that reconciliation a
// restart either strands a job "running" forever with its device stuck
// busy (nothing else keys off worker identity to notice), or, once the
// reaper has already demoted the device, falsifies it as ready while an
// orphaned process from the dead worker may still be pinning it. Both are
// exactly what "never hand out a device we cannot prove is free" forbids,
// so every device backing a reaped in-flight job comes back unhealthy —
// never ready, never left busy — and only an explicit clear (or a verify
// probe standing in for one) puts it back in the pool.
func (s *Store) UpsertWorker(w model.Worker, devices []model.Device) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var priorBootID string
	err = tx.QueryRow(`SELECT boot_id FROM workers WHERE id = ?`, w.ID).Scan(&priorBootID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read prior boot id: %w", err)
	}

	// A changed boot ID is proof the machine rebooted, so nothing from before
	// can still be holding a device. An unchanged or missing ID proves
	// nothing, and an orphaned process may still pin VRAM.
	rebooted := priorBootID != "" && w.BootID != "" && priorBootID != w.BootID
	reason := "worker re-registered"
	if rebooted {
		reason = "host rebooted"
	}

	if _, err := tx.Exec(
		`INSERT INTO workers (id, host, boot_id, last_heartbeat_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET host = excluded.host, boot_id = excluded.boot_id, last_heartbeat_at = excluded.last_heartbeat_at`,
		w.ID, w.Host, w.BootID, w.LastHeartbeatAt.Unix(),
	); err != nil {
		return fmt.Errorf("upsert worker: %w", err)
	}

	// The reap pass quarantines the DEVICE ROW directly (not just the job),
	// so it applies even if this registration's device list no longer
	// declares that device (a dropped or renamed entry in worker.yaml) — the
	// upsert loop below must not be the only thing standing between a
	// stranded job and a device that goes back to ready.
	if err := s.reapInFlightJobsLocked(tx, w.ID, w.LastHeartbeatAt, reason, rebooted); err != nil {
		return fmt.Errorf("reap in-flight jobs for %s: %w", w.ID, err)
	}

	for _, d := range devices {
		state := d.State
		if state == "" {
			state = model.DeviceReady
		}
		// The reap pass above already quarantined every device that had a
		// live job on this worker, so the only device state worth
		// preserving across a re-registration is an existing unhealthy one
		// (e.g. a verify-probe fault with no job attached, or the
		// quarantine that reap pass just applied) — that still requires an
		// explicit clear, registration must not paper over it. Anything
		// else — ready, busy, unknown — is superseded by what was just
		// computed above.
		if _, err := tx.Exec(
			`INSERT INTO devices (id, host, name, worker_id, state, last_heartbeat_at)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   worker_id = excluded.worker_id,
			   last_heartbeat_at = excluded.last_heartbeat_at,
			   state = CASE WHEN devices.state = ? THEN devices.state ELSE excluded.state END`,
			d.ID, d.Host, d.Name, w.ID, string(state), w.LastHeartbeatAt.Unix(), string(model.DeviceUnhealthy),
		); err != nil {
			return fmt.Errorf("upsert device %s: %w", d.ID, err)
		}
	}
	return tx.Commit()
}

// reapInFlightJobsLocked marks every job this worker still has in "assigned"
// or "running" state as lost, releases its lease, and updates the device it
// occupied — all within the caller's transaction. Normally that means
// quarantining the device as unhealthy: silence (or an unannounced restart
// with the same boot ID) is never proof a device is free. The one exception
// is rebooted, set when the caller has proof — a changed boot ID — that the
// machine restarted and nothing from before can still be running; only then
// does the device go back to ready instead.
func (s *Store) reapInFlightJobsLocked(tx *sql.Tx, workerID string, at time.Time, reason string, rebooted bool) error {
	rows, err := tx.Query(
		`SELECT id, device_id FROM jobs WHERE worker_id = ? AND state IN (?, ?)`,
		workerID, string(model.JobAssigned), string(model.JobRunning))
	if err != nil {
		return err
	}
	type inflight struct{ jobID, deviceID string }
	var jobs []inflight
	for rows.Next() {
		var j inflight
		if err := rows.Scan(&j.jobID, &j.deviceID); err != nil {
			rows.Close()
			return err
		}
		jobs = append(jobs, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	quarantineState := model.DeviceUnhealthy
	if rebooted {
		quarantineState = model.DeviceReady
	}

	for _, j := range jobs {
		if _, err := tx.Exec(
			`UPDATE jobs SET state = ?, kill_reason = ?, finished_at = ? WHERE id = ?`,
			string(model.JobLost), reason, at.Unix(), j.jobID,
		); err != nil {
			return fmt.Errorf("mark job %s lost: %w", j.jobID, err)
		}
		if _, err := tx.Exec(
			`UPDATE leases SET released_at = ? WHERE job_id = ? AND released_at IS NULL`,
			at.Unix(), j.jobID,
		); err != nil {
			return fmt.Errorf("release lease for job %s: %w", j.jobID, err)
		}
		if _, err := tx.Exec(
			`UPDATE devices SET state = ? WHERE id = ?`,
			string(quarantineState), j.deviceID,
		); err != nil {
			return fmt.Errorf("quarantine device %s: %w", j.deviceID, err)
		}
	}
	return nil
}

func (s *Store) SetDeviceState(id string, state model.DeviceState, at time.Time) error {
	_, err := s.db.Exec(
		`UPDATE devices SET state = ?, last_heartbeat_at = ? WHERE id = ?`,
		string(state), at.Unix(), id,
	)
	return err
}

func (s *Store) Devices() ([]model.Device, error) {
	rows, err := s.db.Query(
		`SELECT id, host, name, worker_id, state, last_heartbeat_at FROM devices ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Device
	for rows.Next() {
		var d model.Device
		var hb int64
		if err := rows.Scan(&d.ID, &d.Host, &d.Name, &d.WorkerID, &d.State, &hb); err != nil {
			return nil, err
		}
		d.LastHeartbeatAt = time.Unix(hb, 0).UTC()
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) Leases() ([]model.Lease, error) {
	rows, err := s.db.Query(
		`SELECT id, device_id, holder, job_id, acquired_at, expires_at
		 FROM leases WHERE released_at IS NULL ORDER BY acquired_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Lease
	for rows.Next() {
		var l model.Lease
		var acq, exp int64
		if err := rows.Scan(&l.ID, &l.DeviceID, &l.Holder, &l.JobID, &acq, &exp); err != nil {
			return nil, err
		}
		l.AcquiredAt = time.Unix(acq, 0).UTC()
		l.ExpiresAt = time.Unix(exp, 0).UTC()
		out = append(out, l)
	}
	return out, rows.Err()
}

// AssignedJobsFor returns jobs handed to a worker that it has not started yet.
func (s *Store) AssignedJobsFor(workerID string) ([]model.Job, error) {
	rows, err := s.db.Query(
		`SELECT id FROM jobs WHERE worker_id = ? AND state = ? ORDER BY submitted_at`,
		workerID, string(model.JobAssigned))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
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

// ActiveJobs returns jobs that are assigned or running, newest first.
func (s *Store) ActiveJobs() ([]model.Job, error) {
	rows, err := s.db.Query(
		`SELECT id FROM jobs WHERE state IN (?, ?) ORDER BY submitted_at DESC`,
		string(model.JobAssigned), string(model.JobRunning))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
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
