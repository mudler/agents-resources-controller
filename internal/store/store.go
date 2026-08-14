// Package store owns all controller state. The allocation transaction here is
// the mutex that replaces flock: it cannot be bypassed or pointed at a
// divergent path.
package store

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/model"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// ErrDeviceNotFound means the device ID named by a caller matches no row.
// It exists so an update that changed nothing can never be mistaken for one
// that did: see SetDeviceState.
var ErrDeviceNotFound = errors.New("device not found")

// ErrWorkerNotFound means the worker ID named by a caller matches no row.
var ErrWorkerNotFound = errors.New("worker not found")

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

// Now returns the controller's current time as seen through its Clock, so
// tests using a fake clock can stamp rows consistently with the store.
func (s *Store) Now() time.Time { return s.clock.Now() }

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

	// The reap pass above only reaches devices that still had a job in flight
	// on this worker. A host that stayed down longer than the reaper's
	// unhealthyAfter has none: the sweep already marked its jobs lost and
	// quarantined every one of its devices, so without this the boot-ID
	// recovery the spec promises would work for a host that comes back inside
	// five minutes and silently stop working for one rebooted overnight —
	// which is the common case, and exactly the situation that trains
	// operators to clear reflexively.
	if rebooted {
		if err := s.restoreRebootedDevicesLocked(tx, w.ID); err != nil {
			return fmt.Errorf("restore rebooted devices for %s: %w", w.ID, err)
		}
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
		// The quarantine reason travels with the state it explains: a
		// preserved unhealthy keeps the reason it was quarantined with (or a
		// reboot would no longer be able to tell a fault from a lost worker),
		// and any other state carries none.
		if _, err := tx.Exec(
			`INSERT INTO devices (id, host, name, worker_id, state, last_heartbeat_at)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   worker_id = excluded.worker_id,
			   last_heartbeat_at = excluded.last_heartbeat_at,
			   state = CASE WHEN devices.state = ? THEN devices.state ELSE excluded.state END,
			   quarantine_reason = CASE WHEN devices.state = ? THEN devices.quarantine_reason ELSE '' END`,
			d.ID, d.Host, d.Name, w.ID, string(state), w.LastHeartbeatAt.Unix(),
			string(model.DeviceUnhealthy), string(model.DeviceUnhealthy),
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
		if rebooted {
			// A reboot proves no process survived, but proves nothing about
			// the hardware: a device already unhealthy for a self-reported
			// fault (SetDeviceState) must stay quarantined until an explicit
			// clear, not be resurrected just because the host power-cycled.
			// Only devices left busy/unknown by the dead job are proven
			// clean by the reboot here; an unhealthy one is decided by its
			// recorded quarantine reason, in restoreRebootedDevicesLocked.
			if _, err := tx.Exec(
				`UPDATE devices SET state = ?, quarantine_reason = '' WHERE id = ? AND state IN (?, ?)`,
				string(model.DeviceReady), j.deviceID,
				string(model.DeviceBusy), string(model.DeviceUnknown),
			); err != nil {
				return fmt.Errorf("restore device %s: %w", j.deviceID, err)
			}
		} else {
			// An existing quarantine keeps its own reason: a device already
			// out for a hardware fault must not be relabelled as merely
			// having had a worker restart under it, or a later reboot would
			// hand the faulty hardware back to the pool.
			if _, err := tx.Exec(
				`UPDATE devices SET state = ?,
				   quarantine_reason = CASE WHEN state = ? THEN quarantine_reason ELSE ? END
				 WHERE id = ?`,
				string(model.DeviceUnhealthy), string(model.DeviceUnhealthy),
				quarantineRegistration, j.deviceID,
			); err != nil {
				return fmt.Errorf("quarantine device %s: %w", j.deviceID, err)
			}
		}
	}
	return nil
}

// restoreRebootedDevicesLocked returns this worker's quarantined devices to
// the pool after a PROVEN reboot (a changed boot ID) — but only the ones whose
// quarantine the reboot actually answers.
//
// A quarantine means one of two things. Either we could not prove no process
// was still pinning the device (its worker vanished, its lease lapsed, or it
// re-registered with a job in flight) — a reboot is exactly the proof that was
// missing, so the device is clean. Or the host itself reported the hardware
// faulty, which a power cycle does not settle; that one stays out, along with
// any quarantine whose cause was never recorded.
//
// Devices with a live lease are excluded on principle, the same rule
// ClearDevice enforces: nothing may contradict the lease table. In practice
// the reap pass has already released the leases of this worker's in-flight
// jobs, so this is a guard against a lease this path does not know about
// rather than an expected case.
func (s *Store) restoreRebootedDevicesLocked(tx *sql.Tx, workerID string) error {
	args := append([]any{
		string(model.DeviceReady), workerID, string(model.DeviceUnhealthy),
	}, rebootClearableReasons...)
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(rebootClearableReasons)), ", ")

	_, err := tx.Exec(fmt.Sprintf(
		`UPDATE devices SET state = ?, quarantine_reason = ''
		 WHERE worker_id = ? AND state = ?
		   AND quarantine_reason IN (%s)
		   AND id NOT IN (SELECT device_id FROM leases WHERE released_at IS NULL)`,
		placeholders), args...)
	return err
}

// SetDeviceState is the host's own report about one of its devices. Marking a
// device unhealthy through this path is a self-reported FAULT — the worker (or
// a verify probe standing in for it) saying the hardware itself is not fit to
// hand out — which is recorded as such: a fault outlives a reboot, unlike a
// quarantine that merely reflects a process nobody can account for. Any other
// state clears the reason along with the quarantine it explained.
//
// A device ID that matches no row yields ErrDeviceNotFound rather than a
// silent success. An UPDATE that changes zero rows is not an error to SQL,
// but it is very much one here: the caller believes it has just quarantined
// a device, and a worker that logs a successful fault report having changed
// nothing is the worst possible outcome — the device stays schedulable and
// nobody is looking for it.
func (s *Store) SetDeviceState(id string, state model.DeviceState, at time.Time) error {
	reason := ""
	if state == model.DeviceUnhealthy {
		reason = quarantineFault
	}
	res, err := s.db.Exec(
		`UPDATE devices SET state = ?, quarantine_reason = ?, last_heartbeat_at = ? WHERE id = ?`,
		string(state), reason, at.Unix(), id,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: %s", ErrDeviceNotFound, id)
	}
	return nil
}

// WorkerHost returns the host a registered worker ID belongs to, so a
// caller can verify a request naming that worker ID actually agrees about
// which host it is — see handlePushLabels for why: without this check, a
// worker with a typo'd or stale `host:` in worker.yaml could push labels
// that silently land nowhere (a device ID for a host that doesn't exist)
// while reporting success forever, or overwrite another host's labels
// outright if the typo happens to collide with a real one.
//
// An ID that matches no row yields ErrWorkerNotFound rather than an empty
// string, so "unknown worker" is never silently treated as "empty host".
func (s *Store) WorkerHost(workerID string) (string, error) {
	var host string
	err := s.db.QueryRow(`SELECT host FROM workers WHERE id = ?`, workerID).Scan(&host)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: %s", ErrWorkerNotFound, workerID)
	}
	if err != nil {
		return "", err
	}
	return host, nil
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

// RecentJobsForDevice returns up to limit jobs that have run (or are
// running) on this device, most recent submission first — the history
// `rc describe` shows so an agent can see what a box has actually been
// doing, not just what it's doing right now. A device with no history, or
// one this controller has never heard of, yields an empty slice rather than
// an error: a freshly registered device legitimately has nothing to show.
func (s *Store) RecentJobsForDevice(deviceID string, limit int) ([]model.Job, error) {
	rows, err := s.db.Query(
		`SELECT id FROM jobs WHERE device_id = ? ORDER BY submitted_at DESC, rowid DESC LIMIT ?`,
		deviceID, limit)
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

	// Rows are fully drained above before Job() issues its own query: the
	// pool is capped at one connection, so a write or read with the cursor
	// still open would deadlock.
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
