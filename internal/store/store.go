// Package store owns all controller state. The allocation transaction here is
// the mutex that replaces flock: it cannot be bypassed or pointed at a
// divergent path.
package store

import (
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/model"
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
	return &Store{db: db, clock: c}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) UpsertWorker(w model.Worker, devices []model.Device) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`INSERT INTO workers (id, host, last_heartbeat_at) VALUES (?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET host = excluded.host, last_heartbeat_at = excluded.last_heartbeat_at`,
		w.ID, w.Host, w.LastHeartbeatAt.Unix(),
	); err != nil {
		return fmt.Errorf("upsert worker: %w", err)
	}

	for _, d := range devices {
		state := d.State
		if state == "" {
			state = model.DeviceReady
		}
		// A re-registering worker must not steal a device that is currently
		// leased, so only non-busy devices are reset to ready.
		if _, err := tx.Exec(
			`INSERT INTO devices (id, host, name, worker_id, state, last_heartbeat_at)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   worker_id = excluded.worker_id,
			   last_heartbeat_at = excluded.last_heartbeat_at,
			   state = CASE WHEN devices.state = 'busy' THEN devices.state ELSE excluded.state END`,
			d.ID, d.Host, d.Name, w.ID, string(state), w.LastHeartbeatAt.Unix(),
		); err != nil {
			return fmt.Errorf("upsert device %s: %w", d.ID, err)
		}
	}
	return tx.Commit()
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
