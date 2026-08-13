package store

import (
	"fmt"
	"time"

	"github.com/mudler/resource-controller/internal/model"
)

type SweepResult struct {
	DevicesUnknown   []string `json:"devices_unknown"`
	DevicesUnhealthy []string `json:"devices_unhealthy"`
	JobsLost         []string `json:"jobs_lost"`
}

// Sweep demotes devices whose worker has stopped reporting. A device is never
// promoted to ready by this path: silence is not evidence that it is free.
func (s *Store) Sweep(grace, unhealthyAfter time.Duration) (SweepResult, error) {
	var res SweepResult
	now := s.clock.Now()

	tx, err := s.db.Begin()
	if err != nil {
		return res, err
	}
	defer tx.Rollback()

	rows, err := tx.Query(
		`SELECT d.id, d.state, w.last_heartbeat_at
		 FROM devices d JOIN workers w ON w.id = d.worker_id`)
	if err != nil {
		return res, err
	}

	type demotion struct {
		id    string
		state model.DeviceState
	}
	var pending []demotion

	for rows.Next() {
		var id string
		var state model.DeviceState
		var hb int64
		if err := rows.Scan(&id, &state, &hb); err != nil {
			rows.Close()
			return res, err
		}
		silence := now.Sub(time.Unix(hb, 0).UTC())
		switch {
		case silence >= unhealthyAfter && state != model.DeviceUnhealthy:
			pending = append(pending, demotion{id, model.DeviceUnhealthy})
			res.DevicesUnhealthy = append(res.DevicesUnhealthy, id)
		case silence >= grace && silence < unhealthyAfter &&
			(state == model.DeviceReady || state == model.DeviceBusy):
			pending = append(pending, demotion{id, model.DeviceUnknown})
			res.DevicesUnknown = append(res.DevicesUnknown, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, err
	}

	for _, d := range pending {
		if _, err := tx.Exec(`UPDATE devices SET state = ? WHERE id = ?`, string(d.state), d.id); err != nil {
			return res, fmt.Errorf("demote %s: %w", d.id, err)
		}
		if d.state != model.DeviceUnhealthy {
			continue
		}
		// The worker is gone for good: its in-flight jobs are lost and their
		// leases must be released, or the device is stuck forever.
		jobRows, err := tx.Query(
			`SELECT id FROM jobs WHERE device_id = ? AND state IN (?, ?)`,
			d.id, string(model.JobAssigned), string(model.JobRunning))
		if err != nil {
			return res, err
		}
		var lost []string
		for jobRows.Next() {
			var id string
			if err := jobRows.Scan(&id); err != nil {
				jobRows.Close()
				return res, err
			}
			lost = append(lost, id)
		}
		jobRows.Close()
		if err := jobRows.Err(); err != nil {
			return res, err
		}

		for _, id := range lost {
			if _, err := tx.Exec(
				`UPDATE jobs SET state = ?, kill_reason = ?, finished_at = ? WHERE id = ?`,
				string(model.JobLost), "worker lost", now.Unix(), id); err != nil {
				return res, err
			}
			if _, err := tx.Exec(
				`UPDATE leases SET released_at = ? WHERE job_id = ? AND released_at IS NULL`,
				now.Unix(), id); err != nil {
				return res, err
			}
			res.JobsLost = append(res.JobsLost, id)
		}
	}

	if err := tx.Commit(); err != nil {
		return res, err
	}
	return res, nil
}

// RecordHeartbeat refreshes a worker and restores its unknown devices. A
// device whose lease is still live returns to busy, NOT to ready: the job it
// was demoted with is still running on it. Promoting a leased device to ready
// would offer an occupied GPU to the next claimant. Devices marked unhealthy
// stay out until explicitly cleared.
func (s *Store) RecordHeartbeat(workerID string, at time.Time) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`UPDATE workers SET last_heartbeat_at = ? WHERE id = ?`, at.Unix(), workerID); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`UPDATE devices SET state = ?, last_heartbeat_at = ?
		 WHERE worker_id = ? AND state = ?
		   AND id IN (SELECT device_id FROM leases WHERE released_at IS NULL)`,
		string(model.DeviceBusy), at.Unix(), workerID, string(model.DeviceUnknown)); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`UPDATE devices SET state = ?, last_heartbeat_at = ?
		 WHERE worker_id = ? AND state = ?
		   AND id NOT IN (SELECT device_id FROM leases WHERE released_at IS NULL)`,
		string(model.DeviceReady), at.Unix(), workerID, string(model.DeviceUnknown)); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`UPDATE devices SET last_heartbeat_at = ? WHERE worker_id = ?`, at.Unix(), workerID); err != nil {
		return err
	}
	return tx.Commit()
}

// ClearDevice is the explicit operator acknowledgement that a device is free.
func (s *Store) ClearDevice(id string) error {
	_, err := s.db.Exec(
		`UPDATE devices SET state = ? WHERE id = ? AND state = ?`,
		string(model.DeviceReady), id, string(model.DeviceUnhealthy))
	return err
}
