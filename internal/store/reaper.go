package store

import (
	"fmt"
	"strings"
	"time"

	"github.com/mudler/resource-controller/internal/model"
)

// A quarantine reason records WHY a device was taken out of the pool, which
// is the only thing that makes host-reboot recovery safe to apply after the
// fact. A reboot proves no process survived; it proves nothing about the
// hardware. So a device quarantined because a process might still be pinning
// it — the worker vanished, its lease lapsed, or registration reconciled a
// job it left in flight — is answered by proof of a reboot and goes back to
// ready, while one quarantined by a self-reported fault stays out until a
// human clears it, power cycle or no power cycle.
//
// An empty reason means "cause unknown" (a row quarantined before this was
// recorded, i.e. before the migration that added the column). Unknown is
// treated as not revivable: the failure of leaving a healthy device out of
// the pool is a human clearing it, and the failure of the opposite is an OOM
// on hardware nobody checked.
const (
	quarantineFault        = "fault"         // self-reported by the host (SetDeviceState)
	quarantineWorkerLost   = "worker_lost"   // the worker stopped reporting entirely
	quarantineLeaseExpired = "lease_expired" // the lease lapsed with nobody renewing it
	quarantineRegistration = "registration"  // a re-registering worker left a job in flight
)

// rebootClearableReasons are the quarantine causes a proven boot-ID change
// answers. Anything else — a fault, or an unrecorded cause — survives the
// reboot untouched.
var rebootClearableReasons = []any{
	quarantineWorkerLost, quarantineLeaseExpired, quarantineRegistration,
}

type SweepResult struct {
	DevicesUnknown   []string `json:"devices_unknown"`
	DevicesUnhealthy []string `json:"devices_unhealthy"`
	JobsLost         []string `json:"jobs_lost"`
	LeasesExpired    []string `json:"leases_expired"`
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
	// reap holds every device past unhealthyAfter, regardless of whether its
	// state actually transitions this sweep. A device that was already
	// unhealthy (e.g. a worker self-reported a fault via SetDeviceState while
	// its worker later went silent too) must still have its in-flight job
	// reaped, or the job and lease are stranded forever.
	var reap []string

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
		case silence >= unhealthyAfter:
			reap = append(reap, id)
			if state != model.DeviceUnhealthy {
				pending = append(pending, demotion{id, model.DeviceUnhealthy})
				res.DevicesUnhealthy = append(res.DevicesUnhealthy, id)
			}
		case silence >= grace &&
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
		// A demotion to unknown is not a quarantine — the device may well come
		// back on the next heartbeat — so it carries no reason. A demotion to
		// unhealthy records worker loss, which a proven reboot can answer.
		reason := ""
		if d.state == model.DeviceUnhealthy {
			reason = quarantineWorkerLost
		}
		if _, err := tx.Exec(
			`UPDATE devices SET state = ?, quarantine_reason = ? WHERE id = ?`,
			string(d.state), reason, d.id); err != nil {
			return res, fmt.Errorf("demote %s: %w", d.id, err)
		}
	}

	for _, deviceID := range reap {
		// The worker is gone for good: its in-flight jobs are lost and their
		// leases must be released, or the device is stuck forever. This runs
		// for every device past unhealthyAfter, not just ones that just
		// transitioned, so an already-unhealthy device with a still-live job
		// (e.g. one demoted earlier by SetDeviceState) still gets reaped. It
		// is idempotent: once a job is no longer assigned/running, the query
		// below finds nothing and reports it lost only once.
		jobRows, err := tx.Query(
			`SELECT id FROM jobs WHERE device_id = ? AND state IN (?, ?)`,
			deviceID, string(model.JobAssigned), string(model.JobRunning))
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

	// A lease past its expiry is released, but its device is quarantined
	// rather than freed: expiry tells us the holder stopped renewing, not
	// that the hardware is idle. expires_at is a deadline, not a "still
	// live until" marker: at now == expires_at the TTL has fully elapsed,
	// so <= (not <) is correct here.
	expRows, err := tx.Query(
		`SELECT id, job_id, device_id FROM leases
		 WHERE released_at IS NULL AND expires_at <= ?`, now.Unix())
	if err != nil {
		return res, err
	}
	type expired struct{ leaseID, jobID, deviceID string }
	var stale []expired
	for expRows.Next() {
		var e expired
		if err := expRows.Scan(&e.leaseID, &e.jobID, &e.deviceID); err != nil {
			expRows.Close()
			return res, err
		}
		stale = append(stale, e)
	}
	expRows.Close()
	if err := expRows.Err(); err != nil {
		return res, err
	}

	for _, e := range stale {
		// Released by the lease's own id, not job_id: job_id defaults to ""
		// and a future lease kind (interactive holds) will have no job at
		// all, so two live leases could otherwise share a job_id and
		// expiring one would release the other.
		if _, err := tx.Exec(
			`UPDATE leases SET released_at = ? WHERE id = ? AND released_at IS NULL`,
			now.Unix(), e.leaseID); err != nil {
			return res, err
		}
		// An already-unhealthy device keeps the reason it was quarantined
		// with: a hardware fault reported earlier must not be relabelled as a
		// mere expired lease, or a later reboot would revive it.
		if _, err := tx.Exec(
			`UPDATE devices SET state = ?,
			   quarantine_reason = CASE WHEN state = ? THEN quarantine_reason ELSE ? END
			 WHERE id = ?`,
			string(model.DeviceUnhealthy), string(model.DeviceUnhealthy),
			quarantineLeaseExpired, e.deviceID); err != nil {
			return res, err
		}
		if e.jobID != "" {
			result, err := tx.Exec(
				`UPDATE jobs SET state = ?, kill_reason = ?, finished_at = ?
				 WHERE id = ? AND state IN (?, ?)`,
				string(model.JobLost), "lease expired", now.Unix(), e.jobID,
				string(model.JobAssigned), string(model.JobRunning))
			if err != nil {
				return res, err
			}
			// Only report what actually changed: the guard above can miss
			// (the job already finished through another path), and
			// unconditionally appending would over-report a job as newly
			// lost when nothing about it changed here.
			n, err := result.RowsAffected()
			if err != nil {
				return res, err
			}
			if n > 0 {
				res.LeasesExpired = append(res.LeasesExpired, e.jobID)
			}
		} else {
			// A job-less lease (a future interactive hold) has nothing else
			// to report, but its own expiry is still real — report the
			// lease's identity rather than dropping it silently.
			res.LeasesExpired = append(res.LeasesExpired, e.leaseID)
		}
	}

	if err := tx.Commit(); err != nil {
		return res, err
	}
	return res, nil
}

// QuarantineReasons returns the recorded quarantine reason of each device in
// ids, keyed by device ID. Only an id with no device row at all is absent
// from the map; a device that exists but has no reason recorded maps to the
// empty string, which a caller must read as "cause unknown" — a row written
// before the column existed carries the same empty string as one written
// today with nothing to say, and neither is evidence of anything.
//
// This exists as a separate, post-commit read rather than as extra fields on
// SweepResult because Sweep is not to be restructured for it, and because
// the read genuinely must happen outside Sweep's transaction: the pool is
// capped at one connection (see store.Open), so a query issued while Sweep's
// own cursors are open would deadlock. The consequence is that the reason
// returned here is the CURRENT one, which is a moment later than the one the
// sweep wrote — another path (a worker self-reporting a fault, a
// re-registration) may have overwritten it in between. That is the honest
// answer for a notification anyway: it describes why the device is out of
// the pool now, not why it was a few milliseconds ago.
func (s *Store) QuarantineReasons(ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	rows, err := s.db.Query(fmt.Sprintf(
		`SELECT id, quarantine_reason FROM devices WHERE id IN (%s)`,
		strings.Join(placeholders, ", ")), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id, reason string
		if err := rows.Scan(&id, &reason); err != nil {
			return nil, err
		}
		out[id] = reason
	}
	return out, rows.Err()
}

// LeaseDevices maps each id in ids to the device its lease was on. The ids
// are the ones SweepResult.LeasesExpired reports, which is a job ID when the
// expired lease had a job and the lease's own ID when it did not (a hold
// taken with no job behind it), so both columns are matched and both are
// keyed in the result. An id that matches no lease is absent.
//
// It answers for released leases too — deliberately. By the time a sweep
// returns, the leases it expired are already released, but the row keeps its
// device_id, which is the only remaining link between an expired lease and
// the hardware it just took out of the pool.
//
// Same discipline as QuarantineReasons: one query, drained to exhaustion,
// run after Sweep's transaction has committed, never inside one. Ordered by
// acquisition so that if a job somehow ever held two leases, the newest is
// the one that wins the key.
func (s *Store) LeaseDevices(ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids)*2)
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}
	for _, id := range ids {
		args = append(args, id)
	}
	list := strings.Join(placeholders, ", ")
	rows, err := s.db.Query(fmt.Sprintf(
		`SELECT id, job_id, device_id FROM leases
		 WHERE id IN (%s) OR job_id IN (%s)
		 ORDER BY acquired_at`, list, list), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var leaseID, jobID, deviceID string
		if err := rows.Scan(&leaseID, &jobID, &deviceID); err != nil {
			return nil, err
		}
		out[leaseID] = deviceID
		if jobID != "" {
			out[jobID] = deviceID
		}
	}
	return out, rows.Err()
}

// leaseRenewWindow is how far past each heartbeat a live job's lease is
// pushed forward. It must comfortably outlast both the heartbeat interval
// and the sweep interval (10s each, see internal/cli/serve.go) — expiry
// exists to catch a worker that has genuinely stopped reporting, not to put
// a clock on honest work that is still heartbeating on schedule. 15 minutes
// leaves many missed heartbeats' worth of slack before a live job's lease
// could ever lapse.
const leaseRenewWindow = 15 * time.Minute

// RecordHeartbeat refreshes a worker, restores its unknown devices, and
// renews the lease of every job the worker reports it is actually
// supervising. A device whose lease is still live returns to busy, NOT to
// ready: the job it was demoted with is still running on it. Promoting a
// leased device to ready would offer an occupied GPU to the next claimant.
// Devices marked unhealthy stay out until explicitly cleared.
//
// Lease renewal here is what makes expiry (Sweep) safe: without it, any job
// running longer than its lease TTL would be killed and its device
// quarantined while still healthy.
//
// runningJobIDs is the crucial input, and the reason this is not simply
// "renew everything this worker owns". A job can be recorded assigned/running
// on a worker that never actually received it — handleAssignments commits the
// running transition before writing the response, so a lost response (a
// controller restart mid-write, a proxy, a decode error at the worker) leaves
// the controller believing a job is running that the worker has no process
// for and will never report on. Renewing on the strength of the worker merely
// being alive kept that job's lease alive forever: the device stayed busy,
// its holder never changed, and lease expiry — the backstop the design
// promises — could never fire, leaving no way to recover the hardware short
// of restarting the worker and clearing the device by hand.
//
// So renewal follows reality: only the leases of jobs the worker names are
// pushed forward. A job the worker is not running stops being renewed, its
// lease lapses, and Sweep reclaims it — marking the job lost and quarantining
// the device, which is exactly the intended behaviour for a job nobody can
// account for.
func (s *Store) RecordHeartbeat(workerID string, at time.Time, runningJobIDs []string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`UPDATE workers SET last_heartbeat_at = ? WHERE id = ?`, at.Unix(), workerID); err != nil {
		return err
	}
	if len(runningJobIDs) > 0 {
		// The worker's claim is still checked against the controller's own
		// records: a named job must belong to this worker and still be
		// assigned/running, so naming someone else's job renews nothing.
		args := []any{
			at.Add(leaseRenewWindow).Unix(), workerID,
			string(model.JobAssigned), string(model.JobRunning),
		}
		placeholders := make([]string, len(runningJobIDs))
		for i, id := range runningJobIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		if _, err := tx.Exec(fmt.Sprintf(
			`UPDATE leases SET expires_at = ?
			 WHERE released_at IS NULL
			   AND job_id IN (SELECT id FROM jobs
			                  WHERE worker_id = ? AND state IN (?, ?) AND id IN (%s))`,
			strings.Join(placeholders, ", ")), args...); err != nil {
			return err
		}
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
// It must not be able to contradict the lease table: a device with a live
// lease stays unhealthy no matter what the operator asserts. The bool return
// tells the caller whether the device was actually cleared, so a live-lease
// refusal is never reported as success.
func (s *Store) ClearDevice(id string) (bool, error) {
	res, err := s.db.Exec(
		`UPDATE devices SET state = ?, quarantine_reason = '' WHERE id = ? AND state = ?
		   AND id NOT IN (SELECT device_id FROM leases WHERE released_at IS NULL)`,
		string(model.DeviceReady), id, string(model.DeviceUnhealthy))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
