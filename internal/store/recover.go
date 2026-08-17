package store

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/mudler/resource-controller/internal/model"
)

// autoRecoveryWindow and autoRecoveryLimit are the flap guard: a device may
// come back on its own at most autoRecoveryLimit times inside a sliding
// autoRecoveryWindow, after which it waits for a human.
//
// The point is not to ration recoveries — a fleet rolling a new worker image
// recovers every device once and never trips this. It is that a device which
// quarantines, clears, and quarantines again is describing a real problem
// (a card resetting, a driver wedging, a worker crash-looping), and a system
// that silently keeps putting it back hides exactly the signal an operator
// needs. Three in an hour is chosen to sit well above ordinary maintenance
// — even a bad rolling restart touches a device once or twice — and well
// below "nobody will ever notice".
//
// The window slides rather than latching: once the recorded recoveries age
// out, the device is eligible again. A latch would need a second explicit
// human action to release (a clear does not obviously mean "and re-arm
// auto-recovery"), and a device that flapped at 3am and has been healthy
// since should not still be barred at noon.
const (
	autoRecoveryWindow = time.Hour
	autoRecoveryLimit  = 3
)

// Recovery is one device returned to the pool by AutoRecover, and the
// quarantine reason it was cleared FROM. The reason travels back because the
// event this feeds — an operator asking "why is this device back?" — needs
// to name what the device was out for, and by the time the caller looks, the
// row no longer says (the reason is cleared with the quarantine it explains).
type Recovery struct {
	DeviceID string
	Reason   string
}

// AutoRecover returns this worker's quarantined devices to the pool when the
// proof it presented at registration answers what they were quarantined for.
//
// It is the cheap sibling of restoreRebootedDevicesLocked (see store.go),
// which does the same job on the strength of a changed boot ID. The two
// answer the identical question — "can anything from the interrupted job
// still be holding this device?" — and clear the identical set of causes
// (rebootClearableReasons); they differ only in what constitutes the proof.
// A reboot proves it by destroying every process on the machine, which costs
// minutes and a full restart of everything the host runs. A container restart
// proves it by destroying the PID namespace the jobs lived in, and a host
// worker proves it by looking. See model.RecoveryProof.
//
// What is deliberately NOT here:
//
//   - fault. A self-reported hardware problem is not answered by "no
//     processes are running": the probe that reported it tested something
//     this proof does not. It is excluded by rebootClearableReasons, the
//     same list and the same reasoning the reboot path uses.
//   - an unrecorded cause (an empty quarantine_reason). Guessing "probably a
//     lost worker" about a device quarantined before that column existed is
//     the one direction that can hand out bad hardware.
//   - a device with a live lease. Nothing may contradict the lease table —
//     the rule ClearDevice enforces against an operator is enforced here
//     against a proof.
//
// It runs as its own transaction, after UpsertWorker rather than inside it,
// and that ordering is required: UpsertWorker's reap pass is what quarantines
// a device whose worker re-registered with a job in flight, so a recovery
// pass folded into it would either run before the quarantine it exists to
// answer or have to be threaded through the middle of a function that is
// already the most safety-critical one in this package. Between the two calls
// the device is simply still quarantined, which nothing schedules onto, so
// the window is not observable.
func (s *Store) AutoRecover(workerID string, proof model.RecoveryProof) ([]Recovery, error) {
	if !proof.Proves() {
		// Silence is not evidence. A worker that could prove nothing — an old
		// worker with no such field, one that could not read /proc, one told
		// to require a manual clear, or one that looked and found a survivor
		// — leaves every quarantine exactly where it is.
		return nil, nil
	}

	now := s.clock.Now()
	cutoff := now.Add(-autoRecoveryWindow).Unix()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Candidates are read out in full before anything else runs on this
	// connection: the pool is capped at one connection (see Open), so a
	// query issued while these rows are still open deadlocks rather than
	// merely queueing. Same discipline as Sweep.
	args := append([]any{workerID, string(model.DeviceUnhealthy)}, rebootClearableReasons...)
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(rebootClearableReasons)), ", ")
	rows, err := tx.Query(fmt.Sprintf(
		`SELECT id, quarantine_reason FROM devices
		 WHERE worker_id = ? AND state = ?
		   AND quarantine_reason IN (%s)
		   AND id NOT IN (SELECT device_id FROM leases WHERE released_at IS NULL)
		 ORDER BY id`, placeholders), args...)
	if err != nil {
		return nil, err
	}
	var candidates []Recovery
	for rows.Next() {
		var r Recovery
		if err := rows.Scan(&r.DeviceID, &r.Reason); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	recovered := make([]Recovery, 0, len(candidates))
	for _, cand := range candidates {
		// Aged-out records are deleted rather than merely ignored, so this
		// table stays bounded by (devices * autoRecoveryLimit) instead of
		// growing for the life of the controller. Nothing is lost by it: the
		// events emitted for each recovery are the durable history, this
		// table is only the counter behind the guard.
		if _, err := tx.Exec(
			`DELETE FROM device_recoveries WHERE device_id = ? AND at < ?`, cand.DeviceID, cutoff); err != nil {
			return nil, fmt.Errorf("prune recovery history for %s: %w", cand.DeviceID, err)
		}
		var recent int
		if err := tx.QueryRow(
			`SELECT COUNT(*) FROM device_recoveries WHERE device_id = ?`, cand.DeviceID).Scan(&recent); err != nil {
			return nil, fmt.Errorf("count recoveries for %s: %w", cand.DeviceID, err)
		}
		if recent >= autoRecoveryLimit {
			// Logged at warn, not debug: this is the moment the system stops
			// healing itself and starts waiting for a person, and nothing
			// else will say so.
			slog.Warn("device has auto-recovered too often to keep doing it; it stays quarantined until an operator clears it",
				"device", cand.DeviceID, "recoveries", recent, "window", autoRecoveryWindow, "reason", cand.Reason)
			continue
		}

		// The state guard is repeated in the UPDATE even though the SELECT
		// above already filtered on it: both statements are inside the same
		// transaction, so this cannot actually race here, and it costs
		// nothing to make the statement true on its own rather than only in
		// the context of the query that fed it.
		res, err := tx.Exec(
			`UPDATE devices SET state = ?, quarantine_reason = '', quarantine_detail = ''
			 WHERE id = ? AND state = ?`,
			string(model.DeviceReady), cand.DeviceID, string(model.DeviceUnhealthy))
		if err != nil {
			return nil, fmt.Errorf("recover device %s: %w", cand.DeviceID, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			continue
		}
		if _, err := tx.Exec(
			`INSERT INTO device_recoveries (device_id, at, reason) VALUES (?, ?, ?)`,
			cand.DeviceID, now.Unix(), cand.Reason); err != nil {
			return nil, fmt.Errorf("record recovery of %s: %w", cand.DeviceID, err)
		}
		recovered = append(recovered, cand)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return recovered, nil
}
