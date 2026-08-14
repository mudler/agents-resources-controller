package store

import (
	"database/sql"
	"fmt"
)

// migrations are applied in order. PRAGMA user_version records how many have
// run, so a database created before this mechanism existed (user_version 0)
// is brought forward rather than silently keeping the old shape. Never edit
// an applied migration — append a new one.
var migrations = []struct {
	name  string
	stmts []string
}{
	{
		name: "stage2 queue, watchdog ceilings, boot identity",
		stmts: []string{
			`ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE jobs ADD COLUMN max_runtime INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE jobs ADD COLUMN idle_timeout INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE jobs ADD COLUMN queued_at INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE devices ADD COLUMN max_runtime INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE workers ADD COLUMN boot_id TEXT NOT NULL DEFAULT ''`,
			`CREATE TABLE IF NOT EXISTS reservations (
			   job_id     TEXT NOT NULL,
			   device_id  TEXT NOT NULL,
			   created_at INTEGER NOT NULL,
			   PRIMARY KEY (job_id, device_id)
			 )`,
			// One reservation per device: the head job holds it until assigned.
			`CREATE UNIQUE INDEX IF NOT EXISTS reservations_one_per_device
			   ON reservations(device_id)`,
			`CREATE INDEX IF NOT EXISTS jobs_queue
			   ON jobs(state, priority DESC, queued_at)`,
		},
	},
	{
		name: "kill requests",
		stmts: []string{
			`ALTER TABLE jobs ADD COLUMN kill_requested INTEGER NOT NULL DEFAULT 0`,
		},
	},
	{
		name: "kill delivery bookkeeping",
		stmts: []string{
			// When this job's kill flag was last handed to a worker. 0 means
			// never. TakeKillRequests uses it to stop re-delivering the same
			// flag on every poll: a kill nobody can action (the job's worker
			// never actually received it, so no process exists to signal)
			// otherwise ends every long poll instantly, turning the worker's
			// poll loop into a hot loop for as long as the flag stands.
			`ALTER TABLE jobs ADD COLUMN kill_delivered_at INTEGER NOT NULL DEFAULT 0`,
		},
	},
	{
		name: "device quarantine reason",
		stmts: []string{
			// Why a device is unhealthy, which decides whether a proven host
			// reboot may return it to the pool: a quarantine caused by a
			// vanished process (worker loss, lease expiry, registration
			// reconciliation) is answered by the reboot, a self-reported
			// hardware fault is not. See quarantineReason in reaper.go.
			//
			// Existing unhealthy rows default to '' — cause unknown — and are
			// deliberately NOT revived by a reboot: guessing "probably just a
			// lost worker" about a device quarantined before this column
			// existed is the one direction that can hand out bad hardware.
			`ALTER TABLE devices ADD COLUMN quarantine_reason TEXT NOT NULL DEFAULT ''`,
		},
	},
}

// migrate brings the database up to len(migrations). It runs each pending
// migration in its own transaction so a failure leaves user_version pointing
// at the last one that fully applied.
func migrate(db *sql.DB) error {
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read user_version: %w", err)
	}
	if version > len(migrations) {
		return fmt.Errorf(
			"database is at migration %d but this binary only knows %d — "+
				"it was written by a newer version of rc", version, len(migrations))
	}

	for i := version; i < len(migrations); i++ {
		m := migrations[i]
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		for _, stmt := range m.stmts {
			if _, err := tx.Exec(stmt); err != nil {
				tx.Rollback()
				return fmt.Errorf("migration %d (%s): %w", i+1, m.name, err)
			}
		}
		// PRAGMA does not accept a bound parameter.
		if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, i+1)); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d (%s): set user_version: %w", i+1, m.name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("migration %d (%s): commit: %w", i+1, m.name, err)
		}
	}
	return nil
}
