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
	{
		name: "stage3 labels and usage sheets",
		stmts: []string{
			`CREATE TABLE IF NOT EXISTS device_labels (
			   device_id  TEXT NOT NULL,
			   key        TEXT NOT NULL,
			   value      TEXT NOT NULL,
			   source     TEXT NOT NULL,
			   updated_at INTEGER NOT NULL,
			   PRIMARY KEY (device_id, key, source)
			 )`,
			`CREATE INDEX IF NOT EXISTS device_labels_by_device ON device_labels(device_id)`,
			`CREATE TABLE IF NOT EXISTS host_docs (
			   host       TEXT NOT NULL,
			   device_id  TEXT NOT NULL DEFAULT '',
			   body       TEXT NOT NULL,
			   updated_at INTEGER NOT NULL,
			   PRIMARY KEY (host, device_id)
			 )`,
		},
	},
	{
		name: "stage3 interactive holds",
		stmts: []string{
			`ALTER TABLE leases ADD COLUMN kind TEXT NOT NULL DEFAULT 'job'`,
			`ALTER TABLE leases ADD COLUMN reason TEXT NOT NULL DEFAULT ''`,
		},
	},
	{
		name: "stage3 hold job kind",
		stmts: []string{
			// A hold is a job (kind='hold'), not a second lease mechanism —
			// see task 8's design note. These mirror the columns already
			// added to leases above; the job's own copy is what Enqueue
			// writes at submission and QueuedJobs/ActiveJobs read back, and
			// assignQueued copies it onto the lease row it creates so
			// rc devices and the dashboard can label the holder without a
			// join back to jobs.
			`ALTER TABLE jobs ADD COLUMN kind TEXT NOT NULL DEFAULT 'job'`,
			`ALTER TABLE jobs ADD COLUMN reason TEXT NOT NULL DEFAULT ''`,
		},
	},
	{
		name: "stage4 quarantine detail",
		stmts: []string{
			// quarantine_reason is a CATEGORY — 'fault', 'worker_lost',
			// 'registration', 'lease_expired' — and it is load-bearing:
			// clearQuarantineOnReboot matches on it to decide which
			// quarantines a proven reboot may lift. So the operator-facing
			// text needs a column of its own rather than overwriting it.
			//
			// Without this the text was logged and thrown away: a verify
			// probe would report "72G still pinned on gpubox:gpu0", the
			// controller would write the word "fault", and the one piece of
			// information an operator actually needs to decide whether to
			// clear the device existed only in a log line nobody was
			// reading.
			`ALTER TABLE devices ADD COLUMN quarantine_detail TEXT NOT NULL DEFAULT ''`,
		},
	},
	{
		name: "interactive stdio mode",
		stmts: []string{
			// Where this job's standard streams are wired: '' (the log
			// store, which is every job written before this column existed
			// and every ordinary job since), 'tty' or 'pipe'. See
			// model.StdioLogs for what the three mean.
			//
			// The default is load-bearing rather than incidental: an
			// existing row read back as an attached mode would have the
			// worker sit waiting for a relay half that nobody is ever going
			// to open, so the empty string has to be what a pre-existing job
			// says.
			`ALTER TABLE jobs ADD COLUMN stdio TEXT NOT NULL DEFAULT ''`,
		},
	},
	{
		name: "autonomous recovery flap history",
		stmts: []string{
			// One row per device returned to the pool by AutoRecover (see
			// recover.go), which is the only thing standing between a card
			// that quarantines-clears-quarantines and a controller that
			// silently loops putting it back. The count inside
			// autoRecoveryWindow is the guard; older rows are pruned as the
			// window slides past them, so this table is bounded by
			// devices * autoRecoveryLimit rather than growing forever.
			//
			// reason records what the device was quarantined FOR at the
			// moment it was recovered ('worker_lost', 'registration',
			// 'lease_expired'). The counter does not read it — it exists so
			// an operator debugging a flapping device can see whether it has
			// been the same cause every time.
			//
			// Deliberately NOT a primary key on device_id: a device recovers
			// repeatedly and every occurrence has to be countable. No
			// foreign key to devices either, for the same reason nothing else
			// in this schema has one — a device row that is removed and
			// re-registered under the same id (a rename in worker.yaml and
			// back) must not silently take its flap history with it.
			`CREATE TABLE IF NOT EXISTS device_recoveries (
			   device_id TEXT NOT NULL,
			   at        INTEGER NOT NULL,
			   reason    TEXT NOT NULL DEFAULT ''
			 )`,
			`CREATE INDEX IF NOT EXISTS device_recoveries_by_device
			   ON device_recoveries(device_id, at)`,
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
