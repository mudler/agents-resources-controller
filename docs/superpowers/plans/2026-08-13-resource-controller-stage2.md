# Resource Controller Stage 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make jobs safe to leave alone — a queue so a busy device means "you are next", watchdogs so a hung job cannot hold a GPU forever, reboot-aware recovery, `rc kill`/`rc attach`, and a read-only dashboard.

**Architecture:** The queue sits in front of the existing single-transaction allocation, not around it: jobs enter `queued`, a single-goroutine scheduler loop assigns them, and the transaction that flips `ready → busy` is untouched. Watchdogs run on the worker so a controller outage cannot leave a job past its ceiling. The dashboard is embedded assets reading `/v1/state` plus a new SSE stream.

**Tech Stack:** Go 1.26, `modernc.org/sqlite` (no cgo), `spf13/cobra`, `stretchr/testify`, stdlib `net/http`, `embed`.

**Spec:** `docs/superpowers/specs/2026-08-13-resource-controller-stages-2-4-design.md`

## Global Constraints

- Go 1.26, no cgo. Module `github.com/mudler/agents-resources-controller`.
- SQLite at `MaxOpenConns(1)`. Any query that iterates rows and then issues another query MUST drain its rows to completion first, or it deadlocks.
- **The database already exists in production.** `schema.sql` uses `CREATE TABLE IF NOT EXISTS`, which does nothing to an existing table — every schema change in this stage goes through the versioned migration runner built in Task 1. Never edit an existing `CREATE TABLE` and assume deployed databases will follow.
- Controller-side time goes through the `Clock` interface; controller and store tests never sleep. Worker supervision and e2e tests use real time with `require.Eventually`.
- **The allocation transaction is not to be restructured.** `Allocate` flips `device: ready → busy`, inserts the job, and inserts the lease in one transaction, with the partial unique index `leases_one_live_per_device` as the last-resort guard. The queue feeds it; it does not replace it.
- **No "is this device free?" endpoint.** A caller queues or it does not. Nothing may compose into check-then-act.
- Device states: `ready`, `busy`, `unknown`, `unhealthy`. Job states after this stage: `queued`, `assigned`, `running`, `succeeded`, `failed`, `killed`, `lost`.
- A job requesting more than its device's `max_runtime` ceiling is **rejected at submit**, never silently clamped.
- Ctrl-C on a **queued** job cancels it; Ctrl-C on a **running** job still only detaches. The worker owns a running job's lease.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/store/migrate.go` | Versioned migrations via `PRAGMA user_version`. New. |
| `internal/store/schema.sql` | Base schema for a fresh database (migration 0). |
| `internal/store/queue.go` | `Enqueue`, `ScheduleOnce`, reservations, queue position. New. |
| `internal/store/allocate.go` | Existing allocation; gains `AllocateQueued` used by the scheduler. |
| `internal/store/reaper.go` | Existing sweep; gains lease expiry. |
| `internal/store/store.go` | Existing; `UpsertWorker` gains boot-identity semantics. |
| `internal/server/client_api.go` | Submit now enqueues; adds queue position, kill, attach routes. |
| `internal/server/events.go` | `/v1/events` SSE broadcaster. New. |
| `internal/server/dashboard.go` | Serves the embedded dashboard at `/`. New. |
| `internal/server/dashboard/index.html` | The dashboard itself, embedded. New. |
| `internal/worker/watchdog.go` | Wall-clock and idle-output watchdogs. New. |
| `internal/worker/config.go` | Existing; device entries gain `max_runtime`. |
| `internal/worker/bootid.go` | Reads the host's boot identity. New. |
| `internal/client/client.go` | Existing; adds `Kill`, `Attach`, queue-aware submit. |
| `internal/cli/run.go` | Existing; blocking-with-position, `--timeout`, `--no-wait`, `--priority`. |
| `internal/cli/kill.go` | `rc kill` and `rc attach`. New. |
| `e2e/queue_test.go` | Queue, watchdog, and reboot-recovery end-to-end. New. |

---

### Task 1: Versioned migrations and the Stage 2 schema

The existing store applies `schema.sql` with `CREATE TABLE IF NOT EXISTS`, so a deployed database would silently miss every new column. This task builds the migration runner first, then uses it.

**Files:**
- Create: `internal/store/migrate.go`
- Modify: `internal/store/store.go` (call the runner from `Open`)
- Modify: `internal/model/model.go` (new job state and fields)
- Test: `internal/store/migrate_test.go`

**Interfaces:**
- Consumes: `store.Open`, `model.Job`, `model.JobState` from Stage 1.
- Produces: `store.migrations` (unexported ordered slice); `model.JobQueued JobState = "queued"`; `model.Job` gains `Priority int`, `MaxRuntimeSeconds int`, `IdleTimeoutSeconds int`, `QueuedAt time.Time`; `model.Device` gains `MaxRuntimeSeconds int`; `model.Worker` gains `BootID string`.

- [ ] **Step 1: Write the failing migration tests**

Create `internal/store/migrate_test.go`:

```go
package store_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/clock"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// A database created by Stage 1 must gain Stage 2's columns on open, not
// silently keep the old shape.
func TestOpenMigratesAStageOneDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rc.db")

	// Build a Stage 1 database by hand: the old tables, no user_version.
	raw, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	_, err = raw.Exec(`
		CREATE TABLE workers (id TEXT PRIMARY KEY, host TEXT NOT NULL, last_heartbeat_at INTEGER NOT NULL);
		CREATE TABLE devices (id TEXT PRIMARY KEY, host TEXT NOT NULL, name TEXT NOT NULL,
		  worker_id TEXT NOT NULL, state TEXT NOT NULL, last_heartbeat_at INTEGER NOT NULL);
		CREATE TABLE jobs (id TEXT PRIMARY KEY, selector TEXT NOT NULL DEFAULT '', command TEXT NOT NULL,
		  cwd TEXT NOT NULL DEFAULT '', env TEXT NOT NULL DEFAULT '{}', submitter TEXT NOT NULL DEFAULT '',
		  idempotency_key TEXT, state TEXT NOT NULL, device_id TEXT NOT NULL DEFAULT '',
		  worker_id TEXT NOT NULL DEFAULT '', exit_code INTEGER, kill_reason TEXT NOT NULL DEFAULT '',
		  submitted_at INTEGER NOT NULL, started_at INTEGER, finished_at INTEGER);
		CREATE TABLE leases (id TEXT PRIMARY KEY, device_id TEXT NOT NULL, holder TEXT NOT NULL,
		  job_id TEXT NOT NULL DEFAULT '', acquired_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
		  released_at INTEGER);
		INSERT INTO workers VALUES ('w1', 'gpubox', 100);
	`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))
	s, err := store.Open(path, c)
	require.NoError(t, err)
	defer s.Close()

	check, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer check.Close()

	for _, col := range []struct{ table, column string }{
		{"jobs", "priority"},
		{"jobs", "max_runtime"},
		{"jobs", "idle_timeout"},
		{"jobs", "queued_at"},
		{"devices", "max_runtime"},
		{"workers", "boot_id"},
	} {
		var count int
		require.NoError(t, check.QueryRow(
			`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, col.table, col.column,
		).Scan(&count), "%s.%s", col.table, col.column)
		require.Equal(t, 1, count, "%s.%s missing after migration", col.table, col.column)
	}

	var tables int
	require.NoError(t, check.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='reservations'`).Scan(&tables))
	require.Equal(t, 1, tables, "reservations table missing")

	// Pre-existing rows survive.
	var host string
	require.NoError(t, check.QueryRow(`SELECT host FROM workers WHERE id='w1'`).Scan(&host))
	require.Equal(t, "gpubox", host)
}

func TestMigrationsAreIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rc.db")
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))

	for i := 0; i < 3; i++ {
		s, err := store.Open(path, c)
		require.NoError(t, err, "open #%d", i)
		require.NoError(t, s.Close())
	}

	check, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer check.Close()

	var version int
	require.NoError(t, check.QueryRow(`PRAGMA user_version`).Scan(&version))
	require.Greater(t, version, 0, "user_version should record applied migrations")
}
```

Note: migration internals stay unexported. The tests verify behaviour through
`store.Open` and the database itself, never by reaching into the migration
list.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/store/ -run 'Migrat' -v`
Expected: FAIL — the new columns do not exist.

- [ ] **Step 3: Implement the migration runner**

Create `internal/store/migrate.go`:

```go
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
	name string
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
```

- [ ] **Step 4: Wire the runner into `Open`**

In `internal/store/store.go`, immediately after the existing `db.Exec(schema)` call and before `return &Store{...}`, add:

```go
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
```

A fresh database gets `schema.sql` (which creates the Stage 1 tables) and then every migration, so both paths converge on the same shape.

- [ ] **Step 5: Extend the models**

In `internal/model/model.go`, add the queued state constant alongside the others:

```go
	JobQueued JobState = "queued"
```

Add these fields to `Job`, after `IdempotencyKey`:

```go
	Priority           int       `json:"priority"`
	MaxRuntimeSeconds  int       `json:"max_runtime_seconds,omitempty"`
	IdleTimeoutSeconds int       `json:"idle_timeout_seconds,omitempty"`
	QueuedAt           time.Time `json:"queued_at,omitempty"`
```

Add to `Device`, after `State`:

```go
	MaxRuntimeSeconds int `json:"max_runtime_seconds,omitempty"`
```

Add to `Worker`, after `Host`:

```go
	BootID string `json:"boot_id,omitempty"`
```

`JobQueued` is deliberately NOT terminal — leave `Terminal()` unchanged.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./internal/store/ -race -v`
Expected: the two migration tests PASS and every Stage 1 store test still passes.

- [ ] **Step 7: Verify a real Stage 1 database migrates**

```bash
go build -o /tmp/rc-stage2 .
# The Stage 1 binary's database, if you have one from the running container:
docker compose exec controller sh -c 'ls -la /var/lib/rc'
```

If you have no such database, skip this step and say so in your report — the test in Step 1 covers the same path.

- [ ] **Step 8: Commit**

```bash
git add internal/store/migrate.go internal/store/migrate_test.go internal/store/store.go internal/model/model.go
git commit -m "feat: versioned migrations, and stage 2 schema for queue and watchdogs"
```

---

### Task 2: The queue — enqueue, scheduler, reservations

**Files:**
- Create: `internal/store/queue.go`
- Modify: `internal/store/allocate.go` (add `AllocateQueued`)
- Test: `internal/store/queue_test.go`

**Interfaces:**
- Consumes: everything from Task 1; `store.Allocate`, `store.AllocateRequest`, `store.ErrNoDevice`.
- Produces: `(*Store).Enqueue(req store.EnqueueRequest) (*model.Job, error)`; `store.EnqueueRequest{DeviceID, Command []string, Cwd, Env, Submitter, IdempotencyKey string, Priority int, MaxRuntime, IdleTimeout time.Duration}`; `(*Store).ScheduleOnce() ([]model.Job, error)` returning the jobs assigned in this pass; `(*Store).QueuePosition(jobID string) (int, error)` (1-based; 0 when not queued); `(*Store).CancelQueued(jobID, reason string) (bool, error)`; `(*Store).QueuedJobs() ([]model.Job, error)`.

- [ ] **Step 1: Write the failing queue tests**

Create `internal/store/queue_test.go`:

```go
package store_test

import (
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func enq(submitter string, priority int) store.EnqueueRequest {
	return store.EnqueueRequest{
		DeviceID:  "gpubox:gpu0",
		Command:   []string{"./bench"},
		Submitter: submitter,
		Priority:  priority,
	}
}

func TestEnqueueThenScheduleAssignsOneJob(t *testing.T) {
	s, _ := newStore(t)

	first, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, first.State)

	second, err := s.Enqueue(enq("agent-b", 0))
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, second.State)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, first.ID, assigned[0].ID, "FIFO: the first submitter wins")

	// The device is taken, so a second pass assigns nothing.
	again, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Empty(t, again)

	pos, err := s.QueuePosition(second.ID)
	require.NoError(t, err)
	require.Equal(t, 1, pos, "the waiting job is now at the head")
}

func TestHigherPriorityRunsFirst(t *testing.T) {
	s, _ := newStore(t)

	_, err := s.Enqueue(enq("agent-low", 0))
	require.NoError(t, err)
	high, err := s.Enqueue(enq("agent-high", 5))
	require.NoError(t, err)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, high.ID, assigned[0].ID)
}

func TestReleasingADeviceLetsTheNextQueuedJobRun(t *testing.T) {
	s, _ := newStore(t)

	first, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	second, err := s.Enqueue(enq("agent-b", 0))
	require.NoError(t, err)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Equal(t, first.ID, assigned[0].ID)

	code := 0
	require.NoError(t, s.Release(first.ID, model.JobSucceeded, &code, ""))

	assigned, err = s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, second.ID, assigned[0].ID)
}

// The invariant, now with a queue in front of it.
func TestSchedulingNeverProducesTwoLiveLeases(t *testing.T) {
	s, _ := newStore(t)

	for i := 0; i < 20; i++ {
		_, err := s.Enqueue(enq("agent", 0))
		require.NoError(t, err)
	}
	for i := 0; i < 20; i++ {
		if _, err := s.ScheduleOnce(); err != nil {
			t.Fatalf("schedule pass %d: %v", i, err)
		}
		leases, err := s.Leases()
		require.NoError(t, err)
		require.LessOrEqual(t, len(leases), 1, "pass %d produced %d live leases", i, len(leases))
	}
}

func TestCancelQueuedRemovesItFromTheQueue(t *testing.T) {
	s, _ := newStore(t)

	first, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	second, err := s.Enqueue(enq("agent-b", 0))
	require.NoError(t, err)

	ok, err := s.CancelQueued(first.ID, "user cancelled")
	require.NoError(t, err)
	require.True(t, ok)

	reloaded, err := s.Job(first.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobKilled, reloaded.State)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, second.ID, assigned[0].ID)
}

func TestCancelQueuedRefusesARunningJob(t *testing.T) {
	s, _ := newStore(t)

	job, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	_, err = s.ScheduleOnce()
	require.NoError(t, err)

	ok, err := s.CancelQueued(job.ID, "user cancelled")
	require.NoError(t, err)
	require.False(t, ok, "a job that already holds a device is not cancellable this way")
}

func TestQueuedJobOnUnhealthyDeviceIsNotAssigned(t *testing.T) {
	s, c := newStore(t)

	_, err := s.Enqueue(enq("agent-a", 0))
	require.NoError(t, err)
	require.NoError(t, s.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, c.Now()))

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Empty(t, assigned, "an unhealthy device must not be scheduled onto")
}

func TestIdempotentEnqueueReturnsTheSameJob(t *testing.T) {
	s, _ := newStore(t)

	r := enq("agent-a", 0)
	r.IdempotencyKey = "abc123"

	first, err := s.Enqueue(r)
	require.NoError(t, err)
	second, err := s.Enqueue(r)
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)

	queued, err := s.QueuedJobs()
	require.NoError(t, err)
	require.Len(t, queued, 1)
}

func TestEnqueueRejectsRuntimeAboveTheDeviceCeiling(t *testing.T) {
	s, _ := newStore(t)
	require.NoError(t, s.SetDeviceMaxRuntime("gpubox:gpu0", time.Hour))

	r := enq("agent-a", 0)
	r.MaxRuntime = 4 * time.Hour

	_, err := s.Enqueue(r)
	require.ErrorIs(t, err, store.ErrRuntimeAboveCeiling)
	require.Contains(t, err.Error(), "1h0m0s", "the error must name the ceiling")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/store/ -run 'Queue|Enqueue|Schedul|Cancel|Priority|Releasing' -v`
Expected: FAIL — `s.Enqueue undefined`.

- [ ] **Step 3: Implement the queue**

Create `internal/store/queue.go`:

```go
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

// ErrRuntimeAboveCeiling means the job asked for more wall clock than the
// device tolerates. We reject rather than clamp: a silently shortened budget
// produces a run whose submitter believes it had longer.
var ErrRuntimeAboveCeiling = errors.New("requested runtime exceeds the device ceiling")

type EnqueueRequest struct {
	DeviceID       string
	Command        []string
	Cwd            string
	Env            map[string]string
	Submitter      string
	IdempotencyKey string
	Priority       int
	MaxRuntime     time.Duration
	IdleTimeout    time.Duration
}

// SetDeviceMaxRuntime records the ceiling a host declares for one of its
// devices. Called during registration.
func (s *Store) SetDeviceMaxRuntime(deviceID string, d time.Duration) error {
	_, err := s.db.Exec(`UPDATE devices SET max_runtime = ? WHERE id = ?`,
		int64(d.Seconds()), deviceID)
	return err
}

// Enqueue records a job in state queued. It never assigns: ScheduleOnce does
// that, so there is exactly one place where a device changes hands.
func (s *Store) Enqueue(req EnqueueRequest) (*model.Job, error) {
	if len(req.Command) == 0 {
		return nil, errors.New("command required")
	}
	if req.Submitter == "" {
		return nil, errors.New("submitter required")
	}
	now := s.clock.Now()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

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

	var ceiling int64
	err = tx.QueryRow(`SELECT max_runtime FROM devices WHERE id = ?`, req.DeviceID).Scan(&ceiling)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("unknown device %q", req.DeviceID)
	}
	if err != nil {
		return nil, err
	}
	if ceiling > 0 {
		limit := time.Duration(ceiling) * time.Second
		if req.MaxRuntime == 0 || req.MaxRuntime > limit {
			if req.MaxRuntime > limit {
				return nil, fmt.Errorf("%w: %s allows at most %s",
					ErrRuntimeAboveCeiling, req.DeviceID, limit)
			}
			// No request: inherit the device's ceiling.
			req.MaxRuntime = limit
		}
	}

	cmdJSON, err := json.Marshal(req.Command)
	if err != nil {
		return nil, err
	}
	envJSON, err := json.Marshal(req.Env)
	if err != nil {
		return nil, err
	}

	var key any
	if req.IdempotencyKey != "" {
		key = req.IdempotencyKey
	}

	id := uuid.NewString()
	if _, err := tx.Exec(
		`INSERT INTO jobs (id, selector, command, cwd, env, submitter, idempotency_key,
		                   state, device_id, worker_id, submitted_at, queued_at,
		                   priority, max_runtime, idle_timeout)
		 VALUES (?, '', ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?)`,
		id, string(cmdJSON), req.Cwd, string(envJSON), req.Submitter, key,
		string(model.JobQueued), req.DeviceID, now.Unix(), now.Unix(),
		req.Priority, int64(req.MaxRuntime.Seconds()), int64(req.IdleTimeout.Seconds()),
	); err != nil {
		return nil, fmt.Errorf("insert queued job: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.Job(id)
}

// ScheduleOnce makes one scheduling pass: for each queued job in priority then
// FIFO order, assign it if its device is free, otherwise reserve that device
// so nothing behind it can take it first. Returns the jobs assigned.
func (s *Store) ScheduleOnce() ([]model.Job, error) {
	queued, err := s.QueuedJobs()
	if err != nil {
		return nil, err
	}
	if len(queued) == 0 {
		return nil, s.clearReservations()
	}

	// Reservations are rebuilt from scratch each pass: the head job for a
	// device holds it, and a job that has gone away releases it implicitly.
	if err := s.clearReservations(); err != nil {
		return nil, err
	}

	var assigned []model.Job
	reserved := map[string]string{} // device -> job that holds the reservation

	for _, job := range queued {
		if holder, taken := reserved[job.DeviceID]; taken && holder != job.ID {
			continue // someone ahead of us is waiting for this device
		}

		out, err := s.assignQueued(job.ID, job.DeviceID)
		switch {
		case err == nil:
			assigned = append(assigned, *out)
		case errors.Is(err, ErrNoDevice):
			// Not free: hold it for this job so later jobs cannot jump ahead.
			reserved[job.DeviceID] = job.ID
			if err := s.reserve(job.ID, job.DeviceID); err != nil {
				return nil, err
			}
		default:
			return nil, err
		}
	}
	return assigned, nil
}

// QueuedJobs returns queued jobs in scheduling order: priority DESC, then
// oldest first.
func (s *Store) QueuedJobs() ([]model.Job, error) {
	rows, err := s.db.Query(
		`SELECT id FROM jobs WHERE state = ? ORDER BY priority DESC, queued_at, id`,
		string(model.JobQueued))
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

// QueuePosition is 1-based among jobs waiting for the same device; 0 means the
// job is not queued.
func (s *Store) QueuePosition(jobID string) (int, error) {
	job, err := s.Job(jobID)
	if err != nil {
		return 0, err
	}
	if job.State != model.JobQueued {
		return 0, nil
	}
	var ahead int
	err = s.db.QueryRow(
		`SELECT COUNT(*) FROM jobs
		 WHERE state = ? AND device_id = ?
		   AND (priority > ? OR (priority = ? AND (queued_at < ? OR (queued_at = ? AND id < ?))))`,
		string(model.JobQueued), job.DeviceID,
		job.Priority, job.Priority, job.QueuedAt.Unix(), job.QueuedAt.Unix(), job.ID,
	).Scan(&ahead)
	if err != nil {
		return 0, err
	}
	return ahead + 1, nil
}

// CancelQueued removes a job that has not started. It reports false when the
// job is already assigned or running — that is rc kill's job, not this one,
// because a running job owns a device and a live process.
func (s *Store) CancelQueued(jobID, reason string) (bool, error) {
	now := s.clock.Now()
	res, err := s.db.Exec(
		`UPDATE jobs SET state = ?, kill_reason = ?, finished_at = ?
		 WHERE id = ? AND state = ?`,
		string(model.JobKilled), reason, now.Unix(), jobID, string(model.JobQueued))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	_, err = s.db.Exec(`DELETE FROM reservations WHERE job_id = ?`, jobID)
	return true, err
}

func (s *Store) reserve(jobID, deviceID string) error {
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO reservations (job_id, device_id, created_at) VALUES (?, ?, ?)`,
		jobID, deviceID, s.clock.Now().Unix())
	return err
}

func (s *Store) clearReservations() error {
	_, err := s.db.Exec(`DELETE FROM reservations`)
	return err
}
```

- [ ] **Step 4: Implement `assignQueued` in the allocation file**

The assignment must stay in one transaction with the same guards as `Allocate`. Append to `internal/store/allocate.go`:

```go
// assignQueued moves a queued job onto its device in ONE transaction:
// device ready -> busy, job queued -> assigned, lease inserted. Returns
// ErrNoDevice when the device is not free, leaving the job queued.
func (s *Store) assignQueued(jobID, deviceID string) (*model.Job, error) {
	now := s.clock.Now()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var workerID string
	err = tx.QueryRow(
		`SELECT worker_id FROM devices WHERE id = ? AND state = ?`,
		deviceID, string(model.DeviceReady)).Scan(&workerID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoDevice
	}
	if err != nil {
		return nil, fmt.Errorf("select device: %w", err)
	}

	var submitter string
	var ttl int64
	if err := tx.QueryRow(
		`SELECT submitter, max_runtime FROM jobs WHERE id = ? AND state = ?`,
		jobID, string(model.JobQueued)).Scan(&submitter, &ttl); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoDevice // no longer queued; nothing to do
		}
		return nil, err
	}

	if _, err := tx.Exec(
		`UPDATE devices SET state = ? WHERE id = ? AND state = ?`,
		string(model.DeviceBusy), deviceID, string(model.DeviceReady)); err != nil {
		return nil, fmt.Errorf("mark device busy: %w", err)
	}
	if _, err := tx.Exec(
		`UPDATE jobs SET state = ?, worker_id = ? WHERE id = ?`,
		string(model.JobAssigned), workerID, jobID); err != nil {
		return nil, fmt.Errorf("assign job: %w", err)
	}

	// A job lease outlives its runtime ceiling by a margin so the worker's own
	// watchdog fires first; expiry is the backstop for a worker that vanishes.
	expiry := now.Add(defaultLeaseTTL)
	if ttl > 0 {
		expiry = now.Add(time.Duration(ttl)*time.Second + leaseGraceOverRuntime)
	}
	if _, err := tx.Exec(
		`INSERT INTO leases (id, device_id, holder, job_id, acquired_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		uuid.NewString(), deviceID, submitter, jobID, now.Unix(), expiry.Unix()); err != nil {
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
```

Add these constants near the top of `allocate.go`:

```go
const (
	defaultLeaseTTL       = 5 * time.Minute
	leaseGraceOverRuntime = 10 * time.Minute
)
```

- [ ] **Step 5: Teach `Job` to read the new columns**

In `internal/store/allocate.go`, `Job()` currently selects a fixed column list. Add the four new columns to the `SELECT`, declare `priority, maxRuntime, idleTimeout, queuedAt int64` in the scan block, scan them in the same order, and populate:

```go
	j.Priority = int(priority)
	j.MaxRuntimeSeconds = int(maxRuntime)
	j.IdleTimeoutSeconds = int(idleTimeout)
	if queuedAt > 0 {
		j.QueuedAt = time.Unix(queuedAt, 0).UTC()
	}
```

The full select becomes:

```sql
SELECT id, selector, command, cwd, env, submitter, idempotency_key, state,
       device_id, worker_id, exit_code, kill_reason, submitted_at, started_at, finished_at,
       priority, max_runtime, idle_timeout, queued_at
FROM jobs WHERE id = ?
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./internal/store/ -race -v`
Expected: all queue tests plus every Stage 1 store test PASS.

- [ ] **Step 7: Run the invariant test hard**

Run: `go test ./internal/store/ -run 'TestSchedulingNeverProducesTwoLiveLeases|TestConcurrentAllocate' -race -count=20 -v`
Expected: PASS on every iteration. These two together are the guarantee the project exists for; a flake here stops the task.

- [ ] **Step 8: Commit**

```bash
git add internal/store/queue.go internal/store/queue_test.go internal/store/allocate.go
git commit -m "feat: job queue with priority, FIFO ordering, and device reservations"
```

---

### Task 3: Lease expiry and boot-identity recovery

**Files:**
- Modify: `internal/store/reaper.go` (lease expiry in `Sweep`)
- Modify: `internal/store/store.go` (`UpsertWorker` gains boot identity)
- Test: `internal/store/expiry_test.go`

**Interfaces:**
- Consumes: `Sweep`, `UpsertWorker`, `reapInFlightJobsLocked` from Stage 1; `model.Worker.BootID` from Task 1.
- Produces: `store.SweepResult` gains `LeasesExpired []string`; `UpsertWorker` now interprets `w.BootID`.

- [ ] **Step 1: Write the failing tests**

Create `internal/store/expiry_test.go`:

```go
package store_test

import (
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/stretchr/testify/require"
)

func TestExpiredLeaseIsReleasedAndDeviceQuarantined(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	// Past the lease TTL, with the worker still heartbeating so the
	// worker-loss path is not what fires here.
	c.Advance(20 * time.Minute)
	require.NoError(t, s.RecordHeartbeat("w1", c.Now()))

	res, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []string{job.ID}, res.LeasesExpired)

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)
	require.Contains(t, reloaded.KillReason, "lease expired")

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Empty(t, leases)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State,
		"an expired lease proves nothing about whether the device is occupied")
}

func TestLiveLeaseIsNotExpired(t *testing.T) {
	s, c := newStore(t)

	_, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	c.Advance(time.Minute)
	require.NoError(t, s.RecordHeartbeat("w1", c.Now()))

	res, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Empty(t, res.LeasesExpired)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceBusy, devices[0].State)
}

// A reboot is the one event that proves the device is clean, so its devices
// go back to ready rather than being quarantined.
func TestRebootReturnsDevicesToReady(t *testing.T) {
	s, c := newStore(t)

	// Establish a known prior boot. Without this the stored boot ID is empty,
	// which is NOT proof of a reboot and must quarantine — see
	// TestMissingBootIDQuarantines.
	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-1", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-2", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)
	require.Contains(t, reloaded.KillReason, "host rebooted")

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)
}

// A worker process restart with the same boot ID proves nothing: an orphan
// may still be holding the GPU.
func TestWorkerRestartWithSameBootIDQuarantines(t *testing.T) {
	s, c := newStore(t)

	// newStore registered w1 with an empty boot ID; register once with one so
	// the restart below is a genuine same-boot case.
	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-1", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-1", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)
	require.Contains(t, reloaded.KillReason, "worker re-registered")

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State)
}

// No boot ID at all is not proof of a reboot.
func TestMissingBootIDQuarantines(t *testing.T) {
	s, c := newStore(t)

	_, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"}},
	))

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/store/ -run 'Expir|Reboot|BootID|RestartWithSame' -v`
Expected: FAIL — `res.LeasesExpired` undefined and reboot handling absent.

- [ ] **Step 3: Add lease expiry to the sweep**

In `internal/store/reaper.go`, add to `SweepResult`:

```go
	LeasesExpired []string `json:"leases_expired"`
```

Then, inside `Sweep` after the existing worker-loss handling and before `tx.Commit()`, add:

```go
	// A lease past its expiry is released, but its device is quarantined
	// rather than freed: expiry tells us the holder stopped renewing, not
	// that the hardware is idle.
	expRows, err := tx.Query(
		`SELECT job_id, device_id FROM leases
		 WHERE released_at IS NULL AND expires_at <= ?`, now.Unix())
	if err != nil {
		return res, err
	}
	type expired struct{ jobID, deviceID string }
	var stale []expired
	for expRows.Next() {
		var e expired
		if err := expRows.Scan(&e.jobID, &e.deviceID); err != nil {
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
		if _, err := tx.Exec(
			`UPDATE leases SET released_at = ? WHERE job_id = ? AND released_at IS NULL`,
			now.Unix(), e.jobID); err != nil {
			return res, err
		}
		if _, err := tx.Exec(
			`UPDATE devices SET state = ? WHERE id = ?`,
			string(model.DeviceUnhealthy), e.deviceID); err != nil {
			return res, err
		}
		if e.jobID != "" {
			if _, err := tx.Exec(
				`UPDATE jobs SET state = ?, kill_reason = ?, finished_at = ?
				 WHERE id = ? AND state IN (?, ?)`,
				string(model.JobLost), "lease expired", now.Unix(), e.jobID,
				string(model.JobAssigned), string(model.JobRunning)); err != nil {
				return res, err
			}
			res.LeasesExpired = append(res.LeasesExpired, e.jobID)
		}
	}
```

- [ ] **Step 4: Teach `UpsertWorker` about boot identity**

In `internal/store/store.go`, `UpsertWorker` currently reaps in-flight jobs and quarantines their devices unconditionally. Change it to decide by boot ID.

Before the reap call, read the stored boot ID inside the same transaction:

```go
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
```

Pass `reason` through to `reapInFlightJobsLocked`, and give that function a
final parameter controlling the device outcome. Its device update becomes:

```go
	quarantineState := model.DeviceUnhealthy
	if rebooted {
		quarantineState = model.DeviceReady
	}
```

with the existing `UPDATE devices SET state = ?` using `quarantineState`.

Finally, persist the new boot ID in the worker upsert by adding `boot_id` to
its column list and `excluded.boot_id` to the `DO UPDATE SET` clause.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/store/ -race -v`
Expected: every test in the package PASSES, including the Stage 1 reconciliation tests — the unchanged-boot-ID path must still quarantine.

- [ ] **Step 6: Commit**

```bash
git add internal/store/reaper.go internal/store/store.go internal/store/expiry_test.go
git commit -m "feat: lease expiry, and boot identity distinguishing reboot from crash"
```

---

### Task 4: Controller — enqueue, scheduler loop, queue position, kill

**Files:**
- Modify: `internal/server/client_api.go`
- Modify: `internal/server/server.go` (routes)
- Modify: `internal/cli/serve.go` (scheduler goroutine)
- Test: `internal/server/queue_api_test.go`

**Interfaces:**
- Consumes: `store.Enqueue`, `ScheduleOnce`, `QueuePosition`, `CancelQueued`, `QueuedJobs`, `ErrRuntimeAboveCeiling`.
- Produces: `server.SubmitRequest` gains `Priority int`, `MaxRuntimeSeconds int`, `IdleTimeoutSeconds int`, `NoWait bool`; `server.JobView{Job model.Job, QueuePosition int}`; routes `POST /v1/jobs/{id}/kill` and `GET /v1/jobs/{id}?` returning `JobView`; `StateResponse` gains `Queued []model.Job`.

- [ ] **Step 1: Write the failing API tests**

Create `internal/server/queue_api_test.go`:

```go
package server_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/server"
	"github.com/stretchr/testify/require"
)

func TestSubmitQueuesWhenDeviceIsBusy(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	defer first.Body.Close()
	require.Equal(t, http.StatusCreated, first.StatusCode)

	// Give the first job the device.
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	second := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	defer second.Body.Close()
	require.Equal(t, http.StatusCreated, second.StatusCode, "a busy device queues, it does not 409")

	var job model.Job
	require.NoError(t, json.NewDecoder(second.Body).Decode(&job))
	require.Equal(t, model.JobQueued, job.State)
}

func TestSubmitWithNoWaitStillFailsFast(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	first.Body.Close()
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	second := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b", NoWait: true,
	})
	defer second.Body.Close()
	require.Equal(t, http.StatusConflict, second.StatusCode)
}

func TestJobViewReportsQueuePosition(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	a.Body.Close()
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	b := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	var queued model.Job
	require.NoError(t, json.NewDecoder(b.Body).Decode(&queued))
	b.Body.Close()

	view := get(t, ts, "ctok", "/v1/jobs/"+queued.ID)
	defer view.Body.Close()
	var out server.JobView
	require.NoError(t, json.NewDecoder(view.Body).Decode(&out))
	require.Equal(t, 1, out.QueuePosition)
	require.Equal(t, model.JobQueued, out.Job.State)
}

func TestSubmitRejectsRuntimeAboveDeviceCeiling(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)
	require.NoError(t, st.SetDeviceMaxRuntime("gpubox:gpu0", time.Hour))

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
		MaxRuntimeSeconds: 4 * 3600,
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "runtime_above_ceiling", body["error"])
}

func TestKillCancelsAQueuedJob(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	a.Body.Close()
	_, err := st.ScheduleOnce()
	require.NoError(t, err)

	b := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./b"}, Submitter: "agent-b",
	})
	var queued model.Job
	require.NoError(t, json.NewDecoder(b.Body).Decode(&queued))
	b.Body.Close()

	kill := post(t, ts, "ctok", "/v1/jobs/"+queued.ID+"/kill", map[string]string{"submitter": "agent-b"})
	defer kill.Body.Close()
	require.Equal(t, http.StatusOK, kill.StatusCode)

	reloaded, err := st.Job(queued.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobKilled, reloaded.State)
}

func TestKillRefusesSomeoneElsesJob(t *testing.T) {
	ts, st, _, _ := newServer(t)
	registerWorker(t, ts)

	a := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./a"}, Submitter: "agent-a",
	})
	var job model.Job
	require.NoError(t, json.NewDecoder(a.Body).Decode(&job))
	a.Body.Close()

	kill := post(t, ts, "ctok", "/v1/jobs/"+job.ID+"/kill", map[string]string{"submitter": "agent-b"})
	defer kill.Body.Close()
	require.Equal(t, http.StatusForbidden, kill.StatusCode)

	reloaded, err := st.Job(job.ID)
	require.NoError(t, err)
	require.NotEqual(t, model.JobKilled, reloaded.State)
	_ = st
}
```

Add a `get` helper alongside the existing `post` helper in
`internal/server/worker_api_test.go`:

```go
func get(t *testing.T, ts *httptest.Server, token, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}
```

`newServer` must also return the store so these tests can drive
`ScheduleOnce` directly; it already does.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/server/ -run 'Queue|NoWait|Kill|Ceiling' -v`
Expected: FAIL — `SubmitRequest.NoWait` undefined.

- [ ] **Step 3: Update the submit handler**

In `internal/server/client_api.go`, extend `SubmitRequest`:

```go
	Priority           int  `json:"priority,omitempty"`
	MaxRuntimeSeconds  int  `json:"max_runtime_seconds,omitempty"`
	IdleTimeoutSeconds int  `json:"idle_timeout_seconds,omitempty"`
	NoWait             bool `json:"no_wait,omitempty"`
```

Add the job view type:

```go
// JobView is a job plus the queue position a client needs to show progress.
type JobView struct {
	Job           model.Job `json:"job"`
	QueuePosition int       `json:"queue_position,omitempty"`
}
```

Replace `handleSubmit`'s allocation call. It now enqueues, then makes one
scheduling pass so a free device starts immediately rather than waiting for
the loop's next tick:

```go
	job, err := s.cfg.Store.Enqueue(store.EnqueueRequest{
		DeviceID: req.DeviceID, Command: req.Command, Cwd: req.Cwd, Env: req.Env,
		Submitter: req.Submitter, IdempotencyKey: req.IdempotencyKey,
		Priority:    req.Priority,
		MaxRuntime:  time.Duration(req.MaxRuntimeSeconds) * time.Second,
		IdleTimeout: time.Duration(req.IdleTimeoutSeconds) * time.Second,
	})
	if errors.Is(err, store.ErrRuntimeAboveCeiling) {
		writeErr(w, http.StatusBadRequest, "runtime_above_ceiling", err.Error())
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", "could not queue job")
		return
	}

	assigned, err := s.cfg.Store.ScheduleOnce()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", "could not schedule")
		return
	}
	for _, a := range assigned {
		s.notify.poke(a.WorkerID)
	}

	current, err := s.cfg.Store.Job(job.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", "could not read job")
		return
	}

	// --no-wait keeps stage 1's behaviour: never sit in a queue.
	if req.NoWait && current.State == model.JobQueued {
		if _, err := s.cfg.Store.CancelQueued(current.ID, "no-wait: device busy"); err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", "could not cancel")
			return
		}
		writeErr(w, http.StatusConflict, "no_device_available",
			fmt.Sprintf("%s is not free", req.DeviceID))
		return
	}

	writeJSON(w, http.StatusCreated, current)
```

- [ ] **Step 4: Add the job view and kill handlers**

Replace `handleGetJob`'s body so it returns a `JobView`:

```go
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.cfg.Store.Job(r.PathValue("id"))
	if err != nil {
		s.writeJobLookupError(w, err)
		return
	}
	pos, err := s.cfg.Store.QueuePosition(job.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", "could not read queue position")
		return
	}
	writeJSON(w, http.StatusOK, JobView{Job: *job, QueuePosition: pos})
}
```

Add the kill handler:

```go
type KillRequest struct {
	Submitter string `json:"submitter"`
}

// handleKill cancels a queued job outright, or asks the worker to terminate a
// running one by expiring nothing and letting the worker's poll observe the
// kill flag. Ownership is checked against the submitter unless the caller
// holds an admin token.
func (s *Server) handleKill(w http.ResponseWriter, r *http.Request) {
	var req KillRequest
	if !decode(w, r, &req) {
		return
	}
	jobID := r.PathValue("id")

	job, err := s.cfg.Store.Job(jobID)
	if err != nil {
		s.writeJobLookupError(w, err)
		return
	}
	if !isAdmin(r) && job.Submitter != req.Submitter {
		writeErr(w, http.StatusForbidden, "not_job_owner",
			"only the submitter or an admin may kill this job")
		return
	}

	switch job.State {
	case model.JobQueued:
		ok, err := s.cfg.Store.CancelQueued(jobID, "killed by "+req.Submitter)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", "could not cancel")
			return
		}
		if !ok {
			writeErr(w, http.StatusConflict, "not_cancellable", "job already started")
			return
		}
	case model.JobAssigned, model.JobRunning:
		if err := s.cfg.Store.RequestKill(jobID); err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", "could not request kill")
			return
		}
		s.notify.poke(job.WorkerID)
	default:
		writeErr(w, http.StatusConflict, "not_cancellable", "job already finished")
		return
	}
	w.WriteHeader(http.StatusOK)
}
```

`isAdmin` reads the role resolved by the auth middleware. Store the role on
the request context in `require` (the middleware already resolves it) and read
it here:

```go
func isAdmin(r *http.Request) bool {
	role, _ := r.Context().Value(roleContextKey).(string)
	return role == "admin"
}
```

Add `roleContextKey` as an unexported context key type in `server.go`, and in
`require` wrap the request: `r = r.WithContext(context.WithValue(r.Context(), roleContextKey, got))`.

- [ ] **Step 5: Add `RequestKill` to the store**

Append to `internal/store/queue.go`:

```go
// RequestKill flags a running job for termination. The worker sees the flag on
// its next poll and terminates the process group; the terminal report then
// arrives through the normal path, so there is exactly one place where a job
// ends.
func (s *Store) RequestKill(jobID string) error {
	_, err := s.db.Exec(
		`UPDATE jobs SET kill_requested = 1 WHERE id = ? AND state IN (?, ?)`,
		jobID, string(model.JobAssigned), string(model.JobRunning))
	return err
}

// KillRequestedFor lists jobs on a worker that have been flagged for kill.
func (s *Store) KillRequestedFor(workerID string) ([]string, error) {
	rows, err := s.db.Query(
		`SELECT id FROM jobs WHERE worker_id = ? AND kill_requested = 1 AND state IN (?, ?)`,
		workerID, string(model.JobAssigned), string(model.JobRunning))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
```

Add the column in a **new** migration appended to `migrations` in
`internal/store/migrate.go` (never edit the applied one):

```go
	{
		name: "kill requests",
		stmts: []string{
			`ALTER TABLE jobs ADD COLUMN kill_requested INTEGER NOT NULL DEFAULT 0`,
		},
	},
```

- [ ] **Step 6: Register the routes and expose the queue in state**

In `internal/server/server.go`'s `Handler()`, add:

```go
	mux.Handle("POST /v1/jobs/{id}/kill", s.require("client", s.handleKill))
```

In `handleState`, add the queue to the response. Extend `StateResponse`:

```go
	Queued []model.Job `json:"queued"`
```

and populate it with `s.cfg.Store.QueuedJobs()`.

- [ ] **Step 7: Run the scheduler loop in `rc serve`**

In `internal/cli/serve.go`, alongside the existing reaper goroutine, add a
scheduler goroutine on the same cancellable context:

```go
	schedulerWG.Add(1)
	go func() {
		defer schedulerWG.Done()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-reaperCtx.Done():
				return
			case <-t.C:
				assigned, err := st.ScheduleOnce()
				if err != nil {
					slog.Error("schedule", "err", err)
					continue
				}
				for _, job := range assigned {
					srv.Poke(job.WorkerID)
				}
			}
		}
	}()
```

`srv.Poke` is a new exported method on `*server.Server` wrapping
`s.notify.poke`, so the CLI can wake a worker without reaching into
unexported state:

```go
// Poke wakes a worker's assignment long-poll immediately.
func (s *Server) Poke(workerID string) { s.notify.poke(workerID) }
```

Join `schedulerWG` before the deferred `st.Close()`, exactly as the reaper's
WaitGroup is joined, and cancel its context before the wait.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `go test ./... -race`
Expected: all packages PASS. The Stage 1 test asserting a busy device returns
409 will now fail — it is superseded by `TestSubmitQueuesWhenDeviceIsBusy`.
Update it to submit with `NoWait: true`, which is the behaviour it was
actually testing, and note the change in your report.

- [ ] **Step 9: Commit**

```bash
git add internal/server/ internal/store/queue.go internal/store/migrate.go internal/cli/serve.go
git commit -m "feat: submit enqueues, scheduler loop, queue position, and rc kill"
```

---

### Task 5: Worker — boot identity, device ceilings, watchdogs, kill handling

**Files:**
- Create: `internal/worker/bootid.go`, `internal/worker/watchdog.go`
- Modify: `internal/worker/config.go` (device entries with ceilings)
- Modify: `internal/worker/worker.go` (register with boot ID and ceilings; poll envelope; apply watchdogs)
- Modify: `internal/server/worker_api.go` (register accepts ceilings; poll returns an envelope)
- Test: `internal/worker/bootid_test.go`, `internal/worker/watchdog_test.go`, `internal/worker/config_test.go`

**Interfaces:**
- Consumes: `worker.Run`, `worker.JobSpec`, `worker.Result` from Stage 1 (unchanged); `store.KillRequestedFor` from Task 4.
- Produces: `worker.BootID() string`; `worker.DeviceConfig{Name string, MaxRuntime time.Duration}`; `worker.Config.Devices []DeviceConfig`; `worker.watchdogWriter` wrapping a sink to track last-output time; `server.RegisterRequest.Devices []server.DeviceSpec{Name string, MaxRuntimeSeconds int}` and `RegisterRequest.BootID string`; `server.PollResponse{Assignments []Assignment, Kills []string}`.

- [ ] **Step 1: Write the failing boot-identity and config tests**

Create `internal/worker/bootid_test.go`:

```go
package worker_test

import (
	"testing"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

// The boot ID must be stable within a boot: two reads return the same value.
// It is allowed to be empty on a platform that cannot provide one, which the
// controller treats as "no proof of a reboot".
func TestBootIDIsStableWithinABoot(t *testing.T) {
	first := worker.BootID()
	second := worker.BootID()
	require.Equal(t, first, second)
	if first != "" {
		require.NotContains(t, first, "\n", "boot id must be trimmed")
	}
}
```

Create `internal/worker/config_test.go`:

```go
package worker_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "worker.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

// Stage 1 configs are plain strings and must keep working.
func TestLoadConfigAcceptsPlainDeviceNames(t *testing.T) {
	cfg, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - gpu0
  - gpu1
`))
	require.NoError(t, err)
	require.Len(t, cfg.Devices, 2)
	require.Equal(t, "gpu0", cfg.Devices[0].Name)
	require.Zero(t, cfg.Devices[0].MaxRuntime, "no ceiling declared")
}

func TestLoadConfigAcceptsDeviceObjectsWithCeilings(t *testing.T) {
	cfg, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - name: gpu0
    max_runtime: 4h
  - name: gpu1
`))
	require.NoError(t, err)
	require.Len(t, cfg.Devices, 2)
	require.Equal(t, "gpu0", cfg.Devices[0].Name)
	require.Equal(t, 4*time.Hour, cfg.Devices[0].MaxRuntime)
	require.Equal(t, "gpu1", cfg.Devices[1].Name)
	require.Zero(t, cfg.Devices[1].MaxRuntime)
}

func TestLoadConfigRejectsADeviceWithNoName(t *testing.T) {
	_, err := worker.LoadConfig(writeConfig(t, `
controller_url: http://c:8080
token: t
host: box
devices:
  - max_runtime: 4h
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "name")
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/worker/ -run 'BootID|LoadConfig' -v`
Expected: FAIL — `worker.BootID` undefined and `cfg.Devices[0].Name` does not compile.

- [ ] **Step 3: Implement boot identity**

Create `internal/worker/bootid.go`:

```go
package worker

import (
	"bufio"
	"os"
	"strings"
	"sync"
)

var (
	bootIDOnce sync.Once
	bootIDVal  string
)

// BootID returns a value that changes when the machine reboots and is stable
// while it stays up. The controller uses a change as proof that nothing from
// before can still be holding a device; an empty value is treated as no
// proof, which quarantines rather than frees.
func BootID() string {
	bootIDOnce.Do(func() {
		if b, err := os.ReadFile("/proc/sys/kernel/random/boot_id"); err == nil {
			bootIDVal = strings.TrimSpace(string(b))
			return
		}
		// Fallback: the kernel's boot timestamp from /proc/stat.
		f, err := os.Open("/proc/stat")
		if err != nil {
			return
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if rest, ok := strings.CutPrefix(sc.Text(), "btime "); ok {
				bootIDVal = "btime-" + strings.TrimSpace(rest)
				return
			}
		}
	})
	return bootIDVal
}
```

- [ ] **Step 4: Accept both device config shapes**

In `internal/worker/config.go`, add the device type and custom unmarshalling:

```go
// DeviceConfig is one device this host offers. It accepts either a bare name
// (stage 1 style) or an object with a runtime ceiling.
type DeviceConfig struct {
	Name       string        `yaml:"name"`
	MaxRuntime time.Duration `yaml:"max_runtime"`
}

func (d *DeviceConfig) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		return value.Decode(&d.Name)
	}
	type plain DeviceConfig // avoid recursing into this method
	var p plain
	if err := value.Decode(&p); err != nil {
		return err
	}
	*d = DeviceConfig(p)
	if d.Name == "" {
		return fmt.Errorf("device entry needs a name")
	}
	return nil
}
```

Change `Config.Devices` from `[]string` to `[]DeviceConfig`, and update the
existing validation (`len(c.Devices) == 0`) to also reject an entry whose
`Name` is empty.

- [ ] **Step 5: Write the failing watchdog tests**

Create `internal/worker/watchdog_test.go`:

```go
package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestMaxRuntimeWatchdogKillsALongJob(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command:      []string{"sh", "-c", "sleep 60"},
		MaxRuntime:   500 * time.Millisecond,
		GraceCeiling: 500 * time.Millisecond,
	}, &out)

	require.True(t, res.Killed)
	require.Contains(t, res.Reason, "max_runtime")
}

func TestIdleWatchdogKillsASilentJob(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command:      []string{"sh", "-c", "echo starting; sleep 60"},
		IdleTimeout:  700 * time.Millisecond,
		GraceCeiling: 500 * time.Millisecond,
	}, &out)

	require.True(t, res.Killed)
	require.Contains(t, res.Reason, "idle")
	require.Contains(t, out.String(), "starting")
}

// A job that keeps producing output must never trip the idle watchdog.
func TestChattyJobIsNotTrippedByIdleWatchdog(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command:      []string{"sh", "-c", "for i in 1 2 3 4 5 6; do echo tick; sleep 0.2; done"},
		IdleTimeout:  700 * time.Millisecond,
		GraceCeiling: 500 * time.Millisecond,
	}, &out)

	require.False(t, res.Killed, "a job producing output steadily must not be killed")
	require.Equal(t, 0, res.ExitCode)
}

func TestNoWatchdogsMeansNoLimit(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command: []string{"sh", "-c", "sleep 0.3; echo done"},
	}, &out)

	require.False(t, res.Killed)
	require.Equal(t, 0, res.ExitCode)
	require.Contains(t, out.String(), "done")
}
```

- [ ] **Step 6: Implement the watchdogs**

Create `internal/worker/watchdog.go`:

```go
package worker

import (
	"io"
	"sync"
	"time"
)

// watchdogWriter wraps a sink and records when output last flowed, so the
// idle watchdog measures real progress rather than wall clock.
type watchdogWriter struct {
	sink io.Writer

	mu   sync.Mutex
	last time.Time
}

func newWatchdogWriter(sink io.Writer, now time.Time) *watchdogWriter {
	return &watchdogWriter{sink: sink, last: now}
}

func (w *watchdogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.last = time.Now()
	w.mu.Unlock()
	return w.sink.Write(p)
}

func (w *watchdogWriter) idleFor() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return time.Since(w.last)
}
```

In `internal/worker/exec.go`, add to `JobSpec`:

```go
	MaxRuntime  time.Duration
	IdleTimeout time.Duration
```

In `Run`, wrap the sink and start a watchdog goroutine that cancels an
internal context when either limit trips. The cancellation path already
handles SIGTERM → grace → SIGKILL and group survivors, so the watchdog reuses
it rather than duplicating signal handling:

```go
	wd := newWatchdogWriter(sink, time.Now())
	cmd.Stdout = wd
	cmd.Stderr = wd

	runCtx, tripped := context.WithCancel(ctx)
	defer tripped()

	var (
		tripMu     sync.Mutex
		tripReason string
	)
	if spec.MaxRuntime > 0 || spec.IdleTimeout > 0 {
		go func() {
			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()
			deadline := time.Now().Add(spec.MaxRuntime)
			for {
				select {
				case <-runCtx.Done():
					return
				case <-t.C:
					var reason string
					switch {
					case spec.MaxRuntime > 0 && time.Now().After(deadline):
						reason = "max_runtime exceeded (" + spec.MaxRuntime.String() + ")"
					case spec.IdleTimeout > 0 && wd.idleFor() > spec.IdleTimeout:
						reason = "idle: no output for " + spec.IdleTimeout.String()
					default:
						continue
					}
					tripMu.Lock()
					tripReason = reason
					tripMu.Unlock()
					tripped()
					return
				}
			}
		}()
	}
```

Use `runCtx` wherever `ctx` currently drives cancellation inside `Run`. When
the run ends, a watchdog reason takes precedence over the generic
`"cancelled"`:

```go
	tripMu.Lock()
	if tripReason != "" {
		res.Killed = true
		res.Reason = tripReason
	}
	tripMu.Unlock()
```

Place that after the existing result assembly, so a watchdog trip is labelled
by which watchdog fired rather than as a plain cancellation.

- [ ] **Step 7: Send boot ID and ceilings at registration; handle the poll envelope**

In `internal/server/worker_api.go`, change the register request:

```go
type DeviceSpec struct {
	Name              string `json:"name"`
	MaxRuntimeSeconds int    `json:"max_runtime_seconds,omitempty"`
}

type RegisterRequest struct {
	Host    string       `json:"host"`
	BootID  string       `json:"boot_id,omitempty"`
	Devices []DeviceSpec `json:"devices"`
}
```

`handleRegister` builds `model.Device` values as before, passes
`model.Worker{..., BootID: req.BootID}`, and calls `SetDeviceMaxRuntime` for
each device declaring one.

Change the assignments response to an envelope carrying kill requests, so a
kill reaches the worker as fast as an assignment does:

```go
type PollResponse struct {
	Assignments []Assignment `json:"assignments"`
	Kills       []string     `json:"kills,omitempty"`
}
```

`handleAssignments` returns `200` with a `PollResponse` when there are
assignments **or** kills, and `204` only when both are empty. This is a wire
change from Stage 1's bare array; both sides ship together in this stage.

In `internal/worker/worker.go`: send `BootID()` and the device specs at
registration; decode `PollResponse`; and for each job ID in `Kills`, cancel
that job's context through the `running` map so `Run` terminates the process
group. The terminal report then flows through the existing path.

Pass the job's watchdog settings from the assignment into the `JobSpec`. Add
to `Assignment`:

```go
	MaxRuntimeSeconds  int `json:"max_runtime_seconds,omitempty"`
	IdleTimeoutSeconds int `json:"idle_timeout_seconds,omitempty"`
```

and populate them from the job record in `handleAssignments`.

- [ ] **Step 8: Run everything**

Run: `go test ./... -race -count=2`
Expected: all packages PASS, twice. The watchdog tests are timing-sensitive;
if any is flaky, widen its margins rather than deleting the assertion, and say
so in your report.

- [ ] **Step 9: Commit**

```bash
git add internal/worker/ internal/server/worker_api.go
git commit -m "feat: worker reports boot identity, enforces watchdogs, honours kill requests"
```

---

### Task 6: Client and CLI — blocking run with position, kill, attach

**Files:**
- Modify: `internal/client/client.go`
- Modify: `internal/cli/run.go`
- Create: `internal/cli/kill.go`
- Modify: `main.go` (register new commands)
- Test: `internal/client/queue_client_test.go`, `internal/cli/run_queue_test.go`

**Interfaces:**
- Consumes: `server.JobView`, `server.SubmitRequest` (with `Priority`, `MaxRuntimeSeconds`, `IdleTimeoutSeconds`, `NoWait`), `POST /v1/jobs/{id}/kill`.
- Produces: `client.SubmitOptions` gains `Priority int`, `MaxRuntime, IdleTimeout time.Duration`, `NoWait bool`; `(*Client).JobView(ctx, id) (*server.JobView, error)`; `(*Client).Kill(ctx, id, submitter string) error`; `(*Client).WaitScheduled(ctx, id string, onPosition func(int)) (*model.Job, error)`; `cli.NewKillCmd()`, `cli.NewAttachCmd()`.

- [ ] **Step 1: Write the failing client tests**

Create `internal/client/queue_client_test.go`:

```go
package client_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/stretchr/testify/require"
)

// WaitScheduled reports each new queue position, then returns once the job
// leaves the queue.
func TestWaitScheduledReportsPositionsThenReturns(t *testing.T) {
	var polls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := polls.Add(1)
		state, pos := "queued", 3
		switch {
		case n >= 3:
			state, pos = "assigned", 0
		case n == 2:
			pos = 1
		}
		json.NewEncoder(w).Encode(map[string]any{
			"job":            map[string]any{"id": "job1", "state": state},
			"queue_position": pos,
		})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	var seen []int
	job, err := c.WaitScheduled(context.Background(), "job1", func(p int) { seen = append(seen, p) })
	require.NoError(t, err)
	require.Equal(t, "assigned", string(job.State))
	require.Equal(t, []int{3, 1}, seen, "each distinct position is reported once")
}

func TestKillSendsSubmitterAndSurfacesForbidden(t *testing.T) {
	var gotBody map[string]string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "not_job_owner", "message": "only the submitter may kill this job",
		})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	err := c.Kill(context.Background(), "job1", "agent-a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not_job_owner")
	require.Equal(t, "agent-a", gotBody["submitter"])
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/client/ -run 'WaitScheduled|Kill' -v`
Expected: FAIL — `c.WaitScheduled` undefined.

- [ ] **Step 3: Implement the client additions**

Append to `internal/client/client.go`:

```go
// JobView fetches a job together with its queue position.
func (c *Client) JobView(ctx context.Context, id string) (*server.JobView, error) {
	resp, err := c.do(ctx, http.MethodGet, "/v1/jobs/"+id, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError(resp)
	}
	var view server.JobView
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		return nil, err
	}
	return &view, nil
}

// WaitScheduled polls until the job leaves the queue, calling onPosition each
// time its position changes so the caller can show progress.
func (c *Client) WaitScheduled(ctx context.Context, id string, onPosition func(int)) (*model.Job, error) {
	last := -1
	for {
		view, err := c.JobView(ctx, id)
		if err != nil {
			return nil, err
		}
		if view.Job.State != model.JobQueued {
			return &view.Job, nil
		}
		if view.QueuePosition != last {
			last = view.QueuePosition
			if onPosition != nil {
				onPosition(view.QueuePosition)
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Kill cancels a queued job or terminates a running one.
func (c *Client) Kill(ctx context.Context, id, submitter string) error {
	payload, err := json.Marshal(map[string]string{"submitter": submitter})
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, http.MethodPost, "/v1/jobs/"+id+"/kill", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return apiError(resp)
	}
	return nil
}
```

Extend `SubmitOptions` with `Priority int`, `MaxRuntime time.Duration`,
`IdleTimeout time.Duration`, `NoWait bool`, and pass them through in `Submit`.

- [ ] **Step 4: Make `rc run` queue-aware**

In `internal/cli/run.go`, add flags `--priority`, `--max-runtime`,
`--idle-timeout`, `--timeout`, `--no-wait`, then insert the wait between
submit and streaming:

```go
			if job.State == model.JobQueued {
				waitCtx := ctx
				if timeout > 0 {
					var cancelWait context.CancelFunc
					waitCtx, cancelWait = context.WithTimeout(ctx, timeout)
					defer cancelWait()
				}

				scheduled, err := c.WaitScheduled(waitCtx, job.ID, func(pos int) {
					fmt.Fprintf(os.Stderr, "rc: queued at position %d for %s\n", pos, device)
				})
				switch {
				case err == nil:
					job = scheduled
				case ctx.Err() != nil:
					// Ctrl-C while queued cancels outright: nothing is running,
					// so there is no lease to protect.
					if killErr := c.Kill(context.Background(), job.ID, as); killErr != nil {
						fmt.Fprintf(os.Stderr, "rc: could not cancel queued job %s: %v\n", job.ID, killErr)
					} else {
						fmt.Fprintf(os.Stderr, "rc: cancelled queued job %s\n", job.ID)
					}
					return exitCodeError{code: 130}
				default:
					if killErr := c.Kill(context.Background(), job.ID, as); killErr != nil {
						fmt.Fprintf(os.Stderr, "rc: could not cancel queued job %s: %v\n", job.ID, killErr)
					}
					return fmt.Errorf("gave up waiting for %s: %w", device, err)
				}
			}
```

- [ ] **Step 5: Add `rc kill` and `rc attach`**

Create `internal/cli/kill.go`:

```go
package cli

import (
	"fmt"
	"os"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/spf13/cobra"
)

func NewKillCmd() *cobra.Command {
	var as string
	cmd := &cobra.Command{
		Use:   "kill <job-id>",
		Short: "Cancel a queued job or terminate a running one",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			submitter := as
			if submitter == "" {
				submitter = defaultSubmitter()
			}
			c := client.New(controllerURL(), controllerToken())
			if err := c.Kill(cmd.Context(), args[0], submitter); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "rc: kill requested for %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().StringVar(&as, "as", "", "identity to authorise the kill (defaults to user@host/session)")
	return cmd
}

func NewAttachCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "attach <job-id>",
		Short: "Re-stream a running job's output from the beginning",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.New(controllerURL(), controllerToken())
			return c.StreamLogs(cmd.Context(), args[0], os.Stdout)
		},
	}
}
```

Register both in `main.go`'s `root.AddCommand(...)` call.

- [ ] **Step 6: Run everything and check the CLI by hand**

```bash
go test ./... -race
go build -o /tmp/rc . && /tmp/rc kill --help && /tmp/rc attach --help
```

Expected: tests PASS, both help screens render.

- [ ] **Step 7: Commit**

```bash
git add internal/client/ internal/cli/ main.go
git commit -m "feat: rc run blocks with queue position; rc kill and rc attach"
```

---

### Task 7: The event stream

**Files:**
- Create: `internal/server/events.go`
- Modify: `internal/server/server.go` (route), `internal/server/client_api.go` and `internal/store` callers to publish
- Test: `internal/server/events_test.go`

**Interfaces:**
- Consumes: the server core.
- Produces: `server.Event{Kind string, At time.Time, Payload any}`; `(*Server).Publish(kind string, payload any)`; route `GET /v1/events` (SSE).

- [ ] **Step 1: Write the failing test**

Create `internal/server/events_test.go`:

```go
package server_test

import (
	"bufio"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventStreamDeliversPublishedEvents(t *testing.T) {
	ts, _, _, _ := newServer(t)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/events", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")

	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/event-stream", strings.Split(resp.Header.Get("Content-Type"), ";")[0])

	// Registering a worker publishes a state change.
	go func() {
		time.Sleep(100 * time.Millisecond)
		r := post(t, ts, "wtok", "/v1/workers/register", map[string]any{
			"host": "gpubox", "devices": []map[string]any{{"name": "gpu0"}},
		})
		r.Body.Close()
	}()

	sc := bufio.NewScanner(resp.Body)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !sc.Scan() {
			break
		}
		if strings.HasPrefix(sc.Text(), "data:") {
			require.Contains(t, sc.Text(), "devices")
			return
		}
	}
	t.Fatal("no event received within 5s")
}

// A slow reader must not wedge the server.
func TestSlowEventReaderIsDroppedNotBlocking(t *testing.T) {
	ts, _, _, _ := newServer(t)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/events", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	// Never read from resp.Body; just publish a lot and make sure the server
	// stays responsive.
	defer resp.Body.Close()

	for i := 0; i < 500; i++ {
		r := post(t, ts, "wtok", "/v1/workers/register", map[string]any{
			"host": "gpubox", "devices": []map[string]any{{"name": "gpu0"}},
		})
		r.Body.Close()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		r := get(t, ts, "ctok", "/v1/state")
		r.Body.Close()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("server blocked behind a slow event reader")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/server/ -run 'Event' -v`
Expected: FAIL — no `/v1/events` route.

- [ ] **Step 3: Implement the broadcaster**

Create `internal/server/events.go`:

```go
package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Event struct {
	Kind    string    `json:"kind"`
	At      time.Time `json:"at"`
	Payload any       `json:"payload,omitempty"`
}

type broadcaster struct {
	mu          sync.Mutex
	subscribers map[chan Event]struct{}
}

func newBroadcaster() *broadcaster {
	return &broadcaster{subscribers: map[chan Event]struct{}{}}
}

func (b *broadcaster) subscribe() (chan Event, func()) {
	// Buffered: a subscriber that stops reading loses events rather than
	// blocking every publisher behind it.
	ch := make(chan Event, 32)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock()
		delete(b.subscribers, ch)
		b.mu.Unlock()
	}
}

func (b *broadcaster) publish(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subscribers {
		select {
		case ch <- e:
		default: // slow reader: drop rather than block the controller
		}
	}
}

// Publish emits an event to every connected dashboard. It never blocks.
func (s *Server) Publish(kind string, payload any) {
	s.events.publish(Event{Kind: kind, At: s.cfg.Clock.Now(), Payload: payload})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "unsupported", "streaming unsupported")
		return
	}

	ch, unsubscribe := s.events.subscribe()
	defer unsubscribe()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case e := <-ch:
			body, err := json.Marshal(e)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", body)
			flusher.Flush()
		}
	}
}
```

Add `events *broadcaster` to `Server`, initialise it in `New`, and register
the route:

```go
	mux.Handle("GET /v1/events", s.require("client", s.handleEvents))
```

- [ ] **Step 4: Publish on the transitions that matter**

Call `s.Publish` from: `handleRegister` (`"devices"`), `handleSubmit`
(`"jobs"`), `handleJobStatus` (`"jobs"`), `handleKill` (`"jobs"`), and
`handleClearDevice` (`"devices"`). Payload is the kind's own summary — for
`"devices"`, the result of `s.deviceViews()`; for `"jobs"`, the job ID and its
new state. Keep payloads small: the dashboard refetches `/v1/state` when it
needs the full picture.

In `internal/cli/serve.go`, publish `"devices"` after any sweep that changed
something, and `"jobs"` after a scheduling pass that assigned anything.

- [ ] **Step 5: Run and commit**

```bash
go test ./... -race
git add internal/server/
git commit -m "feat: SSE event stream for live dashboard updates"
```

---

### Task 8: Dashboard v0 (read-only)

**Files:**
- Create: `internal/server/dashboard.go`, `internal/server/dashboard/index.html`
- Modify: `internal/server/server.go` (route)
- Test: `internal/server/dashboard_test.go`

**Interfaces:**
- Consumes: `/v1/state`, `/v1/events`.
- Produces: route `GET /` serving the embedded page, unauthenticated (the page itself is not secret; every API call it makes carries the token the user pastes).

- [ ] **Step 1: Write the failing test**

Create `internal/server/dashboard_test.go`:

```go
package server_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDashboardIsServedWithoutAToken(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp, err := ts.Client().Get(ts.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/html")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "resource controller")
	// The page must not embed a token.
	require.NotContains(t, strings.ToLower(string(body)), "bearer ct")
}

func TestUnknownPathIs404NotTheDashboard(t *testing.T) {
	ts, _, _, _ := newServer(t)

	resp, err := ts.Client().Get(ts.URL + "/nope")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/server/ -run 'Dashboard|UnknownPath' -v`
Expected: FAIL — `/` is not routed.

- [ ] **Step 3: Serve the embedded page**

Create `internal/server/dashboard.go`:

```go
package server

import (
	_ "embed"
	"net/http"
)

//go:embed dashboard/index.html
var dashboardHTML []byte

// handleDashboard serves the read-only dashboard. It carries no credential:
// the page prompts for a token and calls the API with it, so serving the
// page itself reveals nothing.
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(dashboardHTML)
}
```

Register it last so it cannot shadow the API:

```go
	mux.HandleFunc("GET /", s.handleDashboard)
```

- [ ] **Step 4: Write the dashboard**

Create `internal/server/dashboard/index.html`. It is one self-contained file —
no external fonts, scripts, or stylesheets, because the controller may run on
a network with no egress.

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>resource controller</title>
<style>
  :root {
    --bg:#fbfbfd; --panel:#fff; --ink:#16181d; --muted:#6b7280; --line:#e5e7eb;
    --ready:#15803d; --busy:#b45309; --unknown:#6b7280; --unhealthy:#b91c1c;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg:#0f1115; --panel:#171a21; --ink:#e8eaed; --muted:#9aa1ab; --line:#262b34;
      --ready:#4ade80; --busy:#fbbf24; --unknown:#9aa1ab; --unhealthy:#f87171;
    }
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--ink);
         font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif; }
  header { display:flex; gap:12px; align-items:center; justify-content:space-between;
           padding:14px 20px; border-bottom:1px solid var(--line); }
  h1 { font-size:15px; margin:0; font-weight:600; letter-spacing:.01em; }
  main { padding:20px; max-width:1200px; margin:0 auto; }
  h2 { font-size:12px; text-transform:uppercase; letter-spacing:.08em;
       color:var(--muted); margin:24px 0 10px; font-weight:600; }
  .grid { display:grid; gap:12px; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); }
  .card { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:14px; }
  .card.alert { border-color:var(--unhealthy); }
  .dev { display:flex; justify-content:space-between; align-items:baseline; gap:8px; }
  .name { font-weight:600; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }
  .state { font-size:12px; font-weight:600; }
  .ready{color:var(--ready)} .busy{color:var(--busy)}
  .unknown{color:var(--unknown)} .unhealthy{color:var(--unhealthy)}
  .meta { color:var(--muted); font-size:12px; margin-top:6px; }
  .cmd { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px;
         margin-top:6px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  table { width:100%; border-collapse:collapse; }
  th,td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--line); font-size:13px; }
  th { color:var(--muted); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:.06em; }
  td.mono { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; }
  .wrap { overflow-x:auto; }
  input { background:var(--panel); color:var(--ink); border:1px solid var(--line);
          border-radius:7px; padding:7px 10px; font:inherit; min-width:260px; }
  button { background:var(--ink); color:var(--bg); border:0; border-radius:7px;
           padding:7px 13px; font:inherit; font-weight:600; cursor:pointer; }
  .empty { color:var(--muted); padding:14px 0; }
  #err { color:var(--unhealthy); font-size:12px; }
  #gate { padding:40px 20px; max-width:460px; margin:0 auto; }
  .hide { display:none; }
</style>
</head>
<body>
<header>
  <h1>resource controller</h1>
  <span id="err"></span>
</header>

<div id="gate">
  <h2>client token</h2>
  <p class="meta">Held for this tab only, never stored on disk.</p>
  <form id="gateForm">
    <input id="token" type="password" autocomplete="off" placeholder="paste a client token" required>
    <button type="submit">connect</button>
  </form>
</div>

<main id="app" class="hide">
  <h2>devices</h2>
  <div class="grid" id="devices"></div>

  <h2>queue</h2>
  <div class="wrap"><table id="queue">
    <thead><tr><th>#</th><th>job</th><th>device</th><th>submitter</th><th>command</th></tr></thead>
    <tbody></tbody>
  </table></div>
  <div class="empty hide" id="queueEmpty">nothing waiting</div>

  <h2>running</h2>
  <div class="wrap"><table id="jobs">
    <thead><tr><th>job</th><th>device</th><th>state</th><th>submitter</th><th>command</th></tr></thead>
    <tbody></tbody>
  </table></div>
  <div class="empty hide" id="jobsEmpty">nothing running</div>
</main>

<script>
(function () {
  var token = sessionStorage.getItem("rc_token") || "";
  var gate = document.getElementById("gate");
  var app = document.getElementById("app");
  var err = document.getElementById("err");

  document.getElementById("gateForm").addEventListener("submit", function (e) {
    e.preventDefault();
    token = document.getElementById("token").value.trim();
    sessionStorage.setItem("rc_token", token);
    start();
  });

  function fail(message) { err.textContent = message; }

  function fetchState() {
    return fetch("/v1/state", { headers: { Authorization: "Bearer " + token } })
      .then(function (r) {
        if (r.status === 401 || r.status === 403) {
          sessionStorage.removeItem("rc_token");
          gate.classList.remove("hide");
          app.classList.add("hide");
          throw new Error("token rejected");
        }
        if (!r.ok) throw new Error("state: " + r.status);
        return r.json();
      });
  }

  function ago(seconds) {
    if (seconds < 60) return seconds + "s";
    if (seconds < 3600) return Math.floor(seconds / 60) + "m" + (seconds % 60) + "s";
    return Math.floor(seconds / 3600) + "h" + Math.floor((seconds % 3600) / 60) + "m";
  }

  function renderDevices(views) {
    var box = document.getElementById("devices");
    box.textContent = "";
    (views || []).forEach(function (v) {
      var d = v.device;
      var stale = v.heartbeat_age_seconds > 30;
      var card = document.createElement("div");
      card.className = "card" + (d.state === "unhealthy" ? " alert" : "");

      var head = document.createElement("div");
      head.className = "dev";
      var name = document.createElement("span");
      name.className = "name";
      name.textContent = d.id;
      var state = document.createElement("span");
      state.className = "state " + d.state;
      state.textContent = d.state;
      head.appendChild(name); head.appendChild(state);
      card.appendChild(head);

      var meta = document.createElement("div");
      meta.className = "meta";
      var bits = [];
      if (v.holder) bits.push("held by " + v.holder + " for " + ago(v.elapsed_seconds));
      if (stale) bits.push("no contact " + ago(v.heartbeat_age_seconds));
      meta.textContent = bits.join(" · ") || "idle";
      card.appendChild(meta);

      if (v.command && v.command.length) {
        var cmd = document.createElement("div");
        cmd.className = "cmd";
        cmd.textContent = v.command.join(" ");
        card.appendChild(cmd);
      }
      box.appendChild(card);
    });
  }

  function fillTable(id, emptyId, rows, cells) {
    var body = document.querySelector("#" + id + " tbody");
    body.textContent = "";
    (rows || []).forEach(function (row, i) {
      var tr = document.createElement("tr");
      cells(row, i).forEach(function (text, idx) {
        var td = document.createElement("td");
        if (idx === 0 || idx === 1) td.className = "mono";
        td.textContent = text;
        tr.appendChild(td);
      });
      body.appendChild(tr);
    });
    var empty = document.getElementById(emptyId);
    var none = !rows || rows.length === 0;
    empty.classList.toggle("hide", !none);
    document.getElementById(id).classList.toggle("hide", none);
  }

  function render(state) {
    err.textContent = "";
    renderDevices(state.devices);
    fillTable("queue", "queueEmpty", state.queued, function (j, i) {
      return [String(i + 1), j.id.slice(0, 8), j.device_id, j.submitter, (j.command || []).join(" ")];
    });
    fillTable("jobs", "jobsEmpty", state.jobs, function (j) {
      return [j.id.slice(0, 8), j.device_id, j.state, j.submitter, (j.command || []).join(" ")];
    });
  }

  function refresh() { fetchState().then(render).catch(function (e) { fail(e.message); }); }

  function start() {
    gate.classList.add("hide");
    app.classList.remove("hide");
    refresh();

    // SSE cannot carry an Authorization header, so the stream is only a
    // nudge: every event triggers an authenticated refetch of /v1/state.
    try {
      var es = new EventSource("/v1/events?token=" + encodeURIComponent(token));
      es.onmessage = refresh;
      es.onerror = function () { fail("event stream dropped; polling"); };
    } catch (e) { /* polling below is the fallback */ }

    setInterval(refresh, 5000);
  }

  if (token) start();
})();
</script>
</body>
</html>
```

- [ ] **Step 5: Accept the token as a query parameter on the event stream only**

`EventSource` cannot send headers, so `/v1/events` must also accept
`?token=…`. In `require`, fall back to the query parameter **only** for that
route, and note in a comment that it is there because SSE has no other way to
authenticate:

```go
		token := strings.TrimPrefix(...)
		if token == "" && r.URL.Path == "/v1/events" {
			token = r.URL.Query().Get("token")
		}
```

A query-string token can land in access logs, so it is confined to the one
route that cannot avoid it.

- [ ] **Step 6: Verify by hand**

```bash
go test ./... -race
RC_TOKENS='wtok:worker,ctok:client,atok:admin' go run . serve --addr 127.0.0.1:19600 --data /tmp/rc-dash &
```

Open `http://127.0.0.1:19600/`, paste `ctok`, and confirm the device grid,
queue, and running tables render, and that killing the worker makes a device
show "no contact". Paste what you saw into your report, then stop the server.

- [ ] **Step 7: Commit**

```bash
git add internal/server/dashboard.go internal/server/dashboard/ internal/server/dashboard_test.go internal/server/server.go
git commit -m "feat: read-only dashboard served from the controller"
```

---

### Task 9: End-to-end coverage and documentation

**Files:**
- Create: `e2e/queue_test.go`
- Modify: `README.md`, `examples/worker.yaml`
- Test: `e2e/queue_test.go`

**Interfaces:** none new.

- [ ] **Step 1: Write the end-to-end tests**

Create `e2e/queue_test.go`. Reuse the harness shape already in
`e2e/e2e_test.go` (real store, real server, real worker); factor its setup
into a helper `newFleet(t, deviceMaxRuntime time.Duration) (*client.Client, *store.Store, string)`
returning a client, the store, and the device ID, and have the existing test
use it too so there is one harness rather than two.

```go
package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/client"
	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/stretchr/testify/require"
)

// Two jobs, one device: the second waits and then runs. The marker files
// prove they never overlapped — which is the whole point of the system.
func TestQueuedJobRunsAfterTheFirstFinishes(t *testing.T) {
	cl, _, device := newFleet(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dir := t.TempDir()
	markers := filepath.Join(dir, "markers")

	first, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-a",
		Command: []string{"sh", "-c", "echo a-start >> " + markers + "; sleep 3; echo a-end >> " + markers},
	})
	require.NoError(t, err)

	second, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-b",
		Command: []string{"sh", "-c", "echo b-start >> " + markers + "; echo b-end >> " + markers},
	})
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, second.State, "the device is busy, so the second job queues")

	view, err := cl.JobView(ctx, second.ID)
	require.NoError(t, err)
	require.Equal(t, 1, view.QueuePosition)

	require.Eventually(t, func() bool {
		v, err := cl.JobView(ctx, second.ID)
		return err == nil && v.Job.State == model.JobSucceeded
	}, 60*time.Second, 250*time.Millisecond, "the queued job never ran")

	firstFinal, err := cl.Job(ctx, first.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, firstFinal.State)

	body, err := os.ReadFile(markers)
	require.NoError(t, err)
	require.Equal(t, "a-start\na-end\nb-start\nb-end\n", string(body),
		"the jobs interleaved, so they held the device at the same time")
}

// A device declaring a 2s ceiling must not let a 30s job outlive it.
func TestWatchdogKillsAnOverrunningJob(t *testing.T) {
	cl, _, device := newFleet(t, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	job, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-a",
		Command: []string{"sh", "-c", "sleep 30"},
	})
	require.NoError(t, err)

	var final *model.Job
	require.Eventually(t, func() bool {
		j, err := cl.Job(ctx, job.ID)
		if err != nil || !j.State.Terminal() {
			return false
		}
		final = j
		return true
	}, 60*time.Second, 250*time.Millisecond, "the watchdog never fired")

	require.Equal(t, model.JobKilled, final.State)
	require.Contains(t, final.KillReason, "max_runtime")

	require.Eventually(t, func() bool {
		state, err := cl.State(ctx)
		return err == nil && len(state.Devices) == 1 &&
			state.Devices[0].Device.State == model.DeviceReady
	}, 30*time.Second, 250*time.Millisecond, "the device did not return to the pool")
}

func TestKillCancelsAQueuedJobEndToEnd(t *testing.T) {
	cl, _, device := newFleet(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")

	_, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-a",
		Command: []string{"sh", "-c", "sleep 4"},
	})
	require.NoError(t, err)

	queued, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: device, Submitter: "agent-b",
		Command: []string{"sh", "-c", "echo ran > " + marker},
	})
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, queued.State)

	require.NoError(t, cl.Kill(ctx, queued.ID, "agent-b"))

	killed, err := cl.Job(ctx, queued.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobKilled, killed.State)

	// Wait past the point where it would have been scheduled had it survived.
	require.Eventually(t, func() bool {
		state, err := cl.State(ctx)
		return err == nil && len(state.Queued) == 0 &&
			len(state.Devices) == 1 && state.Devices[0].Device.State == model.DeviceReady
	}, 60*time.Second, 250*time.Millisecond, "the queue never drained")

	_, statErr := os.Stat(marker)
	require.True(t, os.IsNotExist(statErr), "a killed job must never execute")
}
```

- [ ] **Step 2: Run them repeatedly**

Run: `go test ./e2e/ -race -count=3 -timeout 300s`
Expected: PASS every time. A flaky e2e test is worse than none — widen bounds
rather than deleting assertions, and report anything you had to widen.

- [ ] **Step 3: Update the README**

Replace the "Not built yet" table's Stage 2 rows: the dashboard, queue,
watchdogs, `rc kill` and `rc attach` now exist. Document:

- `rc run` blocks by default and prints its queue position; `--no-wait`
  restores fail-fast; `--timeout` bounds the wait.
- **Ctrl-C on a queued job cancels it; Ctrl-C on a running job still only
  detaches.** State both, next to each other, because the difference is
  surprising until you know the worker owns a running job's lease.
- Per-device `max_runtime` in `worker.yaml`, with the object form shown, and
  that a job asking for more is rejected rather than clamped.
- The dashboard: what it shows, that it is read-only, and that the token is
  held in `sessionStorage` for the tab only.
- **Reboot recovery**: a host that reboots has its devices returned to `ready`
  automatically, because a reboot proves nothing is still running; a worker
  process restart without a reboot still quarantines.
- That the controller now runs a scheduler loop, so `rc serve` is doing work
  even when idle.

- [ ] **Step 4: Update `examples/worker.yaml`**

Show the object form with a ceiling and note that plain names still work.

- [ ] **Step 5: Verify the README's claims by running them**

Start a controller and worker, and execute every command block you wrote.
Paste the real output into your report. Any claim you did not watch happen
must be removed.

- [ ] **Step 6: Commit**

```bash
git add e2e/ README.md examples/worker.yaml
git commit -m "test: end-to-end queue, watchdog and kill coverage; docs: stage 2"
```

---

## Self-Review Notes

Checked against `docs/superpowers/specs/2026-08-13-resource-controller-stages-2-4-design.md`.

**Spec coverage (Stage 2 section):** queue with priority and FIFO (Task 2);
head-of-queue reservations (Task 2); `rc run` blocking with position,
`--timeout`, `--no-wait` (Task 6); Ctrl-C cancelling a queued job while a
running one only detaches (Task 6); per-device `max_runtime` ceilings with
rejection rather than clamping (Tasks 2, 5); wall-clock and idle watchdogs
enforced on the worker (Task 5); lease expiry (Task 3); boot identity with the
three-way outcome table (Task 3); `rc kill` and `rc attach` (Tasks 4, 6);
dashboard v0 with staleness and SSE (Tasks 7, 8); the data-model changes
listed in the spec (Tasks 1, 4).

**Deliberately deferred to Stage 3 or 4, per the spec:** probes, labels,
selectors, usage sheets, `rc describe`, `rc hold`, verify probes, webhook
notifications, dashboard actions.

**Carried forward from Stage 1's ledger, to fix as they are touched:**
`rc worker`'s shutdown-grace comment overstates its retry budget (Task 5
touches that file); the poll loop's floor delay already landed; raw
`err.Error()` still reaches clients from several handlers (Tasks 4 and 7 touch
them — use the generic-message shape `writeJobLookupError` already
establishes); and the `device_not_cleared` 409 message asserts a live lease
when `RowsAffected()==0` has three possible causes (Task 4 touches that
handler).

**Known risks in this plan:**

- Task 5 changes the assignments wire shape from a bare array to an envelope.
  Both sides ship together, but a worker running the old binary against a new
  controller will fail to decode. The README should say workers and controller
  upgrade together in this stage.
- The watchdog tests are timing-sensitive by nature. They use sub-second
  margins to stay fast; if CI is slower than this machine, widen them rather
  than removing the assertions.
- `ScheduleOnce` rebuilds reservations from scratch on every pass, which is
  O(queued jobs) per second. That is fine at the fleet sizes this targets and
  wrong at thousands of queued jobs; the index added in Task 1 keeps the query
  cheap, and nothing in this design needs to scale further.

