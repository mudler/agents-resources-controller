# Resource Controller Stage 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the controller, worker, and client that let remote agents claim a device exclusively, run a supervised job on it, and see who holds what — replacing the `flock` mutex.

**Architecture:** One Go binary with three modes. `rc serve` owns all state in embedded SQLite and hands out device leases inside a single transaction. `rc worker` runs on each device host, dials out over plain HTTP to long-poll for assignments, and supervises job processes in their own process group. `rc run` blocks, streams the job's output, and exits with the job's exit code.

**Tech Stack:** Go 1.26, `modernc.org/sqlite` (pure Go, no cgo), `spf13/cobra`, `stretchr/testify`, stdlib `net/http`.

**Spec:** `docs/superpowers/specs/2026-08-13-resource-controller-design.md`

## Global Constraints

- Go 1.26. Module path `github.com/mudler/resource-controller`.
- No cgo. SQLite driver is `modernc.org/sqlite` v1.56.0, imported for its `sqlite` driver name.
- SQLite opened in WAL mode with `_pragma=busy_timeout(5000)` and `_pragma=foreign_keys(1)`.
- **Stage 1 has no queue.** A submit that finds no free matching device returns `409 no_device_available` immediately. Queueing is Stage 2 and must not be built here.
- Device states in Stage 1: `ready`, `busy`, `unknown`, `unhealthy`. (`draining` is Stage 2.)
- Job states in Stage 1: `assigned`, `running`, `succeeded`, `failed`, `killed`, `lost`. (`queued` is Stage 2.)
- All **controller-side** time (store, reaper, server) passes through the `Clock` interface so those tests never sleep. Worker process supervision and the end-to-end test use real time and `require.Eventually` — a spawned process cannot observe a fake clock.
- SQLite runs with `MaxOpenConns(1)`. Any query that iterates rows and then issues another query MUST drain its rows to completion first (which releases the connection); holding open `Rows` across a nested query deadlocks.
- Every HTTP handler authenticates a bearer token and resolves a role (`worker`, `client`, `admin`).
- The invariant under test everywhere: **no two live leases on one device, ever.**

---

## File Structure

| File | Responsibility |
|---|---|
| `go.mod` | Module definition. |
| `main.go` | Cobra root; wires `serve`, `worker`, `run`, `ps`, `devices`. |
| `internal/model/model.go` | `Device`, `Lease`, `Job`, `Worker` structs; state constants. |
| `internal/clock/clock.go` | `Clock` interface, real and fake implementations. |
| `internal/store/schema.sql` | Table definitions, applied at open. |
| `internal/store/store.go` | Open/migrate; low-level device and worker queries. |
| `internal/store/allocate.go` | The allocation transaction and release. The correctness core. |
| `internal/store/allocate_test.go` | Invariant tests: concurrency, double-release, expiry. |
| `internal/store/reaper.go` | Lease expiry and worker-loss sweep. |
| `internal/server/server.go` | HTTP mux, auth middleware, JSON helpers. |
| `internal/server/worker_api.go` | `register`, `assignments` long-poll, `heartbeat`, `logs`, `status`. |
| `internal/server/client_api.go` | `POST /v1/jobs`, `GET /v1/jobs/:id`, logs SSE, `GET /v1/devices`, `GET /v1/state`. |
| `internal/server/notify.go` | Assignment fan-out to waiting long-polls. |
| `internal/logstore/logstore.go` | Per-job log files: append, read, follow. |
| `internal/worker/config.go` | `/etc/rc/worker.yaml` parsing; device declarations. |
| `internal/worker/worker.go` | Register, heartbeat loop, assignment poll loop. |
| `internal/worker/exec.go` | Process-group spawn, log pump, kill tree, exit reporting. |
| `internal/client/client.go` | Typed HTTP client used by all CLI verbs. |
| `internal/cli/run.go` | `rc run`: submit, attach, stream, propagate exit code. |
| `internal/cli/ps.go` | `rc ps`, `rc devices` table rendering. |
| `e2e/e2e_test.go` | Real controller + real worker + fake device, submit → run → release. |

---

### Task 1: Module scaffold, models, and clock

**Files:**
- Create: `go.mod`, `main.go`, `internal/model/model.go`, `internal/clock/clock.go`
- Test: `internal/clock/clock_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `model.Device{ID, Host, Name, State, LastHeartbeatAt}`, `model.Lease{ID, DeviceID, Holder, JobID, AcquiredAt, ExpiresAt}`, `model.Job{ID, Selector, Command []string, Cwd, Env map[string]string, Submitter, IdempotencyKey, State, DeviceID, WorkerID, ExitCode *int, KillReason, StartedAt, FinishedAt}`, `model.Worker{ID, Host, LastHeartbeatAt}`; state constants `model.DeviceReady|DeviceBusy|DeviceUnknown|DeviceUnhealthy` and `model.JobAssigned|JobRunning|JobSucceeded|JobFailed|JobKilled|JobLost`; `clock.Clock` interface with `Now() time.Time`, `clock.Real()`, and `clock.NewFake(t time.Time)` exposing `Advance(d time.Duration)`.

- [ ] **Step 1: Initialize the module**

```bash
cd /home/mudler/_git/resource-controller
go mod init github.com/mudler/resource-controller
go get modernc.org/sqlite@v1.56.0 github.com/spf13/cobra@v1.10.2 github.com/stretchr/testify@v1.11.1
```

- [ ] **Step 2: Write the failing clock test**

Create `internal/clock/clock_test.go`:

```go
package clock_test

import (
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/stretchr/testify/require"
)

func TestFakeClockAdvances(t *testing.T) {
	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	c := clock.NewFake(base)

	require.Equal(t, base, c.Now())

	c.Advance(90 * time.Second)
	require.Equal(t, base.Add(90*time.Second), c.Now())
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `go test ./internal/clock/ -run TestFakeClockAdvances -v`
Expected: FAIL — package `internal/clock` does not exist.

- [ ] **Step 4: Implement the clock**

Create `internal/clock/clock.go`:

```go
// Package clock provides an injectable time source so tests never sleep.
package clock

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

// Real returns the system clock.
func Real() Clock { return realClock{} }

// Fake is a manually advanced clock for tests.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

func NewFake(t time.Time) *Fake { return &Fake{now: t} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `go test ./internal/clock/ -v`
Expected: PASS.

- [ ] **Step 6: Write the models**

Create `internal/model/model.go`:

```go
// Package model holds the entities shared by the controller, worker, and client.
package model

import "time"

type DeviceState string

const (
	DeviceReady     DeviceState = "ready"
	DeviceBusy      DeviceState = "busy"
	DeviceUnknown   DeviceState = "unknown"
	DeviceUnhealthy DeviceState = "unhealthy"
)

type JobState string

const (
	JobAssigned  JobState = "assigned"
	JobRunning   JobState = "running"
	JobSucceeded JobState = "succeeded"
	JobFailed    JobState = "failed"
	JobKilled    JobState = "killed"
	JobLost      JobState = "lost"
)

// Terminal reports whether the job will never change state again.
func (s JobState) Terminal() bool {
	switch s {
	case JobSucceeded, JobFailed, JobKilled, JobLost:
		return true
	}
	return false
}

type Device struct {
	ID              string      `json:"id"` // "host:name"
	Host            string      `json:"host"`
	Name            string      `json:"name"`
	WorkerID        string      `json:"worker_id"`
	State           DeviceState `json:"state"`
	LastHeartbeatAt time.Time   `json:"last_heartbeat_at"`
}

type Lease struct {
	ID         string    `json:"id"`
	DeviceID   string    `json:"device_id"`
	Holder     string    `json:"holder"`
	JobID      string    `json:"job_id,omitempty"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type Job struct {
	ID             string            `json:"id"`
	Selector       string            `json:"selector"`
	Command        []string          `json:"command"`
	Cwd            string            `json:"cwd"`
	Env            map[string]string `json:"env,omitempty"`
	Submitter      string            `json:"submitter"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	State          JobState          `json:"state"`
	DeviceID       string            `json:"device_id"`
	WorkerID       string            `json:"worker_id"`
	ExitCode       *int              `json:"exit_code,omitempty"`
	KillReason     string            `json:"kill_reason,omitempty"`
	SubmittedAt    time.Time         `json:"submitted_at"`
	StartedAt      *time.Time        `json:"started_at,omitempty"`
	FinishedAt     *time.Time        `json:"finished_at,omitempty"`
}

type Worker struct {
	ID              string    `json:"id"`
	Host            string    `json:"host"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
}
```

- [ ] **Step 7: Write the cobra root**

Create `main.go`:

```go
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:           "rc",
		Short:         "Resource controller: exclusive device leases for shared hardware",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "rc:", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 8: Verify it builds**

Run: `go build ./... && go vet ./...`
Expected: no output, exit 0.

- [ ] **Step 9: Commit**

```bash
git add go.mod go.sum main.go internal/
git commit -m "feat: module scaffold, core models, injectable clock"
```

---

### Task 2: Store schema and the allocation transaction

This is the correctness core. Everything else is plumbing around it.

**Files:**
- Create: `internal/store/schema.sql`, `internal/store/store.go`, `internal/store/allocate.go`
- Test: `internal/store/allocate_test.go`

**Interfaces:**
- Consumes: `model`, `clock` from Task 1.
- Produces: `store.Open(path string, c clock.Clock) (*store.Store, error)`; `(*Store).UpsertWorker(w model.Worker, devices []model.Device) error`; `(*Store).Devices() ([]model.Device, error)`; `(*Store).Allocate(req store.AllocateRequest) (*model.Job, error)` returning `store.ErrNoDevice` when nothing matches; `(*Store).Release(jobID string, state model.JobState, exitCode *int, reason string) error`; `(*Store).Job(id string) (*model.Job, error)`; `(*Store).Leases() ([]model.Lease, error)`; `store.AllocateRequest{DeviceID, Command []string, Cwd, Env, Submitter, IdempotencyKey, LeaseTTL time.Duration}`.

- [ ] **Step 1: Write the schema**

Create `internal/store/schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS workers (
  id                TEXT PRIMARY KEY,
  host              TEXT NOT NULL,
  last_heartbeat_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
  id                TEXT PRIMARY KEY,
  host              TEXT NOT NULL,
  name              TEXT NOT NULL,
  worker_id         TEXT NOT NULL,
  state             TEXT NOT NULL,
  last_heartbeat_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
  id              TEXT PRIMARY KEY,
  selector        TEXT NOT NULL DEFAULT '',
  command         TEXT NOT NULL,
  cwd             TEXT NOT NULL DEFAULT '',
  env             TEXT NOT NULL DEFAULT '{}',
  submitter       TEXT NOT NULL DEFAULT '',
  idempotency_key TEXT,
  state           TEXT NOT NULL,
  device_id       TEXT NOT NULL DEFAULT '',
  worker_id       TEXT NOT NULL DEFAULT '',
  exit_code       INTEGER,
  kill_reason     TEXT NOT NULL DEFAULT '',
  submitted_at    INTEGER NOT NULL,
  started_at      INTEGER,
  finished_at     INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency
  ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS leases (
  id          TEXT PRIMARY KEY,
  device_id   TEXT NOT NULL,
  holder      TEXT NOT NULL,
  job_id      TEXT NOT NULL DEFAULT '',
  acquired_at INTEGER NOT NULL,
  expires_at  INTEGER NOT NULL,
  released_at INTEGER
);

-- The invariant, enforced by the database: at most one live lease per device.
CREATE UNIQUE INDEX IF NOT EXISTS leases_one_live_per_device
  ON leases(device_id) WHERE released_at IS NULL;
```

- [ ] **Step 2: Write the failing allocation tests**

Create `internal/store/allocate_test.go`:

```go
package store_test

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func newStore(t *testing.T) (*store.Store, *clock.Fake) {
	t.Helper()
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))
	s, err := store.Open(filepath.Join(t.TempDir(), "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", LastHeartbeatAt: c.Now()},
		[]model.Device{{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1", State: model.DeviceReady}},
	))
	return s, c
}

func req(submitter string) store.AllocateRequest {
	return store.AllocateRequest{
		DeviceID:  "gpubox:gpu0",
		Command:   []string{"./bench"},
		Submitter: submitter,
		LeaseTTL:  time.Minute,
	}
}

func TestAllocateGrantsDeviceOnce(t *testing.T) {
	s, _ := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	require.Equal(t, model.JobAssigned, job.State)
	require.Equal(t, "gpubox:gpu0", job.DeviceID)

	_, err = s.Allocate(req("agent-b"))
	require.ErrorIs(t, err, store.ErrNoDevice)
}

func TestReleaseReturnsDeviceToPool(t *testing.T) {
	s, _ := newStore(t)

	first, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	code := 0
	require.NoError(t, s.Release(first.ID, model.JobSucceeded, &code, ""))

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)

	second, err := s.Allocate(req("agent-b"))
	require.NoError(t, err)
	require.NotEqual(t, first.ID, second.ID)
}

// The invariant: concurrent claimants, exactly one winner, no torn state.
func TestConcurrentAllocateYieldsExactlyOneWinner(t *testing.T) {
	s, _ := newStore(t)

	const claimants = 32
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		granted int
		denied  int
	)
	start := make(chan struct{})

	for i := 0; i < claimants; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.Allocate(req("agent"))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				granted++
			case errors.Is(err, store.ErrNoDevice):
				denied++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, 1, granted)
	require.Equal(t, claimants-1, denied)

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Len(t, leases, 1)
}

func TestAllocateSkipsUnhealthyDevice(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.SetDeviceState("gpubox:gpu0", model.DeviceUnhealthy, c.Now()))

	_, err := s.Allocate(req("agent-a"))
	require.ErrorIs(t, err, store.ErrNoDevice)
}

func TestIdempotentSubmitReturnsSameJob(t *testing.T) {
	s, _ := newStore(t)

	r := req("agent-a")
	r.IdempotencyKey = "abc123"

	first, err := s.Allocate(r)
	require.NoError(t, err)

	second, err := s.Allocate(r)
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)

	leases, err := s.Leases()
	require.NoError(t, err)
	require.Len(t, leases, 1)
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./internal/store/ -v`
Expected: FAIL — package `internal/store` does not exist.

- [ ] **Step 4: Implement store open and queries**

Create `internal/store/store.go`:

```go
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
```

- [ ] **Step 5: Implement allocation and release**

Create `internal/store/allocate.go`:

```go
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
		j                    model.Job
		cmdJSON, envJSON     string
		started, finished    sql.NullInt64
		exitCode             sql.NullInt64
		idem                 sql.NullString
		submitted            int64
	)
	err := s.db.QueryRow(
		`SELECT id, selector, command, cwd, env, submitter, idempotency_key, state,
		        device_id, worker_id, exit_code, kill_reason, submitted_at, started_at, finished_at
		 FROM jobs WHERE id = ?`, id,
	).Scan(&j.ID, &j.Selector, &cmdJSON, &j.Cwd, &envJSON, &j.Submitter, &idem, &j.State,
		&j.DeviceID, &j.WorkerID, &exitCode, &j.KillReason, &submitted, &started, &finished)
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
	return &j, nil
}

// MarkRunning records that the worker has actually spawned the process.
func (s *Store) MarkRunning(jobID string, at time.Time) error {
	_, err := s.db.Exec(
		`UPDATE jobs SET state = ?, started_at = ? WHERE id = ? AND state = ?`,
		string(model.JobRunning), at.Unix(), jobID, string(model.JobAssigned),
	)
	return err
}
```

- [ ] **Step 6: Add the uuid dependency and run the tests**

```bash
go get github.com/google/uuid@v1.6.0
go test ./internal/store/ -v
```

Expected: all five tests PASS.

- [ ] **Step 7: Run the concurrency test under the race detector**

Run: `go test ./internal/store/ -run TestConcurrentAllocate -race -count=20 -v`
Expected: PASS every iteration. This is the test that stands in for the flock guarantee — if it ever flakes, stop and fix it before building anything on top.

- [ ] **Step 8: Commit**

```bash
git add internal/store/ go.mod go.sum
git commit -m "feat: sqlite store with single-transaction device allocation"
```

---

### Task 3: The reaper — worker loss and lease expiry

Implements the spec's governing invariant: never hand out a device we cannot prove is free. A device whose worker went silent becomes `unknown`, then `unhealthy` — never `ready`.

**Files:**
- Create: `internal/store/reaper.go`
- Test: `internal/store/reaper_test.go`

**Interfaces:**
- Consumes: `store.Store` from Task 2.
- Produces: `(*Store).Sweep(grace, unhealthyAfter time.Duration) (store.SweepResult, error)` where `SweepResult{DevicesUnknown, DevicesUnhealthy, JobsLost []string}`; `(*Store).RecordHeartbeat(workerID string, at time.Time) error`; `(*Store).ClearDevice(id string) error`.

- [ ] **Step 1: Write the failing reaper tests**

Create `internal/store/reaper_test.go`:

```go
package store_test

import (
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/stretchr/testify/require"
)

func TestSweepMarksSilentWorkerDevicesUnknown(t *testing.T) {
	s, c := newStore(t)

	c.Advance(45 * time.Second)
	res, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []string{"gpubox:gpu0"}, res.DevicesUnknown)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnknown, devices[0].State)
}

func TestHeartbeatRestoresUnknownDeviceToReady(t *testing.T) {
	s, c := newStore(t)

	c.Advance(45 * time.Second)
	_, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)

	require.NoError(t, s.RecordHeartbeat("w1", c.Now()))

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)
}

// A device demoted to unknown while it was BUSY must come back as busy, not
// ready: the job is still running on it. Returning it to the pool would hand
// an occupied GPU to the next claimant.
func TestHeartbeatRestoresLeasedDeviceToBusyNotReady(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	c.Advance(45 * time.Second)
	_, err = s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)

	require.NoError(t, s.RecordHeartbeat("w1", c.Now()))

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceBusy, devices[0].State)

	// And it must not be claimable while that lease is live.
	_, err = s.Allocate(req("agent-b"))
	require.ErrorIs(t, err, store.ErrNoDevice)

	// Releasing the original job still returns it to the pool normally.
	code := 0
	require.NoError(t, s.Release(job.ID, model.JobSucceeded, &code, ""))
	devices, err = s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)
}

// A device whose worker never came back is NOT returned to the pool: we cannot
// prove nothing is still occupying it.
func TestSweepMarksLostWorkerDevicesUnhealthyNotReady(t *testing.T) {
	s, c := newStore(t)

	job, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)

	c.Advance(10 * time.Minute)
	res, err := s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, []string{"gpubox:gpu0"}, res.DevicesUnhealthy)
	require.Equal(t, []string{job.ID}, res.JobsLost)

	devices, err := s.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceUnhealthy, devices[0].State)

	reloaded, err := s.Job(job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobLost, reloaded.State)

	// Nothing may be scheduled onto it.
	_, err = s.Allocate(req("agent-b"))
	require.ErrorIs(t, err, store.ErrNoDevice)
}

func TestClearDeviceMakesUnhealthyDeviceSchedulableAgain(t *testing.T) {
	s, c := newStore(t)

	_, err := s.Allocate(req("agent-a"))
	require.NoError(t, err)
	c.Advance(10 * time.Minute)
	_, err = s.Sweep(30*time.Second, 5*time.Minute)
	require.NoError(t, err)

	require.NoError(t, s.ClearDevice("gpubox:gpu0"))
	require.NoError(t, s.RecordHeartbeat("w1", c.Now()))

	job, err := s.Allocate(req("agent-b"))
	require.NoError(t, err)
	require.Equal(t, "gpubox:gpu0", job.DeviceID)
}
```

Add `"github.com/mudler/resource-controller/internal/store"` to the import block if the file does not already have it.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/store/ -run 'Sweep|Heartbeat|ClearDevice' -v`
Expected: FAIL — `s.Sweep undefined`.

- [ ] **Step 3: Implement the reaper**

Create `internal/store/reaper.go`:

```go
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/store/ -race -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/
git commit -m "feat: reaper demotes silent workers, never returns unproven devices to the pool"
```

---

### Task 4: Log store

**Files:**
- Create: `internal/logstore/logstore.go`
- Test: `internal/logstore/logstore_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `logstore.New(dir string) (*logstore.Store, error)`; `(*Store).Append(jobID string, chunk []byte) error`; `(*Store).Read(jobID string) ([]byte, error)`; `(*Store).Follow(ctx context.Context, jobID string, done <-chan struct{}) (<-chan []byte, error)` streaming existing content then new appends until `done` closes.

- [ ] **Step 1: Write the failing test**

Create `internal/logstore/logstore_test.go`:

```go
package logstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/stretchr/testify/require"
)

func TestAppendThenRead(t *testing.T) {
	s, err := logstore.New(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, s.Append("job1", []byte("hello ")))
	require.NoError(t, s.Append("job1", []byte("world")))

	got, err := s.Read("job1")
	require.NoError(t, err)
	require.Equal(t, "hello world", string(got))
}

func TestFollowDeliversExistingThenNewChunks(t *testing.T) {
	s, err := logstore.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, s.Append("job1", []byte("first\n")))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})

	ch, err := s.Follow(ctx, "job1", done)
	require.NoError(t, err)

	require.Equal(t, "first\n", string(<-ch))

	require.NoError(t, s.Append("job1", []byte("second\n")))
	require.Equal(t, "second\n", string(<-ch))

	close(done)
	for range ch { // drain until the follower closes the channel
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/logstore/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement the log store**

Create `internal/logstore/logstore.go`:

```go
// Package logstore keeps one append-only file per job and lets clients follow
// it while the job is still running.
package logstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type Store struct {
	dir string

	mu        sync.Mutex
	listeners map[string][]chan struct{}
}

func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	return &Store{dir: dir, listeners: map[string][]chan struct{}{}}, nil
}

func (s *Store) path(jobID string) (string, error) {
	if !safeID.MatchString(jobID) {
		return "", fmt.Errorf("invalid job id %q", jobID)
	}
	return filepath.Join(s.dir, jobID+".log"), nil
}

func (s *Store) Append(jobID string, chunk []byte) error {
	p, err := s.path(jobID)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(chunk); err != nil {
		return err
	}
	s.wake(jobID)
	return nil
}

func (s *Store) wake(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.listeners[jobID] {
		select {
		case ch <- struct{}{}:
		default: // a pending wakeup is as good as another
		}
	}
}

func (s *Store) subscribe(jobID string) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	s.mu.Lock()
	s.listeners[jobID] = append(s.listeners[jobID], ch)
	s.mu.Unlock()

	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		out := s.listeners[jobID][:0]
		for _, c := range s.listeners[jobID] {
			if c != ch {
				out = append(out, c)
			}
		}
		s.listeners[jobID] = out
	}
}

func (s *Store) Read(jobID string) ([]byte, error) {
	p, err := s.path(jobID)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return b, err
}

// Follow streams the file from the beginning and then tails it. The returned
// channel closes when done closes, the context ends, or the file is unreadable.
func (s *Store) Follow(ctx context.Context, jobID string, done <-chan struct{}) (<-chan []byte, error) {
	p, err := s.path(jobID)
	if err != nil {
		return nil, err
	}
	// Create it so a follower attached before the first write does not fail.
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}

	wake, unsubscribe := s.subscribe(jobID)
	out := make(chan []byte)

	go func() {
		defer close(out)
		defer f.Close()
		defer unsubscribe()

		buf := make([]byte, 32*1024)
		drain := func() bool {
			for {
				n, err := f.Read(buf)
				if n > 0 {
					chunk := make([]byte, n)
					copy(chunk, buf[:n])
					select {
					case out <- chunk:
					case <-ctx.Done():
						return false
					}
					continue
				}
				if err == io.EOF || err == nil {
					return true
				}
				return false
			}
		}

		for {
			if !drain() {
				return
			}
			select {
			case <-wake:
			case <-done:
				drain() // flush whatever landed just before the job ended
				return
			case <-ctx.Done():
				return
			case <-time.After(time.Second): // safety net against a missed wakeup
			}
		}
	}()

	return out, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/logstore/ -race -v`
Expected: both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/logstore/
git commit -m "feat: per-job append-only log store with follow support"
```

---

### Task 5: Server core, auth, and the worker API

**Files:**
- Create: `internal/server/server.go`, `internal/server/notify.go`, `internal/server/worker_api.go`
- Test: `internal/server/worker_api_test.go`

**Interfaces:**
- Consumes: `store`, `logstore`, `clock`, `model`.
- Produces: `server.New(cfg server.Config) *server.Server` with `Config{Store *store.Store, Logs *logstore.Store, Clock clock.Clock, Tokens map[string]string /* token -> role */}`; `(*Server).Handler() http.Handler`; wire types `server.RegisterRequest{Host string, Devices []string}`, `server.RegisterResponse{WorkerID string}`, `server.Assignment{JobID string, DeviceID string, Command []string, Cwd string, Env map[string]string}`, `server.StatusRequest{State model.JobState, ExitCode *int, Reason string}`.

- [ ] **Step 1: Write the failing worker API test**

Create `internal/server/worker_api_test.go`:

```go
package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func newServer(t *testing.T) (*httptest.Server, *store.Store, *clock.Fake) {
	t.Helper()
	dir := t.TempDir()
	c := clock.NewFake(time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC))

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })

	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client", "atok": "admin"},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, st, c
}

func post(t *testing.T, ts *httptest.Server, token, path string, body any) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+path, &buf)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}

func TestUnauthenticatedRequestIsRejected(t *testing.T) {
	ts, _, _ := newServer(t)
	resp := post(t, ts, "bogus", "/v1/workers/register", server.RegisterRequest{Host: "gpubox"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestClientTokenCannotRegisterWorker(t *testing.T) {
	ts, _, _ := newServer(t)
	resp := post(t, ts, "ctok", "/v1/workers/register", server.RegisterRequest{Host: "gpubox"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestRegisterCreatesDevices(t *testing.T) {
	ts, st, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []string{"gpu0", "gpu1"}})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.WorkerID)

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Len(t, devices, 2)
	require.Equal(t, "gpubox:gpu0", devices[0].ID)
	require.Equal(t, model.DeviceReady, devices[0].State)
}

func TestAssignmentsLongPollReturns204WhenIdle(t *testing.T) {
	ts, _, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []string{"gpu0"}})
	var reg server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&reg))
	resp.Body.Close()

	req, err := http.NewRequest(http.MethodGet,
		ts.URL+"/v1/workers/"+reg.WorkerID+"/assignments?wait=50ms", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer wtok")

	got, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer got.Body.Close()
	require.Equal(t, http.StatusNoContent, got.StatusCode)
}

func TestWorkerReportingTerminalStatusFreesDevice(t *testing.T) {
	ts, st, _ := newServer(t)

	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []string{"gpu0"}})
	resp.Body.Close()

	job, err := st.Allocate(store.AllocateRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"true"}, Submitter: "agent-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)

	code := 0
	sr := post(t, ts, "wtok", "/v1/jobs/"+job.ID+"/status",
		server.StatusRequest{State: model.JobSucceeded, ExitCode: &code})
	defer sr.Body.Close()
	require.Equal(t, http.StatusOK, sr.StatusCode)

	devices, err := st.Devices()
	require.NoError(t, err)
	require.Equal(t, model.DeviceReady, devices[0].State)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/server/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement the server core**

Create `internal/server/server.go`:

```go
// Package server exposes the controller's HTTP API. Every route is plain
// HTTP so it passes through the tunnels that already carry ssh.
package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/store"
)

type Config struct {
	Store  *store.Store
	Logs   *logstore.Store
	Clock  clock.Clock
	Tokens map[string]string // token -> role: worker | client | admin
}

type Server struct {
	cfg    Config
	notify *notifier
}

func New(cfg Config) *Server {
	return &Server{cfg: cfg, notify: newNotifier()}
}

type ctxKey string

const roleKey ctxKey = "role"

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("POST /v1/workers/register", s.require("worker", s.handleRegister))
	mux.Handle("GET /v1/workers/{id}/assignments", s.require("worker", s.handleAssignments))
	mux.Handle("POST /v1/workers/{id}/heartbeat", s.require("worker", s.handleHeartbeat))
	mux.Handle("POST /v1/jobs/{id}/logs", s.require("worker", s.handleAppendLogs))
	mux.Handle("POST /v1/jobs/{id}/status", s.require("worker", s.handleJobStatus))

	mux.Handle("POST /v1/jobs", s.require("client", s.handleSubmit))
	mux.Handle("GET /v1/jobs/{id}", s.require("client", s.handleGetJob))
	mux.Handle("GET /v1/jobs/{id}/logs", s.require("client", s.handleStreamLogs))
	mux.Handle("GET /v1/devices", s.require("client", s.handleDevices))
	mux.Handle("GET /v1/state", s.require("client", s.handleState))

	mux.Handle("POST /v1/devices/{id}/clear", s.require("admin", s.handleClearDevice))

	return mux
}

// require authenticates the bearer token and enforces the minimum role.
// Roles are ordered: admin outranks client outranks worker for client routes,
// but worker routes accept only worker and admin tokens.
func (s *Server) require(role string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		got, ok := s.cfg.Tokens[token]
		if token == "" || !ok {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "unknown or missing token")
			return
		}
		if !allows(got, role) {
			writeErr(w, http.StatusForbidden, "forbidden", "token role "+got+" may not call a "+role+" route")
			return
		}
		h(w, r)
	})
}

func allows(have, want string) bool {
	if have == "admin" || have == want {
		return true
	}
	return false
}

type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: code, Message: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write response", "err", err)
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return false
	}
	return true
}
```

- [ ] **Step 4: Implement the assignment notifier**

Create `internal/server/notify.go`:

```go
package server

import "sync"

// notifier wakes a worker's long-poll the moment a job is assigned to it,
// so submit-to-start latency does not depend on the poll interval.
type notifier struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

func newNotifier() *notifier {
	return &notifier{waiters: map[string][]chan struct{}{}}
}

func (n *notifier) wait(workerID string) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	n.waiters[workerID] = append(n.waiters[workerID], ch)
	n.mu.Unlock()

	return ch, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		out := n.waiters[workerID][:0]
		for _, c := range n.waiters[workerID] {
			if c != ch {
				out = append(out, c)
			}
		}
		n.waiters[workerID] = out
	}
}

func (n *notifier) poke(workerID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, ch := range n.waiters[workerID] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
```

- [ ] **Step 5: Implement the worker API**

Create `internal/server/worker_api.go`:

```go
package server

import (
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/mudler/resource-controller/internal/model"
)

type RegisterRequest struct {
	Host    string   `json:"host"`
	Devices []string `json:"devices"`
}

type RegisterResponse struct {
	WorkerID string `json:"worker_id"`
}

type Assignment struct {
	JobID    string            `json:"job_id"`
	DeviceID string            `json:"device_id"`
	Command  []string          `json:"command"`
	Cwd      string            `json:"cwd,omitempty"`
	Env      map[string]string `json:"env,omitempty"`
}

type StatusRequest struct {
	State    model.JobState `json:"state"`
	ExitCode *int           `json:"exit_code,omitempty"`
	Reason   string         `json:"reason,omitempty"`
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Host == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "host required")
		return
	}

	now := s.cfg.Clock.Now()
	// The worker ID is derived from the host so a restarted worker resumes its
	// own devices instead of orphaning them.
	workerID := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(req.Host)).String()

	devices := make([]model.Device, 0, len(req.Devices))
	for _, name := range req.Devices {
		devices = append(devices, model.Device{
			ID: req.Host + ":" + name, Host: req.Host, Name: name,
			WorkerID: workerID, State: model.DeviceReady, LastHeartbeatAt: now,
		})
	}

	if err := s.cfg.Store.UpsertWorker(
		model.Worker{ID: workerID, Host: req.Host, LastHeartbeatAt: now}, devices); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, RegisterResponse{WorkerID: workerID})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if err := s.cfg.Store.RecordHeartbeat(r.PathValue("id"), s.cfg.Clock.Now()); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleAssignments long-polls: it returns as soon as this worker has work,
// otherwise 204 after the wait window so the worker simply calls again.
func (s *Server) handleAssignments(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")

	wait := 30 * time.Second
	if v := r.URL.Query().Get("wait"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 && d <= time.Minute {
			wait = d
		}
	}

	woken, cancel := s.notify.wait(workerID)
	defer cancel()

	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	for {
		jobs, err := s.cfg.Store.AssignedJobsFor(workerID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
		if len(jobs) > 0 {
			out := make([]Assignment, 0, len(jobs))
			for _, j := range jobs {
				out = append(out, Assignment{
					JobID: j.ID, DeviceID: j.DeviceID, Command: j.Command, Cwd: j.Cwd, Env: j.Env,
				})
			}
			writeJSON(w, http.StatusOK, out)
			return
		}
		select {
		case <-woken:
		case <-deadline.C:
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) handleAppendLogs(w http.ResponseWriter, r *http.Request) {
	chunk, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := s.cfg.Logs.Append(r.PathValue("id"), chunk); err != nil {
		writeErr(w, http.StatusInternalServerError, "log_error", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	var req StatusRequest
	if !decode(w, r, &req) {
		return
	}
	jobID := r.PathValue("id")

	if req.State == model.JobRunning {
		if err := s.cfg.Store.MarkRunning(jobID, s.cfg.Clock.Now()); err != nil {
			writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if !req.State.Terminal() {
		writeErr(w, http.StatusBadRequest, "bad_request", "state must be running or terminal")
		return
	}
	if err := s.cfg.Store.Release(jobID, req.State, req.ExitCode, req.Reason); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}
```

- [ ] **Step 6: Add the store query the assignment route needs**

Append to `internal/store/store.go`:

```go
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
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test ./internal/server/ -race -v`
Expected: all five tests PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/server/ internal/store/store.go
git commit -m "feat: controller HTTP core, token auth, and worker API"
```

---

### Task 6: Client API — submit, fetch, stream, list

**Files:**
- Create: `internal/server/client_api.go`
- Test: `internal/server/client_api_test.go`

**Interfaces:**
- Consumes: everything from Task 5.
- Produces: `server.SubmitRequest{DeviceID string, Command []string, Cwd string, Env map[string]string, Submitter string, IdempotencyKey string, LeaseTTLSeconds int}`; `server.StateResponse{Devices []server.DeviceView, Jobs []model.Job}`; `server.DeviceView{Device model.Device, Holder string, JobID string, Command []string, ElapsedSeconds int, HeartbeatAgeSeconds int}`.

- [ ] **Step 1: Write the failing client API test**

Create `internal/server/client_api_test.go`:

```go
package server_test

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

func registerWorker(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	resp := post(t, ts, "wtok", "/v1/workers/register",
		server.RegisterRequest{Host: "gpubox", Devices: []string{"gpu0"}})
	defer resp.Body.Close()
	var reg server.RegisterResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&reg))
	return reg.WorkerID
}

func TestSubmitAllocatesDeviceAndReturnsJob(t *testing.T) {
	ts, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var job model.Job
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&job))
	require.Equal(t, model.JobAssigned, job.State)
	require.Equal(t, "gpubox:gpu0", job.DeviceID)
}

// Stage 1 has no queue: a busy device is refused immediately, not parked.
func TestSubmitOnBusyDeviceReturns409(t *testing.T) {
	ts, _, _ := newServer(t)
	registerWorker(t, ts)

	first := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	first.Body.Close()

	second := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-b",
	})
	defer second.Body.Close()
	require.Equal(t, http.StatusConflict, second.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(second.Body).Decode(&body))
	require.Equal(t, "no_device_available", body["error"])
}

func TestDevicesViewShowsHolderAndHeartbeatAge(t *testing.T) {
	ts, _, c := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	resp.Body.Close()

	c.Advance(90 * time.Second)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/state", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	got, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer got.Body.Close()

	var state server.StateResponse
	require.NoError(t, json.NewDecoder(got.Body).Decode(&state))
	require.Len(t, state.Devices, 1)
	require.Equal(t, "agent-a", state.Devices[0].Holder)
	require.Equal(t, []string{"./bench"}, state.Devices[0].Command)
	require.Equal(t, 90, state.Devices[0].ElapsedSeconds)
	require.Equal(t, 90, state.Devices[0].HeartbeatAgeSeconds)
}

func TestLogStreamDeliversWorkerChunks(t *testing.T) {
	ts, _, _ := newServer(t)
	registerWorker(t, ts)

	resp := post(t, ts, "ctok", "/v1/jobs", server.SubmitRequest{
		DeviceID: "gpubox:gpu0", Command: []string{"./bench"}, Submitter: "agent-a",
	})
	var job model.Job
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&job))
	resp.Body.Close()

	appended := post(t, ts, "wtok", "/v1/jobs/"+job.ID+"/logs", nil)
	appended.Body.Close()

	// Real chunks arrive as a raw body, not JSON.
	raw, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/jobs/"+job.ID+"/logs",
		strings.NewReader("hello from the box\n"))
	require.NoError(t, err)
	raw.Header.Set("Authorization", "Bearer wtok")
	pushed, err := ts.Client().Do(raw)
	require.NoError(t, err)
	pushed.Body.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/jobs/"+job.ID+"/logs", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ctok")
	stream, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer stream.Body.Close()

	sc := bufio.NewScanner(stream.Body)
	require.True(t, sc.Scan())
	require.Contains(t, sc.Text(), "hello from the box")
}
```

Both test files are in package `server_test` and share the `newServer` and `post` helpers already defined in `worker_api_test.go` — do not redeclare them here. Import `net/http/httptest` in this file for the `registerWorker` signature.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/server/ -run 'Submit|DevicesView|LogStream' -v`
Expected: FAIL — `server.SubmitRequest` undefined.

- [ ] **Step 3: Implement the client API**

Create `internal/server/client_api.go`:

```go
package server

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/store"
)

type SubmitRequest struct {
	DeviceID        string            `json:"device_id"`
	Command         []string          `json:"command"`
	Cwd             string            `json:"cwd,omitempty"`
	Env             map[string]string `json:"env,omitempty"`
	Submitter       string            `json:"submitter"`
	IdempotencyKey  string            `json:"idempotency_key,omitempty"`
	LeaseTTLSeconds int               `json:"lease_ttl_seconds,omitempty"`
}

type DeviceView struct {
	Device              model.Device `json:"device"`
	Holder              string       `json:"holder,omitempty"`
	JobID               string       `json:"job_id,omitempty"`
	Command             []string     `json:"command,omitempty"`
	ElapsedSeconds      int          `json:"elapsed_seconds"`
	HeartbeatAgeSeconds int          `json:"heartbeat_age_seconds"`
}

type StateResponse struct {
	Devices []DeviceView `json:"devices"`
	Jobs    []model.Job  `json:"jobs"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req SubmitRequest
	if !decode(w, r, &req) {
		return
	}
	if len(req.Command) == 0 {
		writeErr(w, http.StatusBadRequest, "bad_request", "command required")
		return
	}
	if req.DeviceID == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "device_id required (selectors arrive in stage 2)")
		return
	}

	ttl := time.Duration(req.LeaseTTLSeconds) * time.Second
	job, err := s.cfg.Store.Allocate(store.AllocateRequest{
		DeviceID: req.DeviceID, Command: req.Command, Cwd: req.Cwd, Env: req.Env,
		Submitter: req.Submitter, IdempotencyKey: req.IdempotencyKey, LeaseTTL: ttl,
	})
	if errors.Is(err, store.ErrNoDevice) {
		// No queue in stage 1: say so immediately rather than block.
		writeErr(w, http.StatusConflict, "no_device_available",
			fmt.Sprintf("%s is not free", req.DeviceID))
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}

	s.notify.poke(job.WorkerID)
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.cfg.Store.Job(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleDevices(w http.ResponseWriter, r *http.Request) {
	views, err := s.deviceViews()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	views, err := s.deviceViews()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	jobs, err := s.cfg.Store.ActiveJobs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, StateResponse{Devices: views, Jobs: jobs})
}

// deviceViews joins devices to their live lease so a caller sees who holds
// what and how stale the information is — the thing a lock file cannot say.
func (s *Server) deviceViews() ([]DeviceView, error) {
	devices, err := s.cfg.Store.Devices()
	if err != nil {
		return nil, err
	}
	leases, err := s.cfg.Store.Leases()
	if err != nil {
		return nil, err
	}
	byDevice := map[string]struct {
		holder string
		jobID  string
		since  time.Time
	}{}
	for _, l := range leases {
		byDevice[l.DeviceID] = struct {
			holder string
			jobID  string
			since  time.Time
		}{l.Holder, l.JobID, l.AcquiredAt}
	}

	now := s.cfg.Clock.Now()
	out := make([]DeviceView, 0, len(devices))
	for _, d := range devices {
		v := DeviceView{
			Device:              d,
			HeartbeatAgeSeconds: int(now.Sub(d.LastHeartbeatAt).Seconds()),
		}
		if l, ok := byDevice[d.ID]; ok {
			v.Holder = l.holder
			v.JobID = l.jobID
			v.ElapsedSeconds = int(now.Sub(l.since).Seconds())
			if job, err := s.cfg.Store.Job(l.jobID); err == nil {
				v.Command = job.Command
			}
		}
		out = append(out, v)
	}
	return out, nil
}

func (s *Server) handleClearDevice(w http.ResponseWriter, r *http.Request) {
	if err := s.cfg.Store.ClearDevice(r.PathValue("id")); err != nil {
		writeErr(w, http.StatusInternalServerError, "store_error", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleStreamLogs streams a job's output as newline-delimited chunks and
// ends when the job reaches a terminal state.
func (s *Server) handleStreamLogs(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "unsupported", "streaming unsupported")
		return
	}

	done := make(chan struct{})
	chunks, err := s.cfg.Logs.Follow(r.Context(), jobID, done)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	// Close the follower once the job finishes.
	go func() {
		defer close(done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				job, err := s.cfg.Store.Job(jobID)
				if err == nil && job.State.Terminal() {
					time.Sleep(200 * time.Millisecond) // let trailing chunks land
					return
				}
			}
		}
	}()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	for chunk := range chunks {
		if _, err := w.Write(chunk); err != nil {
			return
		}
		flusher.Flush()
	}
}
```

- [ ] **Step 4: Add the ActiveJobs query**

Append to `internal/store/store.go`:

```go
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/server/ -race -v`
Expected: all tests in both server test files PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/server/ internal/store/store.go
git commit -m "feat: client API for submit, job fetch, log streaming, and state"
```

---

### Task 7: Worker process supervision

The piece that makes a kill actually kill. A SIGKILL to one pid leaves CUDA children alive holding VRAM — which is exactly how a device ends up occupied with nothing visibly running.

**Files:**
- Create: `internal/worker/exec.go`
- Test: `internal/worker/exec_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `worker.Run(ctx context.Context, spec worker.JobSpec, sink io.Writer) worker.Result`; `worker.JobSpec{Command []string, Cwd string, Env map[string]string, GraceCeiling time.Duration}`; `worker.Result{ExitCode int, Killed bool, Reason string, Err error}`.

- [ ] **Step 1: Write the failing supervision tests**

Create `internal/worker/exec_test.go`:

```go
package worker_test

import (
	"bytes"
	"context"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestRunCapturesOutputAndExitCode(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command: []string{"sh", "-c", "echo out; echo err >&2; exit 3"},
	}, &out)

	require.NoError(t, res.Err)
	require.Equal(t, 3, res.ExitCode)
	require.False(t, res.Killed)
	require.Contains(t, out.String(), "out")
	require.Contains(t, out.String(), "err")
}

func TestRunSetsJobEnvironment(t *testing.T) {
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command: []string{"sh", "-c", "echo $RC_DEVICE/$CUDA_VISIBLE_DEVICES"},
		Env:     map[string]string{"RC_DEVICE": "gpubox:gpu0", "CUDA_VISIBLE_DEVICES": "0"},
	}, &out)

	require.NoError(t, res.Err)
	require.Equal(t, 0, res.ExitCode)
	require.Contains(t, out.String(), "gpubox:gpu0/0")
}

func TestRunRunsInRequestedDirectory(t *testing.T) {
	dir := t.TempDir()
	var out syncBuf
	res := worker.Run(context.Background(), worker.JobSpec{
		Command: []string{"pwd"}, Cwd: dir,
	}, &out)

	require.NoError(t, res.Err)
	// macOS reports /private/var for /var; compare the resolved suffix.
	require.Contains(t, strings.TrimSpace(out.String()), strings.TrimPrefix(dir, "/private"))
}

// The one that matters: cancelling must take down the whole process tree,
// not just the shell we spawned.
func TestCancelKillsTheEntireProcessTree(t *testing.T) {
	var out syncBuf
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan worker.Result, 1)
	go func() {
		done <- worker.Run(ctx, worker.JobSpec{
			// Print the grandchild's pid, then keep both alive.
			Command:      []string{"sh", "-c", "sleep 300 & echo child:$!; wait"},
			GraceCeiling: 500 * time.Millisecond,
		}, &out)
	}()

	var childPID int
	require.Eventually(t, func() bool {
		_, after, found := strings.Cut(out.String(), "child:")
		if !found {
			return false
		}
		pid, err := strconv.Atoi(strings.TrimSpace(strings.SplitN(after, "\n", 2)[0]))
		if err != nil {
			return false
		}
		childPID = pid
		return true
	}, 5*time.Second, 20*time.Millisecond)

	cancel()

	res := <-done
	require.True(t, res.Killed)
	require.Equal(t, "cancelled", res.Reason)

	require.Eventually(t, func() bool {
		return !processAlive(childPID)
	}, 5*time.Second, 50*time.Millisecond, "grandchild %d survived the kill", childPID)
}

// kill -0 probes for existence without signalling. os.FindProcess is useless
// here: on Unix it never fails, and Process.Signal(nil) errors on the nil
// signal rather than probing, which would make this always report "dead" and
// the assertion below vacuous.
func processAlive(pid int) bool {
	return exec.Command("kill", "-0", strconv.Itoa(pid)).Run() == nil
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/worker/ -v`
Expected: FAIL — package `internal/worker` does not exist.

- [ ] **Step 3: Implement supervision**

Create `internal/worker/exec.go`:

```go
// Package worker runs jobs on a device host and reports back to the controller.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type JobSpec struct {
	Command      []string
	Cwd          string
	Env          map[string]string
	GraceCeiling time.Duration // SIGTERM -> SIGKILL window; default 10s
}

type Result struct {
	ExitCode int
	Killed   bool
	Reason   string
	Err      error
}

// Run spawns the command in its OWN process group and merges stdout and stderr
// into sink. On cancellation the whole group is signalled, so children that
// outlive their parent — the ones still holding device memory — die too.
func Run(ctx context.Context, spec JobSpec, sink io.Writer) Result {
	if len(spec.Command) == 0 {
		return Result{ExitCode: -1, Err: errors.New("empty command")}
	}
	grace := spec.GraceCeiling
	if grace <= 0 {
		grace = 10 * time.Second
	}

	cmd := exec.Command(spec.Command[0], spec.Command[1:]...)
	cmd.Dir = spec.Cwd
	cmd.Env = os.Environ()
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout = sink
	cmd.Stderr = sink
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return Result{ExitCode: -1, Err: fmt.Errorf("start: %w", err)}
	}
	pgid := cmd.Process.Pid // Setpgid makes the child its own group leader.

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	killGroup := func(sig syscall.Signal) {
		// Negative pid addresses the whole group.
		_ = syscall.Kill(-pgid, sig)
	}

	var killed bool
	var reason string

	select {
	case err := <-waitErr:
		return finish(err, killed, reason)
	case <-ctx.Done():
		killed = true
		reason = "cancelled"
		killGroup(syscall.SIGTERM)

		select {
		case err := <-waitErr:
			return finish(err, killed, reason)
		case <-time.After(grace):
			killGroup(syscall.SIGKILL)
			err := <-waitErr
			return finish(err, killed, reason)
		}
	}
}

func finish(err error, killed bool, reason string) Result {
	res := Result{Killed: killed, Reason: reason}
	if err == nil {
		return res
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		res.ExitCode = ee.ExitCode()
		if res.ExitCode < 0 {
			res.ExitCode = 137 // killed by signal
		}
		return res
	}
	res.ExitCode = -1
	res.Err = err
	return res
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/worker/ -race -v`
Expected: all four tests PASS, including the process-tree kill.

- [ ] **Step 5: Commit**

```bash
git add internal/worker/exec.go internal/worker/exec_test.go
git commit -m "feat: worker spawns jobs in their own process group and kills the whole tree"
```

---

### Task 8: Worker loops — register, heartbeat, poll, report

**Files:**
- Create: `internal/worker/config.go`, `internal/worker/worker.go`
- Test: `internal/worker/worker_test.go`

**Interfaces:**
- Consumes: `worker.Run` from Task 7; the server wire types from Task 5.
- Produces: `worker.Config{ControllerURL, Token, Host string, Devices []string, HeartbeatInterval, PollWait time.Duration}`; `worker.LoadConfig(path string) (worker.Config, error)` reading YAML; `worker.New(cfg worker.Config) *worker.Worker`; `(*Worker).Start(ctx context.Context) error` which registers then runs the heartbeat and poll loops until ctx ends.

- [ ] **Step 1: Write the failing worker loop test**

Create `internal/worker/worker_test.go`:

```go
package worker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestWorkerRegistersRunsAssignmentAndReportsResult(t *testing.T) {
	var (
		mu        sync.Mutex
		logs      []byte
		states    []string
		exitCodes []int
		served    bool
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/workers/register":
			json.NewEncoder(w).Encode(map[string]string{"worker_id": "w1"})

		case r.URL.Path == "/v1/workers/w1/assignments":
			mu.Lock()
			first := !served
			served = true
			mu.Unlock()
			if !first {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			json.NewEncoder(w).Encode([]map[string]any{{
				"job_id":    "job1",
				"device_id": "gpubox:gpu0",
				"command":   []string{"sh", "-c", "echo hello; exit 7"},
			}})

		case r.URL.Path == "/v1/workers/w1/heartbeat":
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/logs":
			buf := make([]byte, 4096)
			n, _ := r.Body.Read(buf)
			mu.Lock()
			logs = append(logs, buf[:n]...)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)

		case r.URL.Path == "/v1/jobs/job1/status":
			var body struct {
				State    string `json:"state"`
				ExitCode *int   `json:"exit_code"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			states = append(states, body.State)
			if body.ExitCode != nil {
				exitCodes = append(exitCodes, *body.ExitCode)
			}
			mu.Unlock()
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL:     ts.URL,
		Token:             "wtok",
		Host:              "gpubox",
		Devices:           []string{"gpu0"},
		HeartbeatInterval: 50 * time.Millisecond,
		PollWait:          100 * time.Millisecond,
	})

	go func() { _ = wk.Start(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(exitCodes) == 1 && string(logs) != ""
	}, 10*time.Second, 50*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, string(logs), "hello")
	require.Equal(t, []int{7}, exitCodes)
	require.Equal(t, []string{"running", "failed"}, states)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/worker/ -run TestWorkerRegisters -v`
Expected: FAIL — `worker.New` undefined.

- [ ] **Step 3: Implement the config**

Create `internal/worker/config.go`:

```go
package worker

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ControllerURL     string        `yaml:"controller_url"`
	Token             string        `yaml:"token"`
	Host              string        `yaml:"host"`
	Devices           []string      `yaml:"devices"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	PollWait          time.Duration `yaml:"poll_wait"`
}

// LoadConfig reads /etc/rc/worker.yaml (or another path) and applies defaults.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read worker config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return Config{}, fmt.Errorf("parse worker config: %w", err)
	}
	if c.Host == "" {
		h, err := os.Hostname()
		if err != nil {
			return Config{}, err
		}
		c.Host = h
	}
	if c.ControllerURL == "" {
		return Config{}, fmt.Errorf("controller_url required in %s", path)
	}
	if len(c.Devices) == 0 {
		return Config{}, fmt.Errorf("at least one device required in %s", path)
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.PollWait <= 0 {
		c.PollWait = 30 * time.Second
	}
	return c, nil
}
```

Add the dependency: `go get gopkg.in/yaml.v3@v3.0.1`

- [ ] **Step 4: Implement the worker loops**

Create `internal/worker/worker.go`:

```go
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type assignment struct {
	JobID    string            `json:"job_id"`
	DeviceID string            `json:"device_id"`
	Command  []string          `json:"command"`
	Cwd      string            `json:"cwd"`
	Env      map[string]string `json:"env"`
}

type Worker struct {
	cfg      Config
	http     *http.Client
	workerID string

	mu      sync.Mutex
	running map[string]context.CancelFunc
}

func New(cfg Config) *Worker {
	return &Worker{
		cfg:     cfg,
		http:    &http.Client{Timeout: 2 * time.Minute},
		running: map[string]context.CancelFunc{},
	}
}

func (w *Worker) Start(ctx context.Context) error {
	if err := w.register(ctx); err != nil {
		return err
	}
	go w.heartbeatLoop(ctx)
	w.pollLoop(ctx)
	return ctx.Err()
}

func (w *Worker) do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, w.cfg.ControllerURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+w.cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	return w.http.Do(req)
}

func (w *Worker) register(ctx context.Context) error {
	payload, err := json.Marshal(map[string]any{"host": w.cfg.Host, "devices": w.cfg.Devices})
	if err != nil {
		return err
	}
	resp, err := w.do(ctx, http.MethodPost, "/v1/workers/register", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("register: controller returned %s", resp.Status)
	}
	var out struct {
		WorkerID string `json:"worker_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	w.workerID = out.WorkerID
	slog.Info("registered", "worker_id", w.workerID, "host", w.cfg.Host, "devices", w.cfg.Devices)
	return nil
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			resp, err := w.do(ctx, http.MethodPost, "/v1/workers/"+w.workerID+"/heartbeat", nil)
			if err != nil {
				slog.Warn("heartbeat failed", "err", err)
				continue
			}
			resp.Body.Close()
		}
	}
}

// pollLoop long-polls for work. A failed poll is retried: the controller being
// down must never abandon jobs already running here.
func (w *Worker) pollLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		assignments, err := w.poll(ctx)
		if err != nil {
			slog.Warn("poll failed", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		for _, a := range assignments {
			go w.execute(ctx, a)
		}
	}
}

func (w *Worker) poll(ctx context.Context) ([]assignment, error) {
	path := fmt.Sprintf("/v1/workers/%s/assignments?wait=%s", w.workerID, w.cfg.PollWait)
	resp, err := w.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("controller returned %s", resp.Status)
	}
	var out []assignment
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (w *Worker) execute(ctx context.Context, a assignment) {
	w.mu.Lock()
	if _, busy := w.running[a.JobID]; busy {
		w.mu.Unlock()
		return // a duplicate poll result must not start the job twice
	}
	jobCtx, cancel := context.WithCancel(ctx)
	w.running[a.JobID] = cancel
	w.mu.Unlock()

	defer func() {
		cancel()
		w.mu.Lock()
		delete(w.running, a.JobID)
		w.mu.Unlock()
	}()

	w.report(ctx, a.JobID, map[string]any{"state": "running"})

	env := map[string]string{}
	for k, v := range a.Env {
		env[k] = v
	}
	env["RC_JOB_ID"] = a.JobID
	env["RC_DEVICE"] = a.DeviceID

	sink := &logSink{w: w, jobID: a.JobID, ctx: ctx}
	res := Run(jobCtx, JobSpec{Command: a.Command, Cwd: a.Cwd, Env: env}, sink)
	sink.Flush()

	state := "succeeded"
	switch {
	case res.Killed:
		state = "killed"
	case res.Err != nil || res.ExitCode != 0:
		state = "failed"
	}
	body := map[string]any{"state": state, "exit_code": res.ExitCode}
	if res.Reason != "" {
		body["reason"] = res.Reason
	}
	if res.Err != nil {
		body["reason"] = res.Err.Error()
	}
	w.report(ctx, a.JobID, body)
}

func (w *Worker) report(ctx context.Context, jobID string, body map[string]any) {
	payload, err := json.Marshal(body)
	if err != nil {
		slog.Error("marshal status", "err", err)
		return
	}
	resp, err := w.do(ctx, http.MethodPost, "/v1/jobs/"+jobID+"/status", bytes.NewReader(payload))
	if err != nil {
		slog.Error("report status", "job", jobID, "err", err)
		return
	}
	resp.Body.Close()
}

// logSink batches output and ships it to the controller, flushing on size or
// time so a live `rc run` sees output while the job is still going.
type logSink struct {
	w     *Worker
	jobID string
	ctx   context.Context

	mu  sync.Mutex
	buf bytes.Buffer
	t   *time.Timer
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.buf.Write(p)
	full := s.buf.Len() >= 64*1024
	if s.t == nil {
		s.t = time.AfterFunc(time.Second, s.Flush)
	}
	s.mu.Unlock()

	if full {
		s.Flush()
	}
	return len(p), nil
}

func (s *logSink) Flush() {
	s.mu.Lock()
	if s.t != nil {
		s.t.Stop()
		s.t = nil
	}
	if s.buf.Len() == 0 {
		s.mu.Unlock()
		return
	}
	chunk := make([]byte, s.buf.Len())
	copy(chunk, s.buf.Bytes())
	s.buf.Reset()
	s.mu.Unlock()

	resp, err := s.w.do(s.ctx, http.MethodPost, "/v1/jobs/"+s.jobID+"/logs", bytes.NewReader(chunk))
	if err != nil {
		slog.Warn("ship logs", "job", s.jobID, "err", err)
		return
	}
	resp.Body.Close()
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/worker/ -race -v`
Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/worker/ go.mod go.sum
git commit -m "feat: worker registration, heartbeat, assignment polling, and log shipping"
```

---

### Task 9: Client library and `rc run`

**Files:**
- Create: `internal/client/client.go`, `internal/cli/run.go`
- Test: `internal/client/client_test.go`

**Interfaces:**
- Consumes: server wire types from Tasks 5 and 6.
- Produces: `client.New(baseURL, token string) *client.Client`; `(*Client).Submit(ctx, client.SubmitOptions) (*model.Job, error)` returning `client.ErrNoDevice` on 409; `(*Client).Job(ctx, id string) (*model.Job, error)`; `(*Client).StreamLogs(ctx, id string, out io.Writer) error`; `(*Client).State(ctx) (*server.StateResponse, error)`; `client.SubmitOptions{DeviceID string, Command []string, Cwd string, Env map[string]string, Submitter string, IdempotencyKey string}`; `cli.NewRunCmd() *cobra.Command`.

- [ ] **Step 1: Write the failing client test**

Create `internal/client/client_test.go`:

```go
package client_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/stretchr/testify/require"
)

func TestSubmitReturnsErrNoDeviceOn409(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "no_device_available", "message": "gpubox:gpu0 is not free",
		})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	_, err := c.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "gpubox:gpu0", Command: []string{"true"},
	})
	require.ErrorIs(t, err, client.ErrNoDevice)
}

func TestSubmitSendsBearerToken(t *testing.T) {
	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"id": "job1", "state": "assigned"})
	}))
	defer ts.Close()

	c := client.New(ts.URL, "ctok")
	job, err := c.Submit(context.Background(), client.SubmitOptions{
		DeviceID: "gpubox:gpu0", Command: []string{"true"},
	})
	require.NoError(t, err)
	require.Equal(t, "job1", job.ID)
	require.Equal(t, "Bearer ctok", gotAuth)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/client/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement the client**

Create `internal/client/client.go`:

```go
// Package client is the typed HTTP client shared by every CLI verb.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
)

// ErrNoDevice mirrors the controller's 409: nothing free right now. Stage 1
// has no queue, so the caller decides whether to retry.
var ErrNoDevice = errors.New("no device available")

type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

func New(baseURL, token string) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		// No global timeout: log streaming runs as long as the job does.
		http: &http.Client{},
	}
}

type SubmitOptions struct {
	DeviceID       string
	Command        []string
	Cwd            string
	Env            map[string]string
	Submitter      string
	IdempotencyKey string
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	return c.http.Do(req)
}

func (c *Client) Submit(ctx context.Context, opts SubmitOptions) (*model.Job, error) {
	payload, err := json.Marshal(server.SubmitRequest{
		DeviceID: opts.DeviceID, Command: opts.Command, Cwd: opts.Cwd, Env: opts.Env,
		Submitter: opts.Submitter, IdempotencyKey: opts.IdempotencyKey,
	})
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, http.MethodPost, "/v1/jobs", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return nil, ErrNoDevice
	}
	if resp.StatusCode != http.StatusCreated {
		return nil, apiError(resp)
	}
	var job model.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (c *Client) Job(ctx context.Context, id string) (*model.Job, error) {
	resp, err := c.do(ctx, http.MethodGet, "/v1/jobs/"+id, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError(resp)
	}
	var job model.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

// StreamLogs copies the job's output to out until the job finishes.
func (c *Client) StreamLogs(ctx context.Context, id string, out io.Writer) error {
	resp, err := c.do(ctx, http.MethodGet, "/v1/jobs/"+id+"/logs", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return apiError(resp)
	}
	_, err = io.Copy(out, resp.Body)
	return err
}

func (c *Client) State(ctx context.Context) (*server.StateResponse, error) {
	resp, err := c.do(ctx, http.MethodGet, "/v1/state", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError(resp)
	}
	var state server.StateResponse
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return nil, err
	}
	return &state, nil
}

// WaitTerminal polls until the job reaches a terminal state.
func (c *Client) WaitTerminal(ctx context.Context, id string) (*model.Job, error) {
	for {
		job, err := c.Job(ctx, id)
		if err != nil {
			return nil, err
		}
		if job.State.Terminal() {
			return job, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func apiError(resp *http.Response) error {
	var body struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Error != "" {
		return fmt.Errorf("%s: %s", body.Error, body.Message)
	}
	return fmt.Errorf("controller returned %s", resp.Status)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/client/ -race -v`
Expected: both tests PASS.

- [ ] **Step 5: Implement `rc run`**

Create `internal/cli/run.go`:

```go
// Package cli holds the cobra commands.
package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/mudler/resource-controller/internal/client"
	"github.com/spf13/cobra"
)

// exitCodeError carries a job's exit status up to main so `rc run` exits with
// the code the job produced, keeping it drop-in for scripts and CI.
type exitCodeError struct{ code int }

func (e exitCodeError) Error() string { return fmt.Sprintf("job exited %d", e.code) }
func (e exitCodeError) ExitCode() int { return e.code }

func defaultSubmitter() string {
	user := os.Getenv("USER")
	host, _ := os.Hostname()
	if session := os.Getenv("CLAUDE_SESSION_ID"); session != "" {
		return fmt.Sprintf("%s@%s/%s", user, host, session)
	}
	return fmt.Sprintf("%s@%s", user, host)
}

func NewRunCmd() *cobra.Command {
	var (
		device string
		cwd    string
		as     string
	)

	cmd := &cobra.Command{
		Use:   "run -d <device> -- <command>...",
		Short: "Claim a device, run a command on it, stream the output",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if device == "" {
				return errors.New("-d/--device is required in stage 1")
			}
			if as == "" {
				as = defaultSubmitter()
			}

			c := client.New(controllerURL(), controllerToken())

			// Cancelling the client cancels the job, matching `flock -c`.
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			job, err := c.Submit(ctx, client.SubmitOptions{
				DeviceID:       device,
				Command:        args,
				Cwd:            cwd,
				Submitter:      as,
				IdempotencyKey: uuid.NewString(),
			})
			if errors.Is(err, client.ErrNoDevice) {
				return fmt.Errorf("%s is busy (stage 1 has no queue — retry or pick another device)", device)
			}
			if err != nil {
				return err
			}

			fmt.Fprintf(os.Stderr, "rc: job %s on %s\n", job.ID, job.DeviceID)

			if err := c.StreamLogs(ctx, job.ID, os.Stdout); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "rc: log stream ended: %v\n", err)
			}

			// Use a fresh context: the job's final state is still worth
			// reporting even when the user just pressed Ctrl-C.
			final, err := c.WaitTerminal(context.Background(), job.ID)
			if err != nil {
				return err
			}
			if final.ExitCode != nil && *final.ExitCode != 0 {
				return exitCodeError{code: *final.ExitCode}
			}
			if final.State != "succeeded" {
				return fmt.Errorf("job %s: %s", final.State, final.KillReason)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&device, "device", "d", "", "device ID, e.g. gpubox:gpu0")
	cmd.Flags().StringVar(&cwd, "cwd", "", "working directory on the device host")
	cmd.Flags().StringVar(&as, "as", "", "identity shown in rc ps (defaults to user@host/session)")
	return cmd
}

func controllerURL() string {
	if v := os.Getenv("RC_CONTROLLER"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func controllerToken() string { return os.Getenv("RC_TOKEN") }
```

- [ ] **Step 6: Propagate the job's exit code from main**

Replace the body of `main()` in `main.go`:

```go
func main() {
	root := &cobra.Command{
		Use:           "rc",
		Short:         "Resource controller: exclusive device leases for shared hardware",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(cli.NewRunCmd())

	if err := root.Execute(); err != nil {
		var coded interface{ ExitCode() int }
		if errors.As(err, &coded) {
			os.Exit(coded.ExitCode())
		}
		fmt.Fprintln(os.Stderr, "rc:", err)
		os.Exit(1)
	}
}
```

Imports become `errors`, `fmt`, `os`, `github.com/mudler/resource-controller/internal/cli`, `github.com/spf13/cobra`.

- [ ] **Step 7: Verify the build**

Run: `go build ./... && go vet ./...`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add internal/client/ internal/cli/ main.go
git commit -m "feat: rc run claims a device, streams output, and exits with the job's code"
```

---

### Task 10: `rc ps`, `rc devices`, `rc serve`, `rc worker`

**Files:**
- Create: `internal/cli/ps.go`, `internal/cli/serve.go`, `internal/cli/worker.go`
- Modify: `main.go`
- Test: `internal/cli/ps_test.go`

**Interfaces:**
- Consumes: `client.Client` from Task 9; `server.New` from Task 5; `worker.New`/`worker.LoadConfig` from Task 8; `store.Sweep` from Task 3.
- Produces: `cli.NewPsCmd()`, `cli.NewDevicesCmd()`, `cli.NewServeCmd()`, `cli.NewWorkerCmd()` all `*cobra.Command`; `cli.RenderDevices(w io.Writer, views []server.DeviceView)`.

- [ ] **Step 1: Write the failing render test**

Create `internal/cli/ps_test.go`:

```go
package cli_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/cli"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/stretchr/testify/require"
)

func TestRenderDevicesShowsHolderElapsedAndStaleness(t *testing.T) {
	var out bytes.Buffer

	cli.RenderDevices(&out, []server.DeviceView{
		{
			Device:              model.Device{ID: "gpubox:gpu0", State: model.DeviceBusy, LastHeartbeatAt: time.Now()},
			Holder:              "mudler@laptop/sess-1",
			Command:             []string{"./bench", "--fast"},
			ElapsedSeconds:      125,
			HeartbeatAgeSeconds: 3,
		},
		{
			Device:              model.Device{ID: "gpubox:gpu1", State: model.DeviceUnknown},
			HeartbeatAgeSeconds: 47,
		},
	})

	got := out.String()
	require.Contains(t, got, "gpubox:gpu0")
	require.Contains(t, got, "busy")
	require.Contains(t, got, "mudler@laptop/sess-1")
	require.Contains(t, got, "2m5s")
	require.Contains(t, got, "./bench --fast")
	// The signal flock cannot express: this worker has gone quiet.
	require.Contains(t, got, "no contact 47s")
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/cli/ -v`
Expected: FAIL — `cli.RenderDevices` undefined.

- [ ] **Step 3: Implement the table and the list commands**

Create `internal/cli/ps.go`:

```go
package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/spf13/cobra"
)

// RenderDevices prints the fleet as a table. Heartbeat age is shown for any
// device that is not reporting, because a silent worker looks identical to a
// healthy one otherwise.
func RenderDevices(w io.Writer, views []server.DeviceView) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "DEVICE\tSTATE\tHOLDER\tELAPSED\tCOMMAND")

	for _, v := range views {
		state := string(v.Device.State)
		if v.Device.State != model.DeviceReady && v.Device.State != model.DeviceBusy {
			state = fmt.Sprintf("%s (no contact %s)", state,
				time.Duration(v.HeartbeatAgeSeconds)*time.Second)
		} else if v.HeartbeatAgeSeconds > 30 {
			state = fmt.Sprintf("%s (no contact %s)", state,
				time.Duration(v.HeartbeatAgeSeconds)*time.Second)
		}

		holder, elapsed, command := "-", "-", "-"
		if v.Holder != "" {
			holder = v.Holder
			elapsed = (time.Duration(v.ElapsedSeconds) * time.Second).String()
		}
		if len(v.Command) > 0 {
			command = strings.Join(v.Command, " ")
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", v.Device.ID, state, holder, elapsed, command)
	}
	tw.Flush()
}

func NewDevicesCmd() *cobra.Command {
	var asJSON bool

	cmd := &cobra.Command{
		Use:   "devices",
		Short: "List devices, their state, and who holds them",
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.New(controllerURL(), controllerToken())
			state, err := c.State(cmd.Context())
			if err != nil {
				return err
			}
			if asJSON {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(state.Devices)
			}
			RenderDevices(os.Stdout, state.Devices)
			return nil
		},
	}
	cmd.Flags().BoolVarP(&asJSON, "json", "o", false, "output JSON")
	return cmd
}

func NewPsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ps",
		Short: "Show running jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			c := client.New(controllerURL(), controllerToken())
			state, err := c.State(cmd.Context())
			if err != nil {
				return err
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "JOB\tDEVICE\tSTATE\tSUBMITTER\tCOMMAND")
			for _, j := range state.Jobs {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
					j.ID, j.DeviceID, j.State, j.Submitter, strings.Join(j.Command, " "))
			}
			return tw.Flush()
		},
	}
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./internal/cli/ -v`
Expected: PASS.

- [ ] **Step 5: Implement `rc serve`**

Create `internal/cli/serve.go`:

```go
package cli

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/spf13/cobra"
)

func NewServeCmd() *cobra.Command {
	var (
		addr    string
		dataDir string
	)

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the controller",
		RunE: func(cmd *cobra.Command, args []string) error {
			tokens, err := loadTokens()
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dataDir, 0o755); err != nil {
				return err
			}

			c := clock.Real()
			st, err := store.Open(filepath.Join(dataDir, "rc.db"), c)
			if err != nil {
				return err
			}
			defer st.Close()

			logs, err := logstore.New(filepath.Join(dataDir, "logs"))
			if err != nil {
				return err
			}

			srv := server.New(server.Config{Store: st, Logs: logs, Clock: c, Tokens: tokens})

			// The reaper: silent workers lose their devices to unknown, then
			// unhealthy. Nothing here ever promotes a device to ready.
			go func() {
				t := time.NewTicker(10 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-cmd.Context().Done():
						return
					case <-t.C:
						res, err := st.Sweep(30*time.Second, 5*time.Minute)
						if err != nil {
							slog.Error("sweep", "err", err)
							continue
						}
						if len(res.DevicesUnhealthy) > 0 || len(res.JobsLost) > 0 {
							slog.Warn("devices demoted",
								"unhealthy", res.DevicesUnhealthy, "jobs_lost", res.JobsLost)
						}
					}
				}
			}()

			httpSrv := &http.Server{Addr: addr, Handler: srv.Handler()}
			go func() {
				<-cmd.Context().Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = httpSrv.Shutdown(shutdownCtx)
			}()

			slog.Info("controller listening", "addr", addr, "data", dataDir)
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&addr, "addr", ":8080", "listen address")
	cmd.Flags().StringVar(&dataDir, "data", "/var/lib/rc", "state directory")
	return cmd
}

// loadTokens reads RC_TOKENS as "token:role,token:role".
func loadTokens() (map[string]string, error) {
	raw := os.Getenv("RC_TOKENS")
	if raw == "" {
		return nil, fmt.Errorf("RC_TOKENS required, e.g. RC_TOKENS='wtok:worker,ctok:client,atok:admin'")
	}
	tokens := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		token, role, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if !ok {
			return nil, fmt.Errorf("malformed RC_TOKENS entry %q", pair)
		}
		switch role {
		case "worker", "client", "admin":
		default:
			return nil, fmt.Errorf("unknown role %q in RC_TOKENS", role)
		}
		tokens[token] = role
	}
	return tokens, nil
}
```

- [ ] **Step 6: Implement `rc worker`**

Create `internal/cli/worker.go`:

```go
package cli

import (
	"github.com/mudler/resource-controller/internal/worker"
	"github.com/spf13/cobra"
)

func NewWorkerCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Run the device-host agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := worker.LoadConfig(configPath)
			if err != nil {
				return err
			}
			return worker.New(cfg).Start(cmd.Context())
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "/etc/rc/worker.yaml", "worker config file")
	return cmd
}
```

- [ ] **Step 7: Register the commands and handle signals in main**

In `main.go`, add to the root command:

```go
	root.AddCommand(cli.NewRunCmd(), cli.NewPsCmd(), cli.NewDevicesCmd(),
		cli.NewServeCmd(), cli.NewWorkerCmd())
```

and replace `root.Execute()` with a signal-aware execution:

```go
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := root.ExecuteContext(ctx); err != nil {
```

Add `context`, `os/signal`, and `syscall` to the imports.

- [ ] **Step 8: Verify the build and full test suite**

Run: `go build ./... && go vet ./... && go test ./... -race`
Expected: clean build, all tests PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/cli/ main.go
git commit -m "feat: rc ps, rc devices, rc serve with reaper, and rc worker"
```

---

### Task 11: End-to-end test and the operator README

**Files:**
- Create: `e2e/e2e_test.go`, `README.md`, `examples/worker.yaml`
- Test: `e2e/e2e_test.go`

**Interfaces:**
- Consumes: everything.
- Produces: no new exported API.

- [ ] **Step 1: Write the end-to-end test**

Create `e2e/e2e_test.go`:

```go
package e2e_test

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/resource-controller/internal/client"
	"github.com/mudler/resource-controller/internal/clock"
	"github.com/mudler/resource-controller/internal/logstore"
	"github.com/mudler/resource-controller/internal/model"
	"github.com/mudler/resource-controller/internal/server"
	"github.com/mudler/resource-controller/internal/store"
	"github.com/mudler/resource-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

// Real controller, real worker, one fake device: submit -> run -> release,
// then prove the device is reusable and that a second claim is refused while
// the first is live.
func TestEndToEndClaimRunRelease(t *testing.T) {
	dir := t.TempDir()
	c := clock.Real()

	st, err := store.Open(filepath.Join(dir, "rc.db"), c)
	require.NoError(t, err)
	defer st.Close()

	logs, err := logstore.New(filepath.Join(dir, "logs"))
	require.NoError(t, err)

	srv := server.New(server.Config{
		Store: st, Logs: logs, Clock: c,
		Tokens: map[string]string{"wtok": "worker", "ctok": "client"},
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wk := worker.New(worker.Config{
		ControllerURL: ts.URL, Token: "wtok", Host: "testbox", Devices: []string{"dev0"},
		HeartbeatInterval: time.Second, PollWait: time.Second,
	})
	go func() { _ = wk.Start(ctx) }()

	cl := client.New(ts.URL, "ctok")

	require.Eventually(t, func() bool {
		state, err := cl.State(ctx)
		return err == nil && len(state.Devices) == 1
	}, 15*time.Second, 100*time.Millisecond, "worker never registered")

	job, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID:  "testbox:dev0",
		Command:   []string{"sh", "-c", "echo hello-from-device; sleep 1"},
		Submitter: "agent-a",
	})
	require.NoError(t, err)

	// While it holds the device, a second claim must be refused.
	_, err = cl.Submit(ctx, client.SubmitOptions{
		DeviceID: "testbox:dev0", Command: []string{"true"}, Submitter: "agent-b",
	})
	require.ErrorIs(t, err, client.ErrNoDevice)

	var out logBuffer
	require.NoError(t, cl.StreamLogs(ctx, job.ID, &out))
	require.Contains(t, out.String(), "hello-from-device")

	final, err := cl.WaitTerminal(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, model.JobSucceeded, final.State)
	require.NotNil(t, final.ExitCode)
	require.Equal(t, 0, *final.ExitCode)

	// The device must come back to the pool.
	require.Eventually(t, func() bool {
		state, err := cl.State(ctx)
		return err == nil && state.Devices[0].Device.State == model.DeviceReady
	}, 15*time.Second, 100*time.Millisecond)

	again, err := cl.Submit(ctx, client.SubmitOptions{
		DeviceID: "testbox:dev0", Command: []string{"true"}, Submitter: "agent-b",
	})
	require.NoError(t, err)
	require.NotEqual(t, job.ID, again.ID)

	_ = os.Remove(filepath.Join(dir, "unused"))
}

type logBuffer struct{ b []byte }

func (l *logBuffer) Write(p []byte) (int, error) {
	l.b = append(l.b, p...)
	return len(p), nil
}

func (l *logBuffer) String() string { return string(l.b) }
```

- [ ] **Step 2: Run it**

Run: `go test ./e2e/ -race -v -timeout 120s`
Expected: PASS.

- [ ] **Step 3: Write the example worker config**

Create `examples/worker.yaml`:

```yaml
controller_url: https://rc.internal.example
token: replace-with-worker-token
# host defaults to the machine's hostname; device IDs become <host>:<name>
devices:
  - gpu0
  - gpu1
heartbeat_interval: 10s
poll_wait: 30s
```

- [ ] **Step 4: Write the README**

Create `README.md`:

````markdown
# resource-controller

Exclusive leases for shared hardware, across hosts, with the state visible.

Replaces a `flock` file mutex: a central controller owns allocation in a single
SQLite transaction, workers on device hosts supervise the jobs, and `rc ps`
shows who holds what.

## Stage 1 scope

Exclusive device leases, supervised job execution, live log streaming, and
fleet visibility. **There is no queue yet** — a busy device is refused
immediately with `no_device_available`.

## Controller

```sh
export RC_TOKENS='wtok:worker,ctok:client,atok:admin'
rc serve --addr :8080 --data /var/lib/rc
```

## Device host

Write `/etc/rc/worker.yaml` (see `examples/worker.yaml`), then:

```sh
rc worker
```

## Client

```sh
export RC_CONTROLLER=https://rc.internal.example
export RC_TOKEN=ctok

rc devices                              # who holds what, and what has gone quiet
rc ps                                   # running jobs
rc run -d gpubox:gpu0 --cwd /src -- ./bench --args
```

`rc run` blocks, streams the job's output, and exits with the job's exit code,
so it drops into scripts wherever `flock /tmp/gpu -c '...'` used to sit.

## Guarantees

- One live lease per device, enforced by a unique index in SQLite — not by
  convention and not by a file in `/tmp`.
- A disconnected client does not release the device; the worker owns the
  process.
- A worker that stops reporting has its devices marked `unknown`, then
  `unhealthy`. They are never returned to the pool on silence alone; clear them
  with `rc devices` plus an admin `POST /v1/devices/{id}/clear`.
- Jobs run in their own process group, so a kill takes the whole tree with it.
````

- [ ] **Step 5: Run everything**

Run: `go build ./... && go vet ./... && go test ./... -race -timeout 180s`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add e2e/ README.md examples/
git commit -m "test: end-to-end claim/run/release; docs: operator README"
```

---

## Self-Review Notes

Checked against `docs/superpowers/specs/2026-08-13-resource-controller-design.md`:

**Covered by this plan:** controller with SQLite as sole allocation authority (Task 2), outbound-dialing worker over plain HTTP with long-poll (Tasks 5, 8), worker-owned leases surviving client disconnect (Tasks 8, 9), worker-loss demotion that never returns unproven devices to the pool (Task 3), process-group kill of the whole tree (Task 7), `RC_JOB_ID`/`RC_DEVICE` job environment (Task 8), idempotent submit (Task 2), token roles (Task 5), staleness as a visible signal (Tasks 6, 10), `rc run` exit-code passthrough (Task 9), no check-then-act endpoint (Task 5 routes).

**Deliberately deferred, per the staging section of the spec:**

- Queue, priorities, reservations — Stage 2. Stage 1 returns `409 no_device_available`.
- Watchdogs (`max_runtime`, `idle_timeout`), `rc kill`, `rc attach` — Stage 2. Task 7 already spawns in a cancellable process group, so watchdogs attach to existing machinery.
- Label probes, selectors, usage sheets, `rc describe`, `rc hold` — Stage 3. `AllocateRequest.DeviceID` becomes a selector there; the `jobs.selector` column already exists.
- Web dashboard, SSE `/v1/events`, verify probes, notifications — Stage 4. `/v1/state` already returns everything the first dashboard needs.
- `CUDA_VISIBLE_DEVICES` is honored by `worker.Run` (Task 7 test) but not yet derived from the device name; that derivation lands with device labels in Stage 3.
