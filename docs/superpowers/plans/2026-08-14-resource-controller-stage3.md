# Resource Controller Stage 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the fleet describe itself — devices carry labels the scheduler can select on, hosts carry an operating manual, and an agent can ask what a box is before writing commands for it.

**Architecture:** Workers gather facts from built-in probes and drop-in scripts and push them at registration; the controller stores each label with its provenance and age. Job submission gains a selector that queues against the *set* of matching devices, reusing Stage 2's queue rather than replacing it. `rc hold` takes the same lease a job would, so interactive work is visible and expires.

**Tech Stack:** Go 1.26, `modernc.org/sqlite` (no cgo), `spf13/cobra`, `stretchr/testify`, stdlib `net/http`, `gopkg.in/yaml.v3`.

**Spec:** `docs/superpowers/specs/2026-08-13-resource-controller-stages-2-4-design.md` (Stage 3 section)

## Global Constraints

- Go 1.26, no cgo. Module `github.com/mudler/agents-resources-controller`.
- SQLite at `MaxOpenConns(1)`. Any query that iterates rows and then issues another query MUST drain its rows to completion first, or it deadlocks.
- **Migrations are append-only.** The runner tracks `PRAGMA user_version`; editing an applied entry means deployed databases never receive the change. There is a live controller with a real database.
- Controller-side time goes through the `Clock` interface; store and server tests never sleep. Worker and e2e tests use real time with `require.Eventually`.
- **The allocation transaction is not to be restructured.** `assignQueued` flips `device: ready → busy`, updates the job and inserts the lease in one transaction, with the partial unique index `leases_one_live_per_device` as the last-resort guard. Selectors change which device is *chosen*, never how it is claimed.
- **No "is this device free?" endpoint.** A caller queues or it does not; nothing may compose into check-then-act.
- A probe failure must never prevent a worker from starting or registering. A wedged `nvidia-smi` is a missing label, not an outage.
- Declared labels never overwrite a detected value for the same key.

## Decisions this plan makes

- **Hooks fire for `rc hold`.** A hold exists so you can use the device yourself, which is exactly when the node's inference server must stand down. The acquire hook runs before the hold is granted and its failure refuses the hold; the release hook runs when the hold ends, through the same linger machinery jobs use. *Cost if wrong: taking a hold restarts a service you wanted left alone — visible immediately and reversible by removing the hook.*
- **Selector matching happens in Go over a label snapshot**, not in SQL. Fleets here are tens of devices; a snapshot keeps the matching logic unit-testable without a database and keeps `MaxOpenConns(1)` out of the hot path. *Cost if wrong: matching is O(devices × terms) per scheduling pass, which is nothing at this scale and would need revisiting at thousands.*

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/store/migrate.go` | Append two migrations: labels/docs tables, lease kind+reason. |
| `internal/store/labels.go` | Label upsert with provenance, snapshot read. New. |
| `internal/store/docs.go` | Usage-sheet upsert and read. New. |
| `internal/selector/selector.go` | Selector parsing and matching. New, dependency-free so it unit-tests without a store. |
| `internal/store/queue.go` | `EnqueueRequest` gains `Selector`; `ScheduleOnce` resolves it to candidate devices. |
| `internal/store/hold.go` | `AcquireHold`, `ReleaseHold`. New. |
| `internal/worker/probe.go` | Built-in probes and `probe.d` runner. New. |
| `internal/worker/sheet.go` | Reads `host.md` and `host.d/<device>.md`. New. |
| `internal/worker/worker.go` | Sends labels and sheets at registration and on the probe interval. |
| `internal/server/worker_api.go` | Registration accepts labels and sheets. |
| `internal/server/client_api.go` | Submit accepts a selector; describe endpoint; hold routes. |
| `internal/cli/describe.go` | `rc describe`. New. |
| `internal/cli/hold.go` | `rc hold`, `rc release`. New. |
| `internal/cli/run.go` | `--select` and `--explain`. |
| `internal/server/dashboard/index.html` | Labels on the device card. |
| `e2e/selector_test.go`, `e2e/hold_test.go` | End-to-end coverage. New. |

---

### Task 1: Schema and models for labels, sheets, and hold leases

**Files:**
- Modify: `internal/store/migrate.go`, `internal/model/model.go`
- Test: `internal/store/migrate_test.go`

**Interfaces:**
- Consumes: the migration runner from Stage 2.
- Produces: `model.Label{Key, Value, Source string, UpdatedAt time.Time}`; `model.SourceDetected = "detected"` and `model.SourceDeclared = "declared"`; `model.LeaseKindJob = "job"` and `model.LeaseKindHold = "hold"`; `model.Lease` gains `Kind string` and `Reason string`; `model.Device` gains `Labels []Label` (populated by the label snapshot, not by `Devices()`).

- [ ] **Step 1: Extend the migration test**

In `internal/store/migrate_test.go`, add to the existing column-presence table in `TestOpenMigratesAStageOneDatabase`:

```go
		{"leases", "kind"},
		{"leases", "reason"},
```

and add a new test asserting the two new tables exist:

```go
func TestStage3TablesArePresent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rc.db")
	c := clock.NewFake(time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
	s, err := store.Open(path, c)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	check, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer check.Close()

	for _, table := range []string{"device_labels", "host_docs"} {
		var n int
		require.NoError(t, check.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n))
		require.Equal(t, 1, n, "%s missing", table)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/store/ -run 'Migrat|Stage3' -v`
Expected: FAIL — `device_labels` missing.

- [ ] **Step 3: Append the migrations**

Append to `migrations` in `internal/store/migrate.go`. **Do not edit any existing entry.**

```go
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
```

The `(device_id, key, source)` primary key is deliberate: a declared and a
detected value for the same key coexist so the conflict stays visible, which
is what the spec requires.

- [ ] **Step 4: Extend the models**

In `internal/model/model.go`:

```go
const (
	SourceDetected = "detected"
	SourceDeclared = "declared"
)

const (
	LeaseKindJob  = "job"
	LeaseKindHold = "hold"
)

// Label is one fact about a device, with where it came from and when it was
// last confirmed. Provenance matters because a hand-written value that
// survives a hardware change is worse than no value at all.
type Label struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Source    string    `json:"source"`
	UpdatedAt time.Time `json:"updated_at"`
}
```

Add `Kind string \`json:"kind"\`` and `Reason string \`json:"reason,omitempty"\`` to `Lease`, and `Labels []Label \`json:"labels,omitempty"\`` to `Device`.

- [ ] **Step 5: Verify and commit**

Run: `go test ./internal/store/ -race && go build ./... && go vet ./...`

```bash
git add internal/store/migrate.go internal/store/migrate_test.go internal/model/model.go
git commit -m "feat: schema for device labels, usage sheets, and hold leases"
```

---

### Task 2: Selector parsing and matching

Dependency-free so it unit-tests without a database. This is the piece that lets an agent stop naming hosts.

**Files:**
- Create: `internal/selector/selector.go`
- Test: `internal/selector/selector_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `selector.Parse(s string) (selector.Selector, error)`; `selector.Selector` with method `Match(labels map[string]string) bool` and `String() string`; `selector.ErrEmpty`.

- [ ] **Step 1: Write the failing tests**

Create `internal/selector/selector_test.go`:

```go
package selector_test

import (
	"testing"

	"github.com/mudler/agents-resources-controller/internal/selector"
	"github.com/stretchr/testify/require"
)

func TestParseAndMatchEquality(t *testing.T) {
	sel, err := selector.Parse("vendor=nvidia")
	require.NoError(t, err)
	require.True(t, sel.Match(map[string]string{"vendor": "nvidia"}))
	require.False(t, sel.Match(map[string]string{"vendor": "amd"}))
	require.False(t, sel.Match(map[string]string{}), "a missing key never matches")
}

func TestParseAndMatchInequality(t *testing.T) {
	sel, err := selector.Parse("vendor!=amd")
	require.NoError(t, err)
	require.True(t, sel.Match(map[string]string{"vendor": "nvidia"}))
	require.False(t, sel.Match(map[string]string{"vendor": "amd"}))
	require.False(t, sel.Match(map[string]string{}),
		"a missing key does not satisfy != either: we cannot prove it")
}

func TestNumericComparisonWithSuffixes(t *testing.T) {
	sel, err := selector.Parse("vram>=40G")
	require.NoError(t, err)
	require.True(t, sel.Match(map[string]string{"vram": "80G"}))
	require.True(t, sel.Match(map[string]string{"vram": "40G"}))
	require.False(t, sel.Match(map[string]string{"vram": "24G"}))
	require.True(t, sel.Match(map[string]string{"vram": "81920M"}))
}

func TestConjunction(t *testing.T) {
	sel, err := selector.Parse("vendor=nvidia,vram>=40G")
	require.NoError(t, err)
	require.True(t, sel.Match(map[string]string{"vendor": "nvidia", "vram": "80G"}))
	require.False(t, sel.Match(map[string]string{"vendor": "nvidia", "vram": "24G"}))
	require.False(t, sel.Match(map[string]string{"vram": "80G"}))
}

func TestStringComparisonWhenNotNumeric(t *testing.T) {
	sel, err := selector.Parse("model>=a100")
	require.NoError(t, err)
	require.True(t, sel.Match(map[string]string{"model": "h100"}))
	require.False(t, sel.Match(map[string]string{"model": "a10"}))
}

func TestParseRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "   ", "vendor", "=nvidia", "vendor=", ",", "a=b,,c=d"} {
		_, err := selector.Parse(in)
		require.Error(t, err, "input %q should not parse", in)
	}
}

func TestStringRoundTrips(t *testing.T) {
	sel, err := selector.Parse(" vendor = nvidia , vram >= 40G ")
	require.NoError(t, err)
	require.Equal(t, "vendor=nvidia,vram>=40G", sel.String(),
		"whitespace is normalised so an equivalent selector has one representation")
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/selector/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

Create `internal/selector/selector.go`:

```go
// Package selector parses and evaluates device selectors such as
// "vendor=nvidia,vram>=40G". It deliberately depends on nothing else in the
// project so it can be reasoned about and tested on its own.
package selector

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrEmpty = errors.New("selector is empty")

type op string

const (
	opEq  op = "="
	opNe  op = "!="
	opGte op = ">="
	opLte op = "<="
)

type term struct {
	key   string
	op    op
	value string
}

// Selector is a conjunction of terms: every term must match.
type Selector struct {
	terms []term
}

// Parse reads a comma-separated conjunction. Order is preserved so String()
// round-trips predictably.
func Parse(s string) (Selector, error) {
	var sel Selector
	if strings.TrimSpace(s) == "" {
		return sel, ErrEmpty
	}
	for _, raw := range strings.Split(s, ",") {
		part := strings.TrimSpace(raw)
		if part == "" {
			return Selector{}, fmt.Errorf("empty term in selector %q", s)
		}
		t, err := parseTerm(part)
		if err != nil {
			return Selector{}, err
		}
		sel.terms = append(sel.terms, t)
	}
	return sel, nil
}

// Longest operators first: ">=" must win over "=".
var operators = []op{opNe, opGte, opLte, opEq}

func parseTerm(part string) (term, error) {
	for _, o := range operators {
		key, value, found := strings.Cut(part, string(o))
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			return term{}, fmt.Errorf("term %q needs a key and a value", part)
		}
		return term{key: key, op: o, value: value}, nil
	}
	return term{}, fmt.Errorf("term %q has no operator (expected one of =, !=, >=, <=)", part)
}

func (s Selector) String() string {
	parts := make([]string, 0, len(s.terms))
	for _, t := range s.terms {
		parts = append(parts, t.key+string(t.op)+t.value)
	}
	return strings.Join(parts, ",")
}

// Match reports whether every term holds for these labels. A term whose key
// is absent never matches — including "!=", because an absent label is not
// proof that the device differs, and handing out a device on the strength of
// a fact we do not have is the mistake this system exists to avoid.
func (s Selector) Match(labels map[string]string) bool {
	for _, t := range s.terms {
		have, ok := labels[t.key]
		if !ok {
			return false
		}
		if !t.matches(have) {
			return false
		}
	}
	return true
}

func (t term) matches(have string) bool {
	if a, b, ok := numeric(have, t.value); ok {
		switch t.op {
		case opEq:
			return a == b
		case opNe:
			return a != b
		case opGte:
			return a >= b
		case opLte:
			return a <= b
		}
		return false
	}
	switch t.op {
	case opEq:
		return have == t.value
	case opNe:
		return have != t.value
	case opGte:
		return have >= t.value
	case opLte:
		return have <= t.value
	}
	return false
}

// numeric reports both sides as numbers when both parse, so "vram>=40G"
// compares 80G as larger and "81920M" as larger still.
func numeric(a, b string) (float64, float64, bool) {
	x, okA := parseQuantity(a)
	y, okB := parseQuantity(b)
	return x, y, okA && okB
}

var suffixes = map[byte]float64{
	'K': 1 << 10,
	'M': 1 << 20,
	'G': 1 << 30,
	'T': 1 << 40,
}

func parseQuantity(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	mult := 1.0
	last := s[len(s)-1]
	if m, ok := suffixes[last&^0x20]; ok { // accept upper and lower case
		mult = m
		s = s[:len(s)-1]
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, false
	}
	return v * mult, true
}
```

- [ ] **Step 4: Verify and commit**

Run: `go test ./internal/selector/ -race -v`
Expected: all PASS.

```bash
git add internal/selector/
git commit -m "feat: device selector parsing and matching"
```

---

### Task 3: Label storage with provenance

**Files:**
- Create: `internal/store/labels.go`
- Test: `internal/store/labels_test.go`

**Interfaces:**
- Consumes: Task 1's schema; `model.Label`, `model.SourceDetected`, `model.SourceDeclared`.
- Produces: `(*Store).ReplaceLabels(deviceID, source string, labels map[string]string, at time.Time) error`; `(*Store).LabelsFor(deviceID string) ([]model.Label, error)`; `(*Store).LabelSnapshot() (map[string]map[string]string, error)` returning device ID → effective labels, where a detected value wins over a declared one for the same key.

- [ ] **Step 1: Write the failing tests**

Create `internal/store/labels_test.go`:

```go
package store_test

import (
	"testing"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/stretchr/testify/require"
)

func TestReplaceLabelsStoresProvenanceAndTime(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia", "vram": "80G"}, c.Now()))

	labels, err := s.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Len(t, labels, 2)
	for _, l := range labels {
		require.Equal(t, model.SourceDetected, l.Source)
		require.Equal(t, c.Now(), l.UpdatedAt)
	}
}

// Replacing a source drops the keys that source no longer reports — a card
// that vanished must not leave its VRAM label behind.
func TestReplaceLabelsRemovesKeysThatSourceNoLongerReports(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia", "vram": "80G"}, c.Now()))
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia"}, c.Now()))

	labels, err := s.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Len(t, labels, 1)
	require.Equal(t, "vendor", labels[0].Key)
}

// The two sources coexist so the conflict stays visible in rc describe.
func TestDeclaredAndDetectedCoexist(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vram": "80G"}, c.Now()))
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDeclared,
		map[string]string{"vram": "40G"}, c.Now()))

	labels, err := s.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Len(t, labels, 2, "both rows survive")
}

// ...but the scheduler uses one value, and detection wins.
func TestSnapshotPrefersDetectedOverDeclared(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDeclared,
		map[string]string{"vram": "40G", "owner": "team-a"}, c.Now()))
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vram": "80G"}, c.Now()))

	snap, err := s.LabelSnapshot()
	require.NoError(t, err)
	require.Equal(t, "80G", snap["gpubox:gpu0"]["vram"],
		"a declared value must never override a detected one")
	require.Equal(t, "team-a", snap["gpubox:gpu0"]["owner"],
		"declared labels still contribute keys detection does not report")
}

func TestReplaceLabelsWithEmptyMapClearsThatSource(t *testing.T) {
	s, c := newStore(t)

	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia"}, c.Now()))
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{}, c.Now()))

	labels, err := s.LabelsFor("gpubox:gpu0")
	require.NoError(t, err)
	require.Empty(t, labels)
}

func TestLabelSnapshotCoversEveryDevice(t *testing.T) {
	s, c := newStore(t)
	require.NoError(t, s.ReplaceLabels("gpubox:gpu0", model.SourceDetected,
		map[string]string{"vendor": "nvidia"}, c.Now()))

	snap, err := s.LabelSnapshot()
	require.NoError(t, err)
	require.Contains(t, snap, "gpubox:gpu0")
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/store/ -run 'Label|Declared|Snapshot' -v`
Expected: FAIL — `s.ReplaceLabels undefined`.

- [ ] **Step 3: Implement**

Create `internal/store/labels.go`:

```go
package store

import (
	"fmt"
	"time"

	"github.com/mudler/agents-resources-controller/internal/model"
)

// ReplaceLabels makes the stored labels for one device and one source exactly
// the given set. Replacing rather than merging is deliberate: a probe that
// stops reporting a key means the fact is gone (a card was swapped, a driver
// downgraded), and a stale label is worse than a missing one.
func (s *Store) ReplaceLabels(deviceID, source string, labels map[string]string, at time.Time) error {
	if source != model.SourceDetected && source != model.SourceDeclared {
		return fmt.Errorf("unknown label source %q", source)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(
		`DELETE FROM device_labels WHERE device_id = ? AND source = ?`, deviceID, source); err != nil {
		return fmt.Errorf("clear labels: %w", err)
	}
	for k, v := range labels {
		if _, err := tx.Exec(
			`INSERT INTO device_labels (device_id, key, value, source, updated_at)
			 VALUES (?, ?, ?, ?, ?)`,
			deviceID, k, v, source, at.Unix()); err != nil {
			return fmt.Errorf("insert label %q: %w", k, err)
		}
	}
	return tx.Commit()
}

// LabelsFor returns every stored row for a device, both sources, so a caller
// can show the conflict rather than hide it.
func (s *Store) LabelsFor(deviceID string) ([]model.Label, error) {
	rows, err := s.db.Query(
		`SELECT key, value, source, updated_at FROM device_labels
		 WHERE device_id = ? ORDER BY key, source`, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.Label
	for rows.Next() {
		var l model.Label
		var updated int64
		if err := rows.Scan(&l.Key, &l.Value, &l.Source, &updated); err != nil {
			return nil, err
		}
		l.UpdatedAt = time.Unix(updated, 0).UTC()
		out = append(out, l)
	}
	return out, rows.Err()
}

// LabelSnapshot returns the effective labels the scheduler matches against:
// device ID -> key -> value, with a detected value winning over a declared
// one for the same key.
func (s *Store) LabelSnapshot() (map[string]map[string]string, error) {
	// Declared first, then detected, so detected overwrites in the map.
	rows, err := s.db.Query(
		`SELECT device_id, key, value FROM device_labels
		 ORDER BY device_id, key, CASE source WHEN ? THEN 0 ELSE 1 END`,
		model.SourceDeclared)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]map[string]string{}
	for rows.Next() {
		var device, key, value string
		if err := rows.Scan(&device, &key, &value); err != nil {
			return nil, err
		}
		if out[device] == nil {
			out[device] = map[string]string{}
		}
		out[device][key] = value
	}
	return out, rows.Err()
}
```

- [ ] **Step 4: Verify and commit**

Run: `go test ./internal/store/ -race`

```bash
git add internal/store/labels.go internal/store/labels_test.go
git commit -m "feat: device labels with provenance, and the scheduler's snapshot"
```

---

### Task 4: Selectors in the queue

**Files:**
- Modify: `internal/store/queue.go`
- Test: `internal/store/selector_queue_test.go`

**Interfaces:**
- Consumes: `selector.Parse`, `(*Store).LabelSnapshot`, the existing `Enqueue`/`ScheduleOnce`/`assignQueued`.
- Produces: `EnqueueRequest` gains `Selector string`; `ErrNoMatchingDevice`; `(*Store).MatchingDevices(sel string) ([]string, error)` returning matching device IDs sorted by ID.

**Design notes an implementer needs:**

- `EnqueueRequest` must carry **exactly one** of `DeviceID` or `Selector`. Both, or neither, is an error — a job that names a device *and* a selector has two answers to the same question.
- A selector matching no device **at submit time** is rejected outright (`ErrNoMatchingDevice`). Queueing forever against a selector that matches nothing is indistinguishable from a hang, and the typo case is far more common than the "that host will register later" case.
- `ScheduleOnce` resolves a selector job's candidates from the label snapshot each pass, tries them in ID order, and assigns the first that succeeds. When none succeeds, it **reserves every candidate** so nothing behind that job can take one. That is the same head-of-queue reservation multi-device jobs use, and it is what stops a selector job starving behind a stream of pinned ones. A broad selector at the head does hold up the devices it matches — correct for FIFO, and worth knowing.
- `QueuePosition` for a selector job counts every queued job ahead of it regardless of device, since its candidate set can change between passes. That is an upper bound, and the CLI should present it as such.

- [ ] **Step 1: Write the failing tests**

Create `internal/store/selector_queue_test.go`:

```go
package store_test

import (
	"testing"

	"github.com/mudler/agents-resources-controller/internal/model"
	"github.com/mudler/agents-resources-controller/internal/store"
	"github.com/stretchr/testify/require"
)

func labelled(t *testing.T, s *store.Store, deviceID string, kv map[string]string) {
	t.Helper()
	require.NoError(t, s.ReplaceLabels(deviceID, model.SourceDetected, kv, nowOf(s)))
}

func selectorEnq(submitter, sel string) store.EnqueueRequest {
	return store.EnqueueRequest{
		Selector:  sel,
		Command:   []string{"./bench"},
		Submitter: submitter,
	}
}

func TestEnqueueRejectsBothDeviceAndSelector(t *testing.T) {
	s, _ := newStore(t)
	r := selectorEnq("agent-a", "vendor=nvidia")
	r.DeviceID = "gpubox:gpu0"
	_, err := s.Enqueue(r)
	require.Error(t, err)
}

func TestEnqueueRejectsNeitherDeviceNorSelector(t *testing.T) {
	s, _ := newStore(t)
	r := selectorEnq("agent-a", "")
	_, err := s.Enqueue(r)
	require.Error(t, err)
}

func TestEnqueueRejectsASelectorMatchingNothing(t *testing.T) {
	s, _ := newStore(t)
	labelled(t, s, "gpubox:gpu0", map[string]string{"vendor": "nvidia"})

	_, err := s.Enqueue(selectorEnq("agent-a", "vendor=amd"))
	require.ErrorIs(t, err, store.ErrNoMatchingDevice)
}

func TestSelectorJobIsAssignedToAMatchingDevice(t *testing.T) {
	s, _ := newStore(t)
	labelled(t, s, "gpubox:gpu0", map[string]string{"vendor": "nvidia", "vram": "80G"})

	job, err := s.Enqueue(selectorEnq("agent-a", "vram>=40G"))
	require.NoError(t, err)
	require.Equal(t, model.JobQueued, job.State)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, "gpubox:gpu0", assigned[0].DeviceID)
}

func TestSelectorJobSkipsANonMatchingDevice(t *testing.T) {
	s, _ := newStore(t)
	registerSecondDevice(t, s) // gpubox:gpu1
	labelled(t, s, "gpubox:gpu0", map[string]string{"vram": "24G"})
	labelled(t, s, "gpubox:gpu1", map[string]string{"vram": "80G"})

	_, err := s.Enqueue(selectorEnq("agent-a", "vram>=40G"))
	require.NoError(t, err)

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, "gpubox:gpu1", assigned[0].DeviceID,
		"the 24G card must never be chosen for a >=40G selector")
}

// The reservation property: a selector job at the head holds its candidates.
func TestQueuedSelectorJobBlocksAPinnedJobBehindIt(t *testing.T) {
	s, _ := newStore(t)
	labelled(t, s, "gpubox:gpu0", map[string]string{"vram": "80G"})

	// Occupy the device.
	first, err := s.Enqueue(selectorEnq("agent-a", "vram>=40G"))
	require.NoError(t, err)
	_, err = s.ScheduleOnce()
	require.NoError(t, err)

	// Head of queue: another selector job waiting for the same device.
	_, err = s.Enqueue(selectorEnq("agent-b", "vram>=40G"))
	require.NoError(t, err)
	// Behind it: a pinned job for that exact device.
	_, err = s.Enqueue(enq("agent-c", 0))
	require.NoError(t, err)

	code := 0
	require.NoError(t, s.Release(first.ID, model.JobSucceeded, &code, ""))

	assigned, err := s.ScheduleOnce()
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.Equal(t, "agent-b", assigned[0].Submitter,
		"the waiting selector job must not be overtaken by a pinned job behind it")
}

func TestMatchingDevicesIsSortedAndFiltered(t *testing.T) {
	s, _ := newStore(t)
	registerSecondDevice(t, s)
	labelled(t, s, "gpubox:gpu0", map[string]string{"vram": "80G"})
	labelled(t, s, "gpubox:gpu1", map[string]string{"vram": "24G"})

	got, err := s.MatchingDevices("vram>=40G")
	require.NoError(t, err)
	require.Equal(t, []string{"gpubox:gpu0"}, got)
}
```

Two helpers this file needs, added to `internal/store/allocate_test.go` beside
the existing `newStore` and `req` so every store test can use them:

```go
// nowOf exposes the fake clock's current time to tests that need to stamp a
// row directly.
func nowOf(s *store.Store) time.Time { return s.Now() }

// registerSecondDevice adds gpubox:gpu1 to the worker newStore created.
func registerSecondDevice(t *testing.T, s *store.Store) {
	t.Helper()
	require.NoError(t, s.UpsertWorker(
		model.Worker{ID: "w1", Host: "gpubox", BootID: "boot-1", LastHeartbeatAt: s.Now()},
		[]model.Device{
			{ID: "gpubox:gpu0", Host: "gpubox", Name: "gpu0", WorkerID: "w1"},
			{ID: "gpubox:gpu1", Host: "gpubox", Name: "gpu1", WorkerID: "w1"},
		},
	))
}
```

That needs `(*Store).Now() time.Time` exported on the store — a one-line
accessor returning `s.clock.Now()`. Add it to `internal/store/store.go`.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/store/ -run 'Selector|Matching' -v`
Expected: FAIL — `EnqueueRequest.Selector` undefined.

- [ ] **Step 3: Implement**

In `internal/store/queue.go`, add the sentinel and field:

```go
// ErrNoMatchingDevice means the selector matches no device that exists. We
// reject at submit rather than queue forever: a selector matching nothing is
// far more often a typo than a bet on a host registering later.
var ErrNoMatchingDevice = errors.New("no device matches the selector")
```

Add `Selector string` to `EnqueueRequest`. In `Enqueue`, before the ceiling
lookup:

```go
	switch {
	case req.DeviceID != "" && req.Selector != "":
		return nil, errors.New("give either device_id or selector, not both")
	case req.DeviceID == "" && req.Selector == "":
		return nil, errors.New("device_id or selector required")
	}

	if req.Selector != "" {
		sel, err := selector.Parse(req.Selector)
		if err != nil {
			return nil, fmt.Errorf("selector: %w", err)
		}
		matches, err := s.MatchingDevices(sel.String())
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("%w: %s", ErrNoMatchingDevice, sel.String())
		}
		req.Selector = sel.String() // store the normalised form
	}
```

The device ceiling lookup and inheritance only run when `req.DeviceID != ""`;
a selector job's ceiling is resolved at assignment time instead, so move that
block inside an `if req.DeviceID != ""`. In `assignQueued`, after selecting
the device, apply that device's `max_runtime` to the job when the job has none
of its own, and reject nothing — the submit-time rejection only applies to a
pinned device, and a selector job inherits whatever device it lands on.

Persist the selector in the existing `jobs.selector` column (it has been
present and unused since Stage 1) and load it in `Job()`.

Add the matcher:

```go
// MatchingDevices returns the device IDs whose effective labels satisfy the
// selector, sorted by ID so scheduling is deterministic.
func (s *Store) MatchingDevices(sel string) ([]string, error) {
	parsed, err := selector.Parse(sel)
	if err != nil {
		return nil, fmt.Errorf("selector: %w", err)
	}
	snap, err := s.LabelSnapshot()
	if err != nil {
		return nil, err
	}
	devices, err := s.Devices()
	if err != nil {
		return nil, err
	}

	var out []string
	for _, d := range devices {
		if parsed.Match(snap[d.ID]) {
			out = append(out, d.ID)
		}
	}
	sort.Strings(out)
	return out, nil
}
```

In `ScheduleOnce`, replace the single-device attempt with a candidate list:

```go
		candidates := []string{job.DeviceID}
		if job.Selector != "" {
			var err error
			candidates, err = s.MatchingDevices(job.Selector)
			if err != nil {
				return nil, err
			}
		}

		// Someone ahead of us is holding one of these devices. Note the
		// existing `reserved` is a map[string]bool — each queued job is
		// visited once per pass, so a job can never encounter its own
		// reservation and no owner needs recording.
		blocked := false
		for _, deviceID := range candidates {
			if reserved[deviceID] {
				blocked = true
				break
			}
		}
		if blocked {
			continue
		}

		placed := false
		for _, deviceID := range candidates {
			out, err := s.assignQueued(job.ID, deviceID)
			switch {
			case err == nil:
				assigned = append(assigned, *out)
				placed = true
			case errors.Is(err, ErrNoDevice):
				continue // this one is busy; try the next candidate
			case errors.Is(err, errJobNoLongerQueued):
				placed = true // vanished; reserve nothing on its behalf
			default:
				return nil, err
			}
			if placed {
				break
			}
		}
		if placed {
			continue
		}

		// Nothing free: hold every candidate for this job so later jobs
		// cannot jump ahead of it.
		for _, deviceID := range candidates {
			reserved[deviceID] = true
			if err := s.reserve(job.ID, deviceID); err != nil {
				return nil, err
			}
		}
```

Update `QueuePosition` so a selector job counts every queued job ahead of it
in scheduling order rather than filtering on `device_id`.

- [ ] **Step 4: Verify**

Run: `go test ./internal/store/ -race -v`, then the invariant pair hard:
`go test ./internal/store/ -run 'TestSchedulingNeverProducesTwoLiveLeases|TestConcurrentAllocate' -race -count=20`
Expected: all PASS, 20/20. Selectors change which device is chosen; they must not weaken the guarantee that only one job holds it.

- [ ] **Step 5: Commit**

```bash
git add internal/store/ internal/selector/
git commit -m "feat: queue jobs against a device selector"
```

---

### Task 5: Probes on the worker

**Files:**
- Create: `internal/worker/probe.go`
- Test: `internal/worker/probe_test.go`

**Interfaces:**
- Consumes: `worker.Run` for process supervision.
- Produces: `worker.probeResult{Host map[string]string, Device map[string]map[string]string}`; `(*Worker).gatherLabels(ctx context.Context) probeResult`; `worker.Config` gains `ProbeDir string` (default `/etc/rc/probe.d`), `ProbeInterval time.Duration` (default 5m), and `DeviceConfig` gains `Labels map[string]string` for declared labels.

**Design notes:**

- A probe is any executable in `ProbeDir` sorted by name. Each gets a 5s timeout and runs in its own process group via `Run`, exactly as jobs and hooks do.
- A probe prints **one flat JSON object** to stdout: `{"vendor":"nvidia","vram":"80G"}`. Values are stringified — a JSON number becomes its decimal string, a bool becomes `"true"`/`"false"`, and a nested object or array is skipped with a warning, because a label is a scalar fact.
- A probe may target one device by naming it: a key of the form `<device-name>.<label>` (`gpu0.vram`) is a device fact; anything else is a host fact applied to every device on that host.
- **A probe that fails, times out, or emits garbage is logged and skipped.** Registration proceeds with whatever succeeded. A wedged `nvidia-smi` costs you a label, never a worker.
- Built-ins run first and are plain Go, not shelling out where the standard library will do: CPU count, total RAM, free disk on the data directory, and kernel version. GPU facts (`vendor`, `model`, `vram`, `driver`, `cuda`) come from `nvidia-smi` when it is on `PATH`, parsed from `--query-gpu=name,memory.total,driver_version --format=csv,noheader,nounits`; when it is absent, no GPU labels are reported and that is not an error.

- [ ] **Step 1: Write the failing tests**

Create `internal/worker/probe_test.go`:

```go
package worker_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mudler/agents-resources-controller/internal/worker"
	"github.com/stretchr/testify/require"
)

func writeProbe(t *testing.T, dir, name, body string) {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755))
}

func TestProbeOutputBecomesHostLabels(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-facts.sh", `echo '{"vendor":"nvidia","cuda":"12.4"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "nvidia", res.Host["vendor"])
	require.Equal(t, "12.4", res.Host["cuda"])
}

func TestDeviceScopedProbeKeys(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-vram.sh", `echo '{"gpu0.vram":"80G","gpu1.vram":"24G"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}, {Name: "gpu1"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "80G", res.Device["gpu0"]["vram"])
	require.Equal(t, "24G", res.Device["gpu1"]["vram"])
	require.NotContains(t, res.Host, "gpu0.vram", "a device-scoped key is not a host fact")
}

func TestFailingProbeIsSkippedNotFatal(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-broken.sh", `echo boom >&2; exit 1`)
	writeProbe(t, dir, "20-good.sh", `echo '{"vendor":"nvidia"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "nvidia", res.Host["vendor"], "a broken probe must not lose a good one")
}

func TestGarbageOutputIsSkipped(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-garbage.sh", `echo 'not json at all'`)
	writeProbe(t, dir, "20-good.sh", `echo '{"vendor":"nvidia"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "nvidia", res.Host["vendor"])
	require.NotContains(t, res.Host, "not")
}

func TestHangingProbeIsTimedOut(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-hang.sh", `sleep 30`)
	writeProbe(t, dir, "20-good.sh", `echo '{"vendor":"nvidia"}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:     []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir:    dir,
		ProbeTimeout: 300 * time.Millisecond,
	})

	start := time.Now()
	res := w.GatherLabelsForTest(context.Background())
	require.Less(t, time.Since(start), 10*time.Second, "a hanging probe must not stall the pass")
	require.Equal(t, "nvidia", res.Host["vendor"])
}

func TestNonScalarValuesAreSkipped(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-nested.sh", `echo '{"ok":"yes","nested":{"a":1},"list":[1,2]}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "yes", res.Host["ok"])
	require.NotContains(t, res.Host, "nested")
	require.NotContains(t, res.Host, "list")
}

func TestNumbersAndBoolsBecomeStrings(t *testing.T) {
	dir := t.TempDir()
	writeProbe(t, dir, "10-types.sh", `echo '{"cores":32,"ecc":true}'`)

	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: dir,
	})

	res := w.GatherLabelsForTest(context.Background())
	require.Equal(t, "32", res.Host["cores"])
	require.Equal(t, "true", res.Host["ecc"])
}

func TestMissingProbeDirIsNotAnError(t *testing.T) {
	w := worker.New(worker.Config{
		ControllerURL: "http://example.invalid", Token: "t", Host: "box",
		Devices:  []worker.DeviceConfig{{Name: "gpu0"}},
		ProbeDir: filepath.Join(t.TempDir(), "does-not-exist"),
	})

	res := w.GatherLabelsForTest(context.Background())
	require.NotNil(t, res.Host, "built-ins still ran")
}
```

`GatherLabelsForTest` is a thin exported wrapper over the unexported
`gatherLabels`, declared in `internal/worker/export_test.go` so the production
API stays unexported:

```go
package worker

import "context"

func (w *Worker) GatherLabelsForTest(ctx context.Context) ProbeResult {
	return w.gatherLabels(ctx)
}
```

The type is `ProbeResult` with exported `Host` and `Device` fields, so the
external test package can read them, while `gatherLabels` itself stays
unexported — the production API gains nothing.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/worker/ -run 'Probe|Garbage|Hanging|Scalar|Numbers|MissingProbe' -v`
Expected: FAIL — `ProbeDir` undefined.

- [ ] **Step 3: Implement**

Create `internal/worker/probe.go` with: the `ProbeResult` type; `builtinLabels()` returning host facts from `runtime.NumCPU()`, `/proc/meminfo`'s `MemTotal`, `syscall.Statfs` on the config's data path, and `/proc/sys/kernel/osrelease`; `nvidiaLabels()` shelling to `nvidia-smi` only when `exec.LookPath` finds it, mapping each line to `gpu<N>.vendor|model|vram|driver`; and `gatherLabels` which merges built-ins, then each executable in `ProbeDir` in name order, with later probes overwriting earlier keys.

Every probe runs through `Run(ctx, JobSpec{Command: []string{path}, MaxRuntime: timeout}, &buf)`, so a probe that hangs is killed as a process group like anything else. A non-zero `Result.ExitCode`, a `Killed` result, or `json.Unmarshal` failure logs a warning naming the probe and moves on.

Add to `Config`: `ProbeDir string \`yaml:"probe_dir"\`` (default `/etc/rc/probe.d`), `ProbeInterval time.Duration \`yaml:"probe_interval"\`` (default 5m), `ProbeTimeout time.Duration \`yaml:"probe_timeout"\`` (default 5s), applied in the same `withDefaults` the hooks work added. Add `Labels map[string]string \`yaml:"labels"\`` to `DeviceConfig`.

- [ ] **Step 4: Verify and commit**

Run: `go test ./internal/worker/ -race -count=2`

```bash
git add internal/worker/probe.go internal/worker/probe_test.go internal/worker/export_test.go internal/worker/config.go
git commit -m "feat: built-in and drop-in probes gather device labels"
```

---

### Task 6: Usage sheets, and pushing labels and sheets to the controller

**Files:**
- Create: `internal/worker/sheet.go`, `internal/store/docs.go`
- Modify: `internal/worker/worker.go`, `internal/server/worker_api.go`
- Test: `internal/worker/sheet_test.go`, `internal/store/docs_test.go`, `internal/server/labels_api_test.go`

**Interfaces:**
- Consumes: Task 3's `ReplaceLabels`, Task 5's `gatherLabels`.
- Produces: `worker.readSheets(dir string, devices []DeviceConfig) (host string, perDevice map[string]string, err error)`; `(*Store).UpsertHostDoc(host, deviceID, body string, at time.Time) error`; `(*Store).HostDoc(host, deviceID string) (string, time.Time, error)`; `server.RegisterRequest` gains `Labels map[string]map[string]string` (device name → labels, with the empty key `""` meaning host-wide), `DeclaredLabels map[string]map[string]string`, `Sheet string`, `DeviceSheets map[string]string`.

**Design notes:**

- Sheets live at `<sheet_dir>/host.md` and `<sheet_dir>/host.d/<device>.md`, default `sheet_dir: /etc/rc`. Missing files are not an error.
- **Size cap 64KB per sheet**, enforced on the worker (truncate with a trailing marker line and log) *and* rejected on the controller (413) so a hand-edited file cannot bloat the database.
- Labels are pushed at registration and again on each probe interval. The probe pass must never block the heartbeat: run it on its own goroutine and push the result when it completes.

- [ ] **Step 1: Write the failing tests**

Cover, in `internal/worker/sheet_test.go`: a host sheet read; a per-device sheet read; both missing (no error, empty strings); an oversized sheet truncated with the marker present and the result at most 64KB.

In `internal/store/docs_test.go`: upsert then read back with timestamp; a second upsert replaces the body; host-level and device-level rows are independent.

In `internal/server/labels_api_test.go`: registration carrying labels stores them as `detected` and declared ones as `declared`; a registration carrying a sheet stores it; an oversized sheet is rejected with 413 and nothing is stored; registration with no labels clears the previously detected set for that device (a probe that stopped reporting).

Write these as real Go test functions with concrete assertions, following the shape of the existing tests in each package — `newStore(t)` in the store package, `newServer(t)` (four return values) plus `post`/`get` in the server package.

- [ ] **Step 2: Run to verify failure, then implement**

Implement `readSheets`, `UpsertHostDoc`/`HostDoc`, the wire fields, and the registration handling. In `handleRegister`, after the existing device upsert: for each device, `ReplaceLabels(deviceID, model.SourceDetected, …)` and `ReplaceLabels(deviceID, model.SourceDeclared, …)`, merging host-wide labels into every device on that host before storing. Then `UpsertHostDoc` for the host sheet and each device sheet.

- [ ] **Step 3: Verify and commit**

Run: `go test ./... -race`

```bash
git add internal/worker/sheet.go internal/store/docs.go internal/server/worker_api.go internal/worker/worker.go
git commit -m "feat: usage sheets, and workers pushing labels and sheets at registration"
```

---

### Task 7: `rc describe`, `--select`, and `--explain`

**Files:**
- Create: `internal/cli/describe.go`
- Modify: `internal/server/client_api.go`, `internal/client/client.go`, `internal/cli/run.go`, `internal/cli/ps.go`
- Test: `internal/server/describe_api_test.go`, `internal/cli/describe_test.go`

**Interfaces:**
- Consumes: `LabelsFor`, `HostDoc`, `MatchingDevices`, `QueuedJobs`.
- Produces: `server.DescribeResponse{Device model.Device, Holder string, JobID string, ElapsedSeconds int, HeartbeatAgeSeconds int, Labels []model.Label, Sheet string, SheetUpdatedAt time.Time, RecentJobs []model.Job}`; route `GET /v1/devices/{id}/describe`; `server.ExplainResponse{Selector string, Matching []string, Free []string, QueueDepth int}`; route `GET /v1/explain?selector=…`; `(*Client).Describe(ctx, deviceID)`, `(*Client).Explain(ctx, selector)`; `cli.NewDescribeCmd()`.

**Design notes:**

- `rc describe gpubox:gpu0` prints, in this order: the device and its state and holder; labels grouped by key showing value, source and age (`vram=80G  detected 4m ago`), with a conflicting declared value shown on its own line beneath rather than hidden; the usage sheet with its age; and the last five jobs with outcome and duration. `-o json` emits `DescribeResponse` verbatim.
- **Age is the point.** A label that has not been confirmed in a week and a sheet last edited in March are exactly what an agent needs to distrust, so both ages are always shown, never omitted when recent.
- `rc run --explain --select 'vram>=40G'` prints which devices match, how many are free, and the queue depth, then exits 0 **without submitting**. It must be impossible to accidentally submit with `--explain`.
- `rc devices --select` filters the table client-side using the same endpoint.

- [ ] **Step 1: Write the failing tests**

In `internal/server/describe_api_test.go`, assert: describe returns labels with both sources and their timestamps; describe returns the sheet and its age; describe on an unknown device is 404; describe requires a client token. In `internal/cli/describe_test.go`, assert the rendered text contains the label with its source and a relative age, that a declared value conflicting with a detected one is visible, and that `-o json` parses back into `DescribeResponse`.

For `--explain`, assert in `internal/cli/run_queue_test.go` style that a stub controller returning two matching devices prints both and that **no submit request is made** — a test that fails if `--explain` ever submits.

- [ ] **Step 2: Run to verify failure, then implement**

Implement the two routes, the client methods, `rc describe`, and the `--select`/`--explain` flags. `rc run` gains `--select`; giving both `--select` and `-d` is an error the CLI rejects before any request, naming both.

- [ ] **Step 3: Verify and commit**

Run: `go test ./... -race`, plus `go build -o /tmp/rc . && /tmp/rc describe --help`

```bash
git add internal/cli/describe.go internal/server/client_api.go internal/client/client.go internal/cli/run.go internal/cli/ps.go
git commit -m "feat: rc describe, --select and --explain"
```

---

### Task 8: `rc hold` and `rc release`

**Files:**
- Create: `internal/cli/hold.go`
- Modify: `internal/store/queue.go`, `internal/server/client_api.go`, `internal/client/client.go`, `internal/worker/worker.go`, `main.go`
- Test: `internal/store/hold_test.go`, `internal/cli/hold_test.go`

**Interfaces:**
- Consumes: `Enqueue`, `RequestKill`, the hook machinery.
- Produces: `EnqueueRequest` gains `Kind string` and `Reason string`; `model.Job` gains `Kind string` and `Reason string`; `(*Client).Hold(ctx, deviceOrSelector, ttl, reason)`, `(*Client).Release(ctx, jobID)`; `cli.NewHoldCmd()`, `cli.NewReleaseCmd()`.

**This task makes a deliberate simplification, and an implementer must understand why before starting.**

The spec implies a second lease mechanism running alongside jobs. This plan instead makes a hold **a job with `kind: hold`** whose command is a sleeper the worker runs, with the TTL as its `max_runtime`.

Everything a hold needs then already exists and is already tested: the same allocation transaction and unique index give it exclusivity; the wall-clock watchdog gives it expiry; `rc kill` gives it `rc release`; the acquire and release hooks fire exactly as they do for a job, so taking a hold stands the node's inference server down as intended; and it shows up in `rc ps`, `rc devices` and the dashboard with no new rendering. A parallel lease type would need every one of those built again and would be the second place in the system where a device changes hands — the thing this project has spent three stages keeping to exactly one.

The cost is honest and worth stating in the README: a hold occupies a worker process running a sleeper, and it appears in job history. *Cost if this ruling is wrong: holds are visible as jobs rather than as a distinct concept, which is cosmetic; the alternative risks a second allocation path, which is not.*

`kind` and `reason` on the lease row (Task 1) are still written, so `rc devices` and the dashboard can label the holder as a hold with its reason rather than showing a mysterious `sleep`.

**Design notes:**

- `rc hold gpubox:gpu0 --ttl 30m --reason "manual profiling"` blocks until granted, then prints the hold's job ID, the device, when it expires, and how to end it early. It stays attached so Ctrl-C releases the hold — and unlike a job, Ctrl-C on a *granted* hold releases it, because a hold's whole purpose is that a human is present.
- `--ttl` is required and capped by the device's `max_runtime` exactly as a job's is, rejected rather than clamped.
- `--select` works for a hold too; it takes the first matching free device and tells you which one it got.
- `rc release <job-id>` ends it, and is a thin alias over kill so ownership is checked identically.
- The sleeper command is chosen by the **worker**, not the client, so a hold cannot be used to run arbitrary code under a different label. The worker recognises `kind == hold` and runs its own sleep for the TTL rather than anything the submitter supplied; the API rejects a hold submission that carries a command.

- [ ] **Step 1: Write the failing tests**

In `internal/store/hold_test.go`: a hold occupies the device so a job queues behind it; a hold's TTL above the device ceiling is rejected; killing a hold frees the device; a hold appears in `QueuedJobs`/`ActiveJobs` with `kind: hold` and its reason.

In `internal/cli/hold_test.go`: `rc hold` prints the granted device and expiry; Ctrl-C on a granted hold releases it (assert the kill request was sent); a hold submission carrying a command is refused.

- [ ] **Step 2: Run to verify failure, then implement**

Add `Kind`/`Reason` through `EnqueueRequest`, the `jobs` table (a new appended migration), `model.Job`, the submit handler (rejecting a command for a hold, and requiring a TTL), the worker's `execute` (substituting its own sleeper for a hold), and the CLI.

- [ ] **Step 3: Verify and commit**

Run: `go test ./... -race`, plus a manual check: hold a device, confirm `rc devices` shows the holder and reason, confirm a job queues behind it, then release it and watch the job start.

```bash
git add internal/cli/hold.go internal/store/ internal/server/ internal/client/ internal/worker/ main.go
git commit -m "feat: rc hold and rc release as a first-class job kind"
```

---

### Task 9: Dashboard labels, end-to-end coverage, and documentation

**Files:**
- Modify: `internal/server/dashboard/index.html`, `README.md`, `examples/worker.yaml`
- Create: `e2e/selector_test.go`, `e2e/hold_test.go`

- [ ] **Step 1: Dashboard**

Show each device's labels on its card — key=value chips, detected and declared visually distinguished, with the oldest label's age surfaced when it exceeds an hour so a stale fleet is visible at a glance. A hold renders with its reason instead of a command. Keep the page self-contained (no remote references) and keep every user-controlled string inserted as a text node.

- [ ] **Step 2: End-to-end tests**

`e2e/selector_test.go`: two devices with different labels; a `--select` job lands on the matching one and never the other; a selector matching nothing is refused at submit with a clear error.

`e2e/hold_test.go`: a hold occupies a device, a job queues behind it, releasing the hold lets the job run, and the acquire and release hooks each fired exactly once across the whole sequence.

Use `require.Eventually` with generous bounds, and follow the existing `newFleet` harness rather than building a second one.

- [ ] **Step 3: Documentation**

Update the README's "Not built yet" table — probes, labels, selectors, usage sheets, `rc describe` and `rc hold` all now exist; leave the Stage 4 rows. Document: the probe protocol including the one-flat-JSON-object contract, the device-scoped key form, and that a failing probe costs a label rather than a worker; declared labels in `worker.yaml` and the rule that detection wins; selectors with their operators and suffixes; that a selector matching nothing is rejected at submit; sheets and their 64KB cap; `rc describe` and what its ages mean; and `rc hold`, including that it is a job, that Ctrl-C releases it, and that hooks fire for it.

Update `examples/worker.yaml` with `probe_dir`, `probe_interval`, declared `labels` on a device, and `sheet_dir`.

- [ ] **Step 4: Verify by running it**

Start a controller and a worker on ports above 19000 with a real probe script and a real `host.md`. Run `rc describe`, a `--select` job, `rc run --explain`, and a hold with a job queued behind it. Paste the actual output into your report; every README claim must be one you watched happen.

- [ ] **Step 5: Commit**

```bash
git add internal/server/dashboard/ e2e/ README.md examples/worker.yaml
git commit -m "test: end-to-end selector and hold coverage; docs: stage 3"
```

---

## Self-Review Notes

**Spec coverage:** probes with built-ins, drop-ins, timeouts and failure tolerance (Task 5); labels with provenance and the declared-never-overwrites-detected rule (Tasks 3, 6); selectors with the documented operators and suffixes, queueing against the matching set (Tasks 2, 4); `rc run --explain` (Task 7); usage sheets with the 64KB cap and per-device overrides (Task 6); `rc describe` with ages and `-o json` (Task 7); `rc hold`/`rc release` with expiry and visibility (Task 8); the data-model changes (Tasks 1, 8).

**Deliberate deviations from the spec, both stated in their tasks:**

- **A hold is a job with `kind: hold`**, not a parallel lease mechanism. It reuses the one allocation transaction, the watchdog for expiry, kill for release, and the hooks. The alternative would rebuild all of that and create a second place where a device changes hands.
- **A selector matching nothing is rejected at submit** rather than queued indefinitely. The typo case is far more common than the will-register-later case, and an indefinite queue is indistinguishable from a hang.

**Carried from earlier stages' ledgers, to fix where this stage touches them:** `rc ps` gained queued jobs in Stage 2 but still needs the hold kind rendered (Task 8); the `reservations` table is still write-only and Task 4 touches that code — decide there whether to read it or drop it, and say which.

**A known weakness in this plan, stated so nobody mistakes it for an oversight:** Tasks 1–5 carry complete test code. Tasks 6–9 specify what each test must assert but leave the implementer to write it. That is weaker than the standard the earlier plans held, and it is exactly where a smoke test can slip in wearing an e2e test's name — Stage 2 produced one such test and a review caught it only because it was reverted and re-run. Reviewers of those four tasks should verify test strength by breaking the code and watching the test fail, not by reading the assertions.

**Known risks:**

- Task 4 changes `ScheduleOnce`, the hottest correctness path in the project. Its invariant test must be run at `-count=20` and any flake stops the task.
- Probes execute operator-supplied scripts as the worker user, like hooks. The same blast radius applies and the README should say so once, next to the hooks note.
- Sheets and labels grow the registration payload; a 64KB sheet per device on a 8-GPU host is half a megabyte per registration. The cap is per sheet, not per registration — if that proves too loose, cap the total.

