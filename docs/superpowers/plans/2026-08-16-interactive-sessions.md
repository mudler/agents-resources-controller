# Interactive sessions (`rc run --tty`) — Implementation Plan

> **STATUS:** IN PROGRESS (2026-08-17). Task 1 is on master; Task 2 exists on the `tty-relay` branch, unreviewed. Tasks 5-7 (`rc lock`, the `rc devices`->`rc list` rename, whole-host claims) are explicitly NOT being done.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `rc run --tty` gives you a real interactive shell on a leased device, supervised the same way an ordinary job is — killable, watchdogged, re-attachable.

**Architecture:** A job marked interactive runs under a PTY in the worker. Output already flows worker → controller → client; this adds the return path (keystrokes and terminal resizes) as a second stream the worker **dials out** to fetch, preserving the property that the controller never connects to a box. Both directions are chunked HTTP over the existing token auth — no websocket dependency.

**Tech Stack:** Go 1.26, stdlib `net/http` only, `creack/pty` for the PTY, `golang.org/x/term` for client-side raw mode. Both are new dependencies and are justified in Task 1.

**Spec:** `docs/superpowers/specs/2026-08-16-ssh-leases-and-k8s-design.md`

## Global Constraints

- Go 1.26, no cgo. Module `github.com/mudler/resource-controller`.
- **The worker dials out. Nothing connects to a box.** Every new connection in this plan is worker → controller or client → controller. This is what makes NAT'd hosts work and why the controller holds no credentials; a design that has the controller open a connection to a worker is wrong no matter how convenient.
- **stdlib `net/http` for transport.** The project's dependency list is deliberate (`modernc.org/sqlite`, `spf13/cobra`, `stretchr/testify`, `gopkg.in/yaml.v3`, `google/uuid`). A websocket library is a new transport dependency for the controller and is rejected — see Task 2.
- SQLite at `MaxOpenConns(1)`; any query iterating rows then issuing another must drain first. **A live TTY session must not touch the database on the hot path** — a keystroke must never wait on a lock the scheduler wants.
- Migrations are append-only, and there is a live controller with a real database.
- Controller-side time goes through the `Clock` interface; store and server tests never sleep.
- **The allocation transaction is not to be restructured.**
- Interactive sessions are the `client` role, exactly like `rc run`. A TTY grants no authority `rc run` does not already grant.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/worker/pty.go` | Run a command under a PTY; resize it. New. |
| `internal/server/tty.go` | In-memory session registry and the four relay endpoints. New. |
| `internal/server/tty_frame.go` | The wire framing shared by both ends. New. |
| `internal/client/tty.go` | Client half: raw mode, resize, connect both streams. New. |
| `internal/worker/worker.go` | Route an interactive assignment to the PTY path. |
| `internal/cli/run.go` | `--tty`, and the whole-host claim it implies. |
| `internal/cli/devices.go` → `list.go` | `rc devices` becomes `rc list`. |
| `internal/cli/lock.go` | `rc lock`. New. |

---

## The transport, decided once so every task shares it

Four streams, all plain chunked HTTP, all opened by the side that is allowed to dial:

```
   client                     CONTROLLER                     worker
   ──────                     ──────────                     ──────
   GET  /v1/jobs/{id}/tty/out ◀── relay ──  POST /v1/jobs/{id}/tty/out
        (receives output)                        (streams PTY output up)

   POST /v1/jobs/{id}/tty/in  ──▶ relay ──▶  GET  /v1/jobs/{id}/tty/in
        (sends keys, resize)                     (receives keys, resize)
```

The worker opens **both** of its connections outbound when it picks up an
interactive assignment. The controller holds the two halves in memory and
copies between them; it is a relay, not a store.

**Why not websockets.** They would halve the connection count and give framing
for free, but the controller has no websocket dependency today and adding one
puts a third-party library on the path of every interactive session. Chunked
HTTP in both directions is already how this codebase streams logs, works
through the same proxies and tokens, and needs no new dependency. The cost is
four connections instead of two, which for a handful of interactive sessions
is not a real cost.

**Framing.** The `in` direction carries two kinds of message and is tiny, so it
is newline-delimited JSON:

```json
{"t":"d","b":"bHM=\n"}      // data: base64 keystrokes
{"t":"r","rows":48,"cols":180}   // resize
```

The `out` direction is high-volume and single-purpose, so it is **raw bytes**
with no framing at all.

---

### Task 1: Run a command under a PTY

**Files:**
- Create: `internal/worker/pty.go`, `internal/worker/pty_test.go`
- Modify: `go.mod`

**Interfaces:**
- Produces: `worker.startPTY(ctx, JobSpec) (*ptySession, error)` where `ptySession` exposes `io.ReadWriteCloser` for the terminal and `Resize(rows, cols uint16) error`; and `Wait() Result` matching what `Run` already returns.

`creack/pty` is the standard Go PTY package and is a thin wrapper over the
`openpty` syscalls; writing that by hand is not a good use of anyone's time.
It is ~200 lines with no transitive dependencies.

- [ ] **Step 1: Write the failing test**

```go
package worker

import (
    "bufio"
    "context"
    "strings"
    "testing"
    "time"

    "github.com/stretchr/testify/require"
)

// A PTY is not just "a pipe with extra steps": programs behave differently
// when stdout is a terminal, and that difference is the whole reason this
// exists. `test -t 1` is the smallest thing that can tell them apart.
func TestPTYLooksLikeATerminalToTheProcess(t *testing.T) {
    s, err := startPTY(context.Background(), JobSpec{
        Command: []string{"/bin/sh", "-c", "test -t 1 && echo IS_TTY || echo NOT_TTY"},
    })
    require.NoError(t, err)
    defer s.Close()

    line, err := bufio.NewReader(s).ReadString('\n')
    require.NoError(t, err)
    require.Contains(t, line, "IS_TTY",
        "the process must see a terminal on stdout, or this is just a pipe")
}

// Keystrokes go in, output comes back. The interactive loop in one test.
func TestPTYCarriesInputBackToTheProcess(t *testing.T) {
    s, err := startPTY(context.Background(), JobSpec{Command: []string{"/bin/sh"}})
    require.NoError(t, err)
    defer s.Close()

    _, err = s.Write([]byte("echo hello-from-stdin\n"))
    require.NoError(t, err)

    deadline := time.Now().Add(5 * time.Second)
    var got strings.Builder
    buf := make([]byte, 1024)
    for time.Now().Before(deadline) {
        n, err := s.Read(buf)
        if n > 0 {
            got.Write(buf[:n])
            if strings.Contains(got.String(), "hello-from-stdin") {
                return
            }
        }
        if err != nil {
            break
        }
    }
    t.Fatalf("never saw the command's output; got: %q", got.String())
}

// A shell asks the terminal how big it is. Getting this wrong is why
// remote shells wrap text at 80 columns forever.
func TestPTYResizeIsVisibleToTheProcess(t *testing.T) {
    s, err := startPTY(context.Background(), JobSpec{Command: []string{"/bin/sh"}})
    require.NoError(t, err)
    defer s.Close()

    require.NoError(t, s.Resize(48, 180))
    _, err = s.Write([]byte("stty size\n"))
    require.NoError(t, err)

    deadline := time.Now().Add(5 * time.Second)
    var got strings.Builder
    buf := make([]byte, 1024)
    for time.Now().Before(deadline) {
        n, _ := s.Read(buf)
        if n > 0 {
            got.WriteString(string(buf[:n]))
            if strings.Contains(got.String(), "48 180") {
                return
            }
        }
    }
    t.Fatalf("resize never reached the process; got: %q", got.String())
}

// The process group rule the rest of this worker lives by: a PTY session
// must be killable as a group, or an interactive shell's children outlive it
// and keep the GPU.
func TestPTYKillsTheWholeProcessGroup(t *testing.T) {
    ctx, cancel := context.WithCancel(context.Background())
    s, err := startPTY(ctx, JobSpec{Command: []string{"/bin/sh"}})
    require.NoError(t, err)

    _, err = s.Write([]byte("sleep 300 & echo STARTED\n"))
    require.NoError(t, err)
    time.Sleep(500 * time.Millisecond)

    cancel()
    res := s.Wait()
    require.True(t, res.Killed, "cancelling the context must kill the session")
    // The child must be gone too — see exec.go's killTree, which this reuses.
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/worker/ -run TestPTY -v`
Expected: FAIL — `startPTY` undefined.

- [ ] **Step 3: Add the dependency and implement**

```bash
go get github.com/creack/pty@latest
```

Implement `startPTY` in `internal/worker/pty.go`. **Reuse `exec.go`'s existing
process-group setup and `killTree`** rather than writing a second supervision
path — the whole value of `--tty` over `rc ssh` is that it is supervised, and
that supervision already exists. The PTY replaces the pipes; nothing about
process groups, `SIGTERM` → grace → `SIGKILL`, or the watchdogs changes.

- [ ] **Step 4: Run, then commit**

Run: `go test ./internal/worker/ -race -count=2 -run TestPTY`, then `go test ./... -race`

```bash
git add internal/worker/pty.go internal/worker/pty_test.go go.mod go.sum
git commit -m "feat: run a job under a PTY, supervised as a process group"
```

---

### Task 2: The controller relay

**Files:**
- Create: `internal/server/tty.go`, `internal/server/tty_frame.go`, `internal/server/tty_test.go`
- Modify: `internal/server/server.go` (routes)

**Interfaces:**
- Produces: four routes, and `server.ttyRegistry` mapping job ID → a live session with two halves.
- Consumes: nothing from Task 1 — this task is transport only and is tested with fake ends.

- [ ] **Step 1: Write the failing tests**

The properties that matter, each its own test:

1. **A byte written by the worker half reaches the client half**, and vice versa.
2. **A session is per job, and isolated** — bytes for job A never appear on job B's stream. Drive two sessions concurrently and assert no crossover; this is the test that would catch a registry keyed by worker rather than job.
3. **The client half may connect before the worker half, and after** — an operator's terminal and the worker's dial-out race, and neither order may drop bytes. Assert both orders.
4. **A closed worker half closes the client half**, so a killed job does not leave a terminal hanging forever.
5. **Auth**: `client` role for the client halves, `worker` role for the worker halves. A client token must not be able to open the *worker* side of a session and inject output into someone's terminal.
6. **Nothing touches the store on the relay path.** Assert by driving a session against a server whose store handle is closed — a keystroke must not need the database.

Write these as real tests against `httptest`, following `internal/server`'s
existing conventions (`newServer(t)` returns four values).

- [ ] **Step 2: Run to verify failure, then implement**

Implement the registry as a `map[string]*ttySession` under a mutex, each
session holding two `io.ReadWriteCloser` halves and a `sync.Once` for
teardown. Copy with `io.Copy` in both directions and `http.NewResponseController`
to flush — the same flushing `handleStreamLogs` already does.

**The registry is in memory and deliberately not persisted.** A controller
restart drops live sessions; that is correct, because the terminals are gone
too. Persisting them would put a keystroke path through SQLite, which the
constraints forbid.

- [ ] **Step 3: Commit**

```bash
git add internal/server/tty.go internal/server/tty_frame.go internal/server/tty_test.go internal/server/server.go
git commit -m "feat: in-memory TTY relay, four streams, no store on the hot path"
```

---

### Task 3: The worker end

**Files:**
- Modify: `internal/worker/worker.go`
- Test: `internal/worker/tty_worker_test.go`

**Interfaces:**
- Consumes: `startPTY` (Task 1), the relay routes (Task 2).
- Produces: an assignment carrying `tty: true` runs under a PTY and connects both streams.

- [ ] **Step 1: Write the failing test**

Against an `httptest` controller that serves the two worker-side routes:
assert the worker POSTs PTY output up, GETs the `in` stream, applies a resize
frame it receives, and that a normal (non-TTY) assignment still uses the
existing pipe path untouched. That last assertion is the regression guard —
every existing job must be unaffected by this change.

- [ ] **Step 2: Implement, run, commit**

In `execute`, branch on the assignment's `tty` flag. **Everything after the
process exits is unchanged**: the terminal report, the verify probes, the
release linger, the hooks. An interactive job is an ordinary job with a
different stdio.

Run: `go test ./internal/worker/ -race -count=2`, then `go test ./... -race`

---

### Task 4: The client end

**Files:**
- Create: `internal/client/tty.go`
- Modify: `internal/cli/run.go`
- Test: `internal/client/tty_test.go`

**Interfaces:**
- Produces: `client.AttachTTY(ctx, jobID) error`, and `--tty` on `rc run`.

- [ ] **Step 1: Write the failing test**

Client-side raw mode cannot be tested without a terminal, so test what can be:
that `AttachTTY` sends a resize frame on start, forwards written bytes as `d`
frames, copies received bytes to its output, and returns when the stream
closes. Drive it against an `httptest` server, with an `io.Reader`/`io.Writer`
pair standing in for the terminal.

State plainly in the report that raw-mode behaviour was verified by hand, and
what you actually typed — a test cannot cover it.

- [ ] **Step 2: Implement**

`golang.org/x/term` for `MakeRaw`/`Restore` and `GetSize`. **Restore the
terminal on every exit path**, including panic and signal — a client that dies
in raw mode leaves the operator with an unusable shell, which is a worse first
impression than the feature is worth. Send a resize frame on `SIGWINCH`.

- [ ] **Step 3: Verify by hand and commit**

Start a controller and a worker on ports above 19000, run
`rc run --tty -d <device>`, and actually use it: run `vim`, resize the window,
press Ctrl-C, run `stty size`. Paste what you observed into the report.

---

### Task 5: Interactive claims take the whole host

**Files:**
- Modify: `internal/cli/run.go`, `internal/store/allocate.go`
- Test: `internal/store/allocate_test.go`, `internal/cli/run_test.go`

**Interfaces:**
- Produces: `--tty` claims every device on the chosen host, all-or-nothing.

An interactive shell reaches every GPU on the machine — `CUDA_VISIBLE_DEVICES`
is a suggestion a shell user can override — so a lease covering one of four
would have the fleet advertise the other three as free while someone is on
them.

- [ ] **Step 1: Decide the shape, and write the test that pins it**

On a **single-device host** — which is the entire current fleet — a host claim
is exactly the existing single-device claim, and no new store code is needed.
The test must assert that.

On a **multi-device host**, this needs every device's lease in one
transaction. **Do not build that yet.** Instead: refuse, loudly, with a message
naming the devices and telling the operator to claim one explicitly. Write the
test for that refusal.

This is deliberate: there is no multi-GPU box in the fleet, the atomic
multi-lease is a change to the allocation core — the one part of this system
that has been protected hardest — and building it on speculation would be
the wrong trade. Failing loudly is honest; under-claiming silently is the lie
this feature exists to prevent.

- [ ] **Step 2: Implement, run, commit**

---

### Task 6: `rc list` and `rc lock`

**Files:**
- Rename: `internal/cli/devices.go` → `internal/cli/list.go`
- Create: `internal/cli/lock.go`
- Modify: `cmd/rc/main.go`

- [ ] **Step 1: Rename `rc devices` to `rc list`**

A rename, not an alias: one name for the thing. Update every reference in
`README.md`, `docs/agents.md`, `docs/install.md` and the dashboard's help text.
Grep for `rc devices` across the repo and fix each hit.

- [ ] **Step 2: `rc lock`**

`rc hold` with the device inferred from the machine you are on: match the
local hostname against the fleet's hosts, claim that host. For the case where
someone reached a box by plain `ssh` and wants to be honest about it.

Fail clearly when the local host is not in the fleet — that is a person on the
wrong machine, and guessing a device for them would be worse than saying so.

- [ ] **Step 3: Run, commit**

---

### Task 7: End-to-end, and the docs

**Files:**
- Create: `e2e/tty_test.go`
- Modify: `README.md`, `docs/agents.md`

- [ ] **Step 1: An end-to-end interactive session**

Through the real controller and a real worker: claim, run a command through
the PTY, assert the output arrives, assert `rc kill` from another client
actually ends it — **that last one is the whole argument for `--tty` over
`rc ssh`** and must be proven, not assumed.

- [ ] **Step 2: Document it in the agent guide**

`docs/agents.md` gains the interactive path, and the two-mechanism table from
the spec. Be explicit that `--tty` claims the whole host and why.

- [ ] **Step 3: Run everything and commit**

`go clean -testcache && go test ./... -race`, `go vet ./...`, `gofmt -l .`

---

## Self-Review Notes

**Spec coverage.** `rc run --tty` (Tasks 1–4), whole-host claims (Task 5),
`rc list` and `rc lock` (Task 6). `rc cp` is the next plan and needs Task 2's
stream. The Kubernetes work is a separate plan, already written.

**Two new dependencies, both justified rather than assumed.** `creack/pty` is
the standard PTY wrapper and writing `openpty` by hand is not worth it;
`golang.org/x/term` is the standard raw-mode package. Both are narrow, widely
used, and used only on the interactive path. A **websocket** library was
considered and rejected: chunked HTTP in both directions matches how this
codebase already streams, works with the same tokens and proxies, and keeps
the controller's dependency list as deliberate as it is.

**The deferral in Task 5 is a real deviation from the spec** and is called out
rather than hidden. The spec says an interactive claim takes the whole host
atomically. On this fleet every host has one GPU, so that is satisfied by
existing code; the atomic multi-device transaction is deferred until a
multi-GPU box exists, and multi-GPU hosts refuse loudly in the meantime.

**Known risks.**

- **Raw mode is the part tests cannot cover.** A client that exits without
  restoring the terminal leaves the operator typing into a broken shell. Task 4
  requires hand verification and says so.
- **The relay must never touch the store.** A keystroke waiting on the same
  single SQLite connection the scheduler uses would make typing stutter
  whenever a job was submitted. Task 2 tests this directly.
- **Four concurrent streams per session** means four goroutines and two
  connections per side. Leaks here are invisible until a controller has been
  up for a week; Task 2 should check goroutine counts across many
  connect/disconnect cycles.
- **`rc attach` to a TTY session is not designed here.** `rc run --tty`
  reconnecting after a dropped terminal is desirable and is not in this plan;
  the job keeps running, which is the existing behaviour, but rejoining its
  terminal is future work.
