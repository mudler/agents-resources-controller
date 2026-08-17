# `rc cp` — file transfer over an existing lease — Implementation Plan

> **STATUS:** NOT STARTED as of 2026-08-17. Blocked on the relay above. Note that `/workspace` on dgx and thor is already shared storage reachable from the LAN, so this now targets the narrower gap: orin (local disk) and callers off the LAN.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `rc cp ./train.py dgx:gpu0:/workspace/` — move a file onto a box you hold, and back off it, without inventing an upload service.

**Architecture:** `tar` over the interactive exec stream. The client streams a tarball into a `tar -x` running on the far side through the same relay `rc run --tty` uses. No new endpoint, no upload storage, no controller-side buffering — the controller relays bytes it never keeps.

**Tech Stack:** Go 1.26, stdlib `archive/tar`, the relay from the interactive-sessions plan.

**Spec:** `docs/superpowers/specs/2026-08-16-ssh-leases-and-k8s-design.md`

**Depends on:** `docs/superpowers/plans/2026-08-16-interactive-sessions.md`, Tasks 1–3. This plan cannot start until the relay carries bytes in both directions, because it *is* that relay with a different program on the far end. This is how `kubectl cp` is built, for the same reason.

## Global Constraints

- Go 1.26, no cgo. Module `github.com/mudler/resource-controller`.
- **The controller relays, it does not store.** A transferred file must never land in SQLite or on the controller's disk. The controller is a scheduler with a single-connection database; putting file bytes through it is the wrong shape and would block the scheduler on a slow copy.
- **You may only copy to a device you hold.** The transfer runs under a lease, exactly like any other execution. There is no unauthenticated upload path and no way to write to a box someone else is using.
- **This narrows a stated non-goal, and only this far.** The project says "no shipping code to hosts… it is not a deployment tool and never copies a payload anywhere." `rc cp` moves a file you name, onto a box you hold, once. It does not sync, watch, reconcile, or install. Anything recurring or large belongs on the NAS mount.
- `tar` must exist on the far side. It does in the worker image, which is ours.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/client/cp.go` | Build the tar stream, drive the transfer, report progress. New. |
| `internal/cli/cp.go` | `rc cp` command and its path parsing. New. |
| `e2e/cp_test.go` | Round-trip through a real controller and worker. New. |

No server files. That is the point of the design: if this plan touches `internal/server`, the approach has drifted.

---

### Task 1: Path parsing, which is where this command gets dangerous

**Files:**
- Create: `internal/cli/cp.go`, `internal/cli/cp_test.go`

**Interfaces:**
- Produces: `parseCpArg(string) (remote bool, device, path string, err error)`.

A remote path is `<host>:<name>:<path>` — `dgx:gpu0:/workspace/x`. Device IDs
already contain a colon, which is exactly the ambiguity that makes naive
splitting wrong.

- [ ] **Step 1: Write the failing tests**

```go
package cli

import (
    "testing"

    "github.com/stretchr/testify/require"
)

func TestParseCpArgSplitsDeviceFromPath(t *testing.T) {
    remote, device, path, err := parseCpArg("dgx:gpu0:/workspace/train.py")
    require.NoError(t, err)
    require.True(t, remote)
    require.Equal(t, "dgx:gpu0", device, "a device ID contains a colon; only the LAST one separates the path")
    require.Equal(t, "/workspace/train.py", path)
}

func TestParseCpArgTreatsAPlainPathAsLocal(t *testing.T) {
    remote, _, path, err := parseCpArg("./train.py")
    require.NoError(t, err)
    require.False(t, remote)
    require.Equal(t, "./train.py", path)
}

// A Windows-style or relative path with a colon must not be mistaken for a
// device. Getting this wrong sends a local file to a device that does not
// exist, which at least fails — the reverse would silently write locally.
func TestParseCpArgRejectsAnAmbiguousRemote(t *testing.T) {
    _, _, _, err := parseCpArg("gpu0:/workspace/x")
    require.Error(t, err, "a device ID is host:name — one colon is not enough to name a device")
    require.Contains(t, err.Error(), "host:name:path")
}

func TestParseCpArgRejectsAnEmptyRemotePath(t *testing.T) {
    _, _, _, err := parseCpArg("dgx:gpu0:")
    require.Error(t, err)
}
```

- [ ] **Step 2: Run to verify failure, then implement**

Split on the **last** colon, then require the remainder to itself contain
exactly one colon to be a device ID.

- [ ] **Step 3: Commit**

```bash
git add internal/cli/cp.go internal/cli/cp_test.go
git commit -m "feat: rc cp path parsing, where a device ID's own colon lives"
```

---

### Task 2: Send a file, by streaming a tar into the far side

**Files:**
- Create: `internal/client/cp.go`, `internal/client/cp_test.go`
- Modify: `internal/cli/cp.go`

**Interfaces:**
- Consumes: the relay from the interactive-sessions plan (`client.AttachTTY`'s underlying streams — refactor the transport out of it rather than duplicating it).
- Produces: `client.CopyTo(ctx, device, localPath, remotePath) error`.

- [ ] **Step 1: Write the failing tests**

Test against a fake far end that runs a real `tar -x` in a temp directory, so
the assertion is "the file arrived with the right bytes and the right mode",
not "we wrote something to a socket":

1. A file arrives with identical contents.
2. **The executable bit survives.** A copied script that lands non-executable
   is the most common way this feature disappoints, and it costs one tar
   header field to get right.
3. A directory copies recursively.
4. **A symlink is not followed out of the tree.** Archive it as a symlink or
   refuse it — a `tar` that follows `../../etc` is a way to write outside the
   target.
5. A transfer to a device the caller does not hold is refused, and the error
   says so.

- [ ] **Step 2: Implement**

`archive/tar` from stdlib. Stream it — never build the archive in memory,
because the point of this design is that nothing buffers the whole payload,
and a 2GB checkpoint would otherwise be held twice.

Run `tar -xf - -C <dir>` on the far side through the exec stream.

- [ ] **Step 3: Falsify each test, then commit**

Break the mode bits, break the recursion, and watch the relevant test fail.
Report what you saw.

---

### Task 3: Fetch a file back

**Files:**
- Modify: `internal/client/cp.go`, `internal/cli/cp.go`

**Interfaces:**
- Produces: `client.CopyFrom(ctx, device, remotePath, localPath) error`.

The mirror: run `tar -cf - -C <dir> <name>` on the far side and extract
locally.

- [ ] **Step 1: Write the failing tests**

The same five properties in reverse, plus the one that only applies in this
direction: **an archive from the far side must not be able to write outside
the local destination.** The remote end is not necessarily trustworthy — a
malicious or broken `tar` stream containing `../../.ssh/authorized_keys` must
be rejected, not extracted. Assert that a crafted archive with a `..`
component is refused.

- [ ] **Step 2: Implement, falsify, commit**

Reject any entry whose cleaned path escapes the destination. Test it with an
archive you build by hand.

---

### Task 4: End to end, and the docs

**Files:**
- Create: `e2e/cp_test.go`
- Modify: `README.md`, `docs/agents.md`

- [ ] **Step 1: Round-trip through a real fleet**

Claim a device, copy a file up, run a job that reads it, copy the result back,
assert the bytes match. Then assert a copy to a device held by **someone else**
is refused.

- [ ] **Step 2: Document the line this draws**

In `docs/agents.md`: `rc cp` for a script or a config; the NAS mount for
models, datasets and checkpoints. Say plainly that streaming tens of gigabytes
through the controller is the wrong shape, so an agent does not reach for it
by default.

Update the README's "Deliberately out of scope" so the non-goal reads
accurately: no deployment, no sync, no package management — a file you name,
onto a box you hold.

- [ ] **Step 3: Run everything and commit**

`go clean -testcache && go test ./... -race`, `go vet ./...`, `gofmt -l .`

---

## Self-Review Notes

**Spec coverage.** The spec's "Getting files onto a box" section in full: `rc cp`
as tar over the exec stream (Tasks 2–3), the NAS for large data (documented,
Task 4), and the narrowed non-goal (Task 4).

**Why there are no server tasks.** If this plan ends up touching
`internal/server`, the design has drifted into building an upload service. The
controller relays bytes it never keeps; that is what makes this cheap and what
keeps file transfer off the scheduler's critical path.

**Known risks.**

- **Path traversal, in both directions.** Task 2 and Task 3 each have a test
  for it because the two directions fail differently: sending follows a
  symlink out of the source tree, fetching extracts `..` out of the
  destination. Neither is theoretical.
- **The executable bit** is the detail that makes this feel broken when it is
  technically working. Pinned by a test.
- **No resume, no progress bar, no integrity check.** A dropped transfer is
  retried from the start. Acceptable for scripts and configs, which is what
  this is for; not acceptable for a 40GB model, which is why the docs point at
  the NAS.
- **This plan cannot start before the interactive-sessions plan reaches
  Task 3.** There is no useful subset to build first.
