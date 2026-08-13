# Resource Controller — Stages 2-4 Design

**Date:** 2026-08-13
**Status:** Approved for planning
**Builds on:** `2026-08-13-resource-controller-design.md` (Stage 1, shipped)

## What exists today

Stage 1 shipped and merged: a controller owning allocation in a single SQLite
transaction, workers on device hosts supervising jobs in their own process
groups, and `rc run` / `rc ps` / `rc devices`. Exclusivity holds under load,
kills take the whole process tree, and a silent worker's devices are demoted
`unknown` → `unhealthy` and never returned to the pool on silence alone.

What it does not do is everything an operator needs to leave it alone: a hung
job holds its GPU indefinitely, a busy device means "try again later" rather
than "you are next", the fleet cannot describe itself, and there is nothing to
look at but a terminal.

## Sequencing

Three stages, each its own plan and build cycle, reviewed at the boundaries.

**Stage 2 — jobs you can leave alone.** Queue, watchdogs, lease expiry, boot
identity, `rc kill`, `rc attach`, and a read-only dashboard.

**Stage 3 — a fleet that describes itself.** Probes, labels, selectors, usage
sheets, `rc describe`, `rc hold`.

**Stage 4 — operations.** Verify probes, webhook notifications, dashboard
actions.

Two deliberate departures from the original staging:

- **The dashboard moves forward into Stage 2**, read-only. It is mostly a
  rendering of `/v1/state`, which already exists, and building it last means
  working blind through two stages. The cost is roughly 20% rework when the
  queue and labels land, paid for by having something to look at throughout.
- **Lease expiry moves into Stage 2** from wherever it was implied. Stage 1
  writes `leases.expires_at` and never reads it. `rc hold` in Stage 3 is
  unsafe without enforcement — an interactive lease that cannot expire is a
  GPU leaked by anyone who closes their laptop.

---

# Stage 2 — Jobs you can leave alone

## The queue

Jobs enter in state `queued`. A single-goroutine scheduler loop in the
controller assigns them FIFO within priority. The head job **reserves**
devices as they free, so a multi-device request cannot starve behind a stream
of single-device ones.

Priority is an integer set with `--priority` (default `0`, higher runs first),
bounded to a small range so it stays a nudge rather than a scheduling
language. There is no preemption: a higher-priority job waits for the running
job to finish, it never displaces it. A job already holding a device is never
interrupted by scheduling decisions — only by a watchdog, an explicit
`rc kill`, or lease expiry.

Assignment remains exactly one SQLite transaction: `device: ready → busy`,
`job: queued → assigned`, and the lease insert commit together or not at all.
The queue sits in front of that transaction; it does not replace it. The
partial unique index `leases_one_live_per_device` remains the last-resort
guard.

There is still no "is this device free?" endpoint. A caller queues or it does
not; nothing composes into check-then-act.

## `rc run` blocking behaviour

`rc run` blocks by default, which is what `flock` did and what stops agents
from writing retry loops:

```
$ rc run -d gpubox:gpu0 -- ./bench
rc: queued at position 3 for gpubox:gpu0
rc: queued at position 1 for gpubox:gpu0
rc: job 3df1acbe on gpubox:gpu0
<output streams>
```

- `--timeout 30m` bounds the wait; on expiry the job is removed from the queue
  and `rc run` exits non-zero with a message naming the position it reached.
- `--no-wait` restores Stage 1's fail-fast (`409 no_device_available`).
- **Ctrl-C while queued cancels the queued job outright.** Nothing is
  executing, so there is no lease to protect. This is deliberately different
  from Ctrl-C on a *running* job, which still only detaches — the worker owns
  that lease and a client disconnect must never free an occupied GPU.

Queue position is derived, not stored: it is the count of jobs ahead of this
one in the same priority band matching the same device set.

## Watchdogs

Two limits, both enforced **on the worker**, so a controller outage cannot
leave a job running past its ceiling:

- `max_runtime` — hard wall clock.
- `idle_timeout` — no output written for N.

Ceilings are declared per device in `worker.yaml`:

```yaml
devices:
  - name: gpu0
    max_runtime: 4h
  - name: gpu1
    max_runtime: 24h
```

A job may request **less** than its device's ceiling, never more; a submit
requesting more is rejected at the API with the cap named, rather than
silently clamped. The limit lives with the machine that cannot tolerate being
exceeded.

On a trip: SIGTERM to the process group, grace, SIGKILL — the existing
supervision path — recorded as `killed` with `kill_reason` naming which
watchdog fired (`max_runtime exceeded (4h)`, `no output for 30m`).

**Device config is a breaking change to `worker.yaml`.** Stage 1 accepts a
list of strings; Stage 2 accepts either that (no ceiling) or a list of
objects. Both parse, so existing configs keep working.

## Lease expiry

The reaper gains a second responsibility: a lease whose `expires_at` has
passed is released, its job marked `lost` with reason `lease expired`, and its
device marked `unhealthy` — not `ready`, because an expired lease tells us
nothing about whether the device is occupied.

Job leases are renewed implicitly by the worker's heartbeat for as long as the
job runs, so this fires only when something has genuinely stopped reporting.
Interactive leases (`rc hold`, Stage 3) carry a fixed TTL and are the real
consumer of this machinery.

## Boot identity and host reboot recovery

Stage 1 treats a rebooted host identically to a crashed worker process: the
device is quarantined `unhealthy` and needs an explicit clear. That is correct
for a crash — an orphaned CUDA process may still be pinning VRAM — but wrong
for a reboot, which is the one event that *guarantees* the device is clean.
Requiring a manual clear after every reboot trains operators to clear
reflexively, which is exactly the habit to avoid when a quarantine is real.

The worker reports its host's boot identity at registration, read from
`/proc/sys/kernel/random/boot_id` (falling back to `/proc/stat`'s `btime` if
unreadable, and to empty if neither is available). The controller stores it on
the worker row. On re-registration:

| Boot ID | Meaning | In-flight jobs | Devices |
|---|---|---|---|
| Changed | The machine rebooted; nothing from before survives | `lost`, reason `host rebooted` | back to **`ready`** |
| Unchanged | The worker process restarted; an orphan may survive | `lost`, reason `worker re-registered` | **`unhealthy`** |
| Empty/unavailable | No proof either way | `lost`, reason `worker re-registered` | **`unhealthy`** |

This is the spec's own rule — *clearing requires a returning worker reporting
the device clean* — with machine-checkable proof rather than a promise.

A job killed by a host reboot is **not** re-queued. A half-finished training
run silently restarting is worse than being told it died.

## `rc kill` and `rc attach`

`rc kill <job>` cancels a queued job or terminates a running one through the
existing supervision path. Ownership-checked: you may kill jobs you submitted;
an admin token may kill anything. This is the server-side cancellation whose
absence made Stage 1's Ctrl-C purely a detach.

`rc attach <job>` re-streams a running job's logs from the beginning and then
follows. This is what makes detaching honest — Stage 1 has no way back to a
job you walked away from.

## Dashboard v0 (read-only)

Served by the controller at `/`, assets embedded in the binary, no build step
at deploy. The browser never contacts device hosts: the controller already
knows everything.

The page loads a snapshot from `GET /v1/state`, then subscribes to a new
`GET /v1/events` SSE stream for deltas — device state changes, job
transitions, queue movement, watchdog trips, and log lines for an open job. A
5s poll backs it up if the stream drops.

Layout: device grid coloured by state, each tile showing holder, elapsed,
command, and **heartbeat age**; queue below with positions; click through to
live logs; history of finished jobs with exit codes and durations. Unhealthy
devices and watchdog trips pin to the top.

**Staleness stays a first-class signal.** A worker that stopped reporting
greys out with "no contact 47s" rather than looking healthy. A job tile shows
elapsed against its watchdog margin, so "running 8m of a 4h limit, last output
6m ago" is legible before it becomes a dead box.

Authentication: the page prompts for a client token and holds it in
`sessionStorage` for the tab's lifetime. No cookies, no server-side sessions.

## Stage 2 data model changes

- `jobs`: `priority INTEGER`, `max_runtime INTEGER`, `idle_timeout INTEGER`,
  `queued_at INTEGER`; state gains `queued`.
- `devices`: `max_runtime INTEGER` (the declared ceiling).
- `workers`: `boot_id TEXT`.
- `reservations` (new): `job_id`, `device_id`, `created_at` — the head-job
  reservation that prevents starvation.

## Stage 2 testing

The queue's correctness is testable without hardware, on the injected clock:
FIFO within priority, reservations preventing starvation, cancellation while
queued, and — the invariant that must not regress — **no two live leases on
one device, ever**, now with queued contenders in the mix.

Watchdogs test against `sleep` and `yes`: a wall-clock trip, an idle trip, a
job that produces output steadily and is never tripped, and a job requesting
more than its device ceiling being rejected at submit.

Boot-ID recovery gets three end-to-end cases: changed ID → device `ready`;
unchanged ID → device `unhealthy`; unavailable → device `unhealthy`.

---

# Stage 3 — A fleet that describes itself

## Probes

Drop-in executables at `/etc/rc/probe.d/*.sh` print one flat JSON object to
stdout. Each gets a 5s timeout; a non-zero exit, a timeout, or malformed
output is logged and ignored rather than failing registration — a wedged
`nvidia-smi` must not stop a worker from starting.

Built-ins ship for the common cases: GPU model, total and free VRAM, driver
and CUDA version, CPU count, RAM, free disk, kernel. They run at startup and
every 5 minutes.

Probes report per-device where the fact is device-specific (VRAM) and per-host
where it is not (kernel, RAM); host-level facts are attached to every device
on that host.

## Labels and provenance

Every label carries a value, a source (`detected` or `declared`), and a
timestamp. A hand-written `vram=80G` that survives a card swap is worse than
no label, so `rc describe` shows how old each fact is and where it came from.
Declared labels in `worker.yaml` never overwrite a detected value for the same
key; they are reported alongside it and the conflict is visible.

## Selectors

`--select 'vendor=nvidia,vram>=40G'` — comma-separated conjunctions with `=`,
`!=`, `>=`, `<=`. Values compare numerically when both sides parse as numbers,
with `K`/`M`/`G`/`T` suffixes understood; otherwise as strings.

A selector queues against the **set** of matching devices and takes the first
that frees. This composes with Stage 2's queue and is the behaviour that lets
an agent stop naming hosts: `rc run --select 'vram>=40G' -- ./bench`.

`rc run --explain --select …` reports which devices match, how many are free,
and the queue depth, without submitting.

## Usage sheets

`/etc/rc/host.md` — Markdown with YAML frontmatter, the same shape as a skill
file, because that is what it is: the operating manual for that box. Pushed at
registration, size-capped at 64KB, cached by the controller. Optional
per-device overrides at `/etc/rc/host.d/<device>.md`.

Authored on the host so it is editable by whoever is standing in front of the
machine and versionable in that host's config repo. The controller may carry
an operator annotation layered on top; the host file wins on conflict.

A usage sheet is unenforced prose and will drift. `rc describe` therefore
shows its age, and anything load-bearing — a runtime ceiling, a device
tolerance — belongs in a label the scheduler reads, not in a paragraph an
agent may skim.

## `rc describe`

`rc describe gpubox:gpu0` prints labels with provenance and freshness, the
usage sheet, current state and holder, and recent job history. Output is
formatted to paste directly into an agent's context. `-o json` for machines.

This is what shrinks the agent-facing skill to one instruction: run
`rc describe` before writing commands for a box you have not used.

## `rc hold`

`rc hold gpubox:gpu0 --ttl 30m --reason "manual profiling"` takes an
interactive lease for the case the scheduler cannot cover — you need a shell
on the box. Same exclusivity, same visibility, same expiry, and it appears in
`rc devices` and the dashboard with its holder and reason.

It exists because without it people work around the system the moment they
need to poke at something, and stacked jobs return. It depends on Stage 2's
lease expiry: a hold that cannot expire is a GPU leaked by anyone who closes
their laptop. `rc release <lease>` ends one early.

## Stage 3 data model changes

- `device_labels` (new): `device_id`, `key`, `value`, `source`, `updated_at`.
- `host_docs` (new): `host`, `device_id` (nullable), `body`, `updated_at`.
- `leases`: `kind TEXT` (`job` | `hold`), `reason TEXT`.

---

# Stage 4 — Operations

## Verify probes

`/etc/rc/verify.d/*.sh` run after every job on that device, before it returns
to the pool: does `nvidia-smi` respond, are there leftover processes from that
job, is VRAM back under threshold. A non-zero exit marks the device
`unhealthy` with the probe's stderr as the reason.

This catches "job exited, VRAM still pinned" — the failure that boot-ID
recovery cannot help with, because nothing rebooted. Off unless a host ships a
script.

## Notifications

A configured webhook URL receives a JSON POST per event:

```json
{"event":"watchdog_trip","device":"gpubox:gpu0","job":"3df1acbe",
 "reason":"no output for 30m","at":"2026-08-13T20:47:23Z"}
```

Events: `watchdog_trip`, `device_unhealthy`, `worker_lost`, `job_lost`,
`verify_failed`, `lease_expired`.

Delivery runs on its own goroutine with bounded retries and backoff, and can
**never** block scheduling — an unreachable webhook must not stop jobs from
starting. Failures are logged and dropped after the retry budget.

## Dashboard actions

The pasted client token in `sessionStorage` covers reading and killing **your
own** jobs. Clearing an unhealthy device stays admin-only, and the page
prompts for an admin token *at the moment you click*, uses it for that one
request, and never stores it.

The credential living in the browser is therefore always the weaker one, and
the dangerous one exists only for the seconds it is in use. This matters
because the controller is reachable on the network and a bearer token in
`sessionStorage` is the softest target in the system.

---

## Non-goals across all three stages

- Multi-controller replication. One controller, one SQLite file.
- Fractional device sharing. Devices stay exclusive; nothing can enforce a
  declared VRAM budget.
- Shipping code to hosts. Jobs still run in checkouts that already exist
  there; the `payload` field stays reserved and unimplemented.
- Automatic retry of jobs lost to host failure.
- TLS termination. The controller stays behind a tunnel or private network.
- Per-worker authentication tokens. The ownership check continues to trust a
  caller-supplied `worker_id`, which defends against worker bugs but not
  against a malicious holder of the shared worker token. Closing that means
  per-worker credentials and is out of scope here.

## Decisions and rationale

| Decision | Why |
|---|---|
| `rc run` blocks by default | Matches `flock`; agents need no retry logic and cannot stampede. |
| Ctrl-C cancels a queued job, detaches a running one | Nothing is executing while queued, so there is no lease to protect. |
| Runtime ceilings declared per device | The limit belongs with the machine that cannot tolerate being exceeded; no global default is right for both a bench box and a training box. |
| A job over its device ceiling is rejected, not clamped | Silent clamping produces a run the submitter believes had a longer budget. |
| Boot ID distinguishes reboot from crash | A reboot proves the device is clean; a crash proves nothing. Without this, reflexive clearing becomes a habit. |
| Reboot-killed jobs are not retried | A half-finished training run restarting silently is worse than being told it died. |
| Dashboard read-only in Stage 2, actions in Stage 4 | Ships visibility fastest, and defers browser-held credentials until there is a considered auth story. |
| Admin token prompted per action, never stored | Keeps the strongest credential out of `sessionStorage` on a network-reachable service. |
| Lease expiry pulled into Stage 2 | `rc hold` is unsafe without it, and Stage 1 already writes the column. |
| Dashboard pulled into Stage 2 | It renders existing state; building it last means working blind through two stages. |
