# Resource Controller — Design

**Date:** 2026-08-13
**Status:** Approved for planning

## Problem

Agents, sessions, and people share a pool of hosts carrying special hardware
(GPUs today, other devices later). Work must not stack: two concurrent jobs on
one GPU means contended measurements at best and an OOM that reboots the box at
worst.

Today this is handled by a `flock` mutex on an agreed path (`/tmp/gpu`), taught
through the `sharing-a-gpu-with-flock` skill. That mechanism is correct on one
host and solves nothing else:

- **No multi-host story.** Each box has its own lock file. Nothing coordinates
  across the pool, and nothing tells an agent which box to use.
- **No visibility.** A lock file has no identity, no start time, and no record.
  You cannot tell who holds a device, for how long, or what they are running.
- **No liveness.** A holder that hangs holds forever. A job that exits leaving
  VRAM pinned looks free. `flock` cannot distinguish alive from wedged.
- **Bypassable.** The mutex only binds people who remember it, agree on the
  path, and whose ssh session survives. It fails silently when they don't.
- **No fleet knowledge.** Which host has the 80 GB card, how to invoke it, what
  it cannot tolerate — all of that lives in a skill file and in one person's
  head.

## Goal

A small scheduler for devices. Agents ask for a device by capability, get one
exclusively, run their work under supervision, and release it — with the full
state of the pool visible at all times, and stuck work detected and killed
rather than waited on.

## Non-goals

- Bin-packing, preemption, backfill, or fair-share accounting.
- Fractional device sharing (two jobs per GPU). Devices are exclusive.
- Shipping code to hosts. Jobs run in checkouts that already exist there.
- Replacing ssh for interactive work. `rc hold` coexists with it.
- Multi-controller replication. One controller, one SQLite file.

## Topology

Devices live on dedicated hosts. Agents run elsewhere — laptop, devbox, cloud —
and dispatch work. One always-on host runs the controller and holds all
authoritative state.

```
  agents (anywhere)                controller (always-on)          device hosts
  ─────────────────                ──────────────────────          ────────────
  rc run ─────── REST/SSE ───────▶ scheduler + SQLite ◀── REST ──── rc worker
  rc ps                            web dashboard         (long-poll) │
  rc describe                                                        └─▶ jobs
```

## Architecture

One Go binary, three roles.

### `rc serve` — controller

Runs on the always-on host. Owns all state in embedded SQLite (WAL mode).
Single-goroutine scheduler loop. Serves the REST API, the SSE event stream, and
the embedded web dashboard (assets compiled in; no build step at deploy).

**Assignment is one SQLite transaction**: `device: ready → busy` and
`job: queued → assigned` commit together or not at all. This transaction is the
mutex. Unlike a lock file it cannot be bypassed, swept from `/tmp`, or pointed
at a divergent path.

### `rc worker` — device host agent

Declares its devices from local config, then talks to the controller over plain
HTTP, always outbound. Device hosts need no inbound ports, no fixed address, and
may be ephemeral.

Responsibilities: register devices and the host usage sheet; run capability
probes; poll for assignments; spawn jobs in an isolated process group; stream
logs; enforce watchdogs locally; run verify probes between jobs; reap children.

### `rc <verb>` — client

Run from anywhere, including inside an agent session. Talks REST + SSE. A thin
client over the public API: anything the CLI does, a script or the dashboard can
do too.

## Transport

All HTTP. No persistent socket, no gRPC — every call passes through the same
proxies and tunnels that already carry ssh, and is reproducible with `curl`.

**Worker → controller:**

| Call | Purpose |
|---|---|
| `POST /v1/workers/register` | Devices, labels, usage sheet. Returns worker ID. |
| `GET /v1/workers/:id/assignments?wait=30s` | Long-poll. Returns on assignment, else 204. |
| `POST /v1/workers/:id/heartbeat` | Every 10s: device states, job progress, probe results. |
| `POST /v1/jobs/:id/logs` | Batched chunks, flushed at 1s or 64KB. |
| `POST /v1/jobs/:id/status` | Terminal state, exit code, kill reason. |

**Client → controller:** `POST /v1/jobs` (submit), `GET /v1/jobs/:id/logs`
(SSE), `POST /v1/jobs/:id/kill`, `POST /v1/leases`, `DELETE /v1/leases/:id`,
`GET /v1/devices`, `GET /v1/devices/:id` (describe), `GET /v1/state`,
`GET /v1/events` (SSE).

**There is deliberately no "is this device free?" endpoint.** A claim is a
single atomic call that returns a running job or a queue position. Nothing in
the API composes into check-then-act. `GET /v1/devices` reports state for
display only; it can never be the first half of an allocation.

If assignment latency becomes a problem, the long-poll can become a WebSocket
without changing anything else.

### Authentication

Bearer token per role. Worker tokens may register devices and report job state;
client tokens may submit jobs and take leases; admin tokens may clear devices
and force-kill. Deployed behind an existing tunnel or private network — this
system does not invent a security perimeter.

## Data model

### Device

`id` (`host:name`, e.g. `gpubox:gpu0`), `host`, `labels` (map with per-key
provenance and timestamp), `state`, `last_heartbeat_at`.

States: `ready`, `busy`, `draining`, `unknown`, `unhealthy`.

- `unknown` — worker not reporting, within grace. Not schedulable.
- `unhealthy` — worker lost past grace, or a verify probe failed. Not
  schedulable until cleared.
- `draining` — operator-initiated; finishes current job, accepts no more.

### Worker

`id`, `host`, declared devices, `last_heartbeat_at`, version.

### Lease

`id`, `device_id`, `holder`, `acquired_at`, `expires_at`, optional `job_id`,
`reason`. Exactly one live lease per occupied device. The single unit of
exclusivity — job leases and interactive leases are the same object.

### Job

`id`, `selector`, `count`, `command` (argv), `env`, `cwd`, `priority`,
`submitter`, `idempotency_key`, `max_runtime`, `idle_timeout`, `state`,
`device_ids`, `worker_id`, `submitted_at`, `started_at`, `finished_at`,
`exit_code`, `kill_reason`, `log_ref`.

States: `queued → assigned → running → succeeded | failed | killed | lost`.

## Scheduling

Strict FIFO with priority; first-fit against the label selector. A job requesting
N devices allocates all-or-nothing, and the queue head **reserves** devices as
they free so a multi-device job does not starve behind a stream of single-device
jobs.

Selectors match labels with equality and comparison (`vendor=nvidia`,
`vram>=40G`), or pin a device by ID.

No bin-packing, no preemption, no backfill. These are where schedulers become
subtle; add backfill later only if queue behaviour demands it.

## Discovery and the host usage sheet

Two kinds of facts, separated because they rot differently.

### Structured labels — machine-maintained

The worker runs probes on start and every 5 minutes, reporting what it finds:
GPU model, VRAM, driver and CUDA version, CPU, RAM, free disk, kernel. Built-in
probes cover the common cases; drop-in scripts at `/etc/rc/probe.d/*.sh` emit
JSON for anything else, so a capture card, a USB dongle, or an ARM board is
described the same way without a controller change.

Every label carries provenance (`detected` or `declared`) and a timestamp.
Hand-written labels go stale and lie silently; auto-detection is what keeps
`vram=80G` true after someone swaps a card.

### Usage sheet — human-authored, travels with the machine

`/etc/rc/host.md` on each host, with optional per-device overrides, pushed to
the controller on registration and cached there. Markdown with YAML frontmatter
— deliberately the same shape as a skill file, because that is what it is: the
operating manual for that box. Ssh alias, where datasets live, env that must be
set, build quirks, and the warnings that matter ("110 GB unified memory — two
stacked benches OOM and the box reboots").

Authored on the host so it is editable by whoever is standing in front of the
machine and versionable in that host's config repo. The controller caches it and
may carry an operator annotation layered on top, but the host file wins.

A usage sheet is unenforced prose and will drift. `rc describe` therefore shows
its age and last edit, and anything load-bearing — max job duration, device
tolerances — belongs in labels the scheduler reads, not in paragraphs an agent
may skim.

### Agent-facing discovery

- `rc devices` — table: device, host, labels, state, holder, elapsed, queue depth.
- `rc devices -o json --selector 'vram>=40G'` — machine-readable selection, so an
  agent picks a target instead of guessing a hostname.
- `rc describe gpubox:gpu0` — labels with provenance and freshness, the usage
  sheet, current state, recent job history. Formatted to paste into agent context.
- `rc run --explain` — which devices match, how many are free, queue depth,
  before committing.

## Failure handling

The governing invariant: **never hand out a device we cannot prove is free.**

**Client dies or ssh drops.** Nothing happens to the job. The worker owns the
process, so a disconnected `rc run` is a lost terminal, not a lost run; reattach
with `rc attach <job>`. This removes the largest class of today's failures, where
the lock's lifetime was tied to a fragile session.

**Worker dies or stops reporting.** Its devices go to `unknown`, not `ready`.
Within a 60s grace window the worker may reconnect and reconcile still-running
children by job ID. Past that, jobs become `lost` and devices become
`unhealthy` — deliberately not schedulable, because a dead worker tells us
nothing about whether something still occupies the device. Clearing requires a
returning worker reporting the device clean, or an explicit `rc device clear`.

**Controller down.** Workers keep supervising, buffer logs, and retry. No new
assignments occur. Fail-closed: work stalls, jobs never stack.

**Job hangs.** Two watchdogs, both enforced on the worker so a controller outage
cannot leave a job running forever:

- `max_runtime` — hard wall-clock ceiling.
- `idle_timeout` — no output for N minutes.

Ceilings come from device labels. A job may request less than a host's maximum,
never more: the limit lives with the machine that cannot tolerate exceeding it.
On trip: SIGTERM to the process group, grace period, then SIGKILL; state
`killed` with the reason recorded and an event emitted.

**Kills that actually kill.** Jobs run in their own process group, and in a
systemd transient scope where available, so a kill takes down the whole tree. A
SIGKILL to a single pid leaves CUDA children alive holding VRAM — precisely how
a device ends up occupied with nothing visibly running.

The old skill's "never kill jobs you did not start" stops being an honor-system
rule: `rc kill` accepts job IDs only, and only ones the caller owns (admin
tokens excepted).

**Verify probes.** Optional drop-in scripts at `/etc/rc/verify.d/*.sh` run after
every job on that device: nvidia-smi responds, no leftover processes from that
job, VRAM back below threshold. A failure marks the device `unhealthy` instead
of returning it to the pool. This is the only check that catches "job exited,
VRAM still pinned" before the next job OOMs the box. Off unless a host ships a
script.

## Client interface

```
rc run --select 'vendor=nvidia,vram>=40G' -- ./bench --args   # blocks, streams, exits with job's code
rc run -d gpubox:gpu0 --max-runtime 30m -- ./bench            # pin a device
rc run --explain --select 'vram>=40G'                         # what would this get me
rc submit ... ; rc attach <id> ; rc logs -f <id> ; rc kill <id>
rc hold gpubox:gpu0 --ttl 30m --reason "manual profiling" ; rc release <id>
rc ps ; rc devices ; rc describe <device> ; rc status
rc device drain|clear <device>                                # admin
```

`rc run` is the blocking, flock-shaped path, implemented over submit + attach.
Ctrl-C cancels the job, matching the `flock` mental model; `--detach`
backgrounds it. Exit code, stdout, and stderr pass through faithfully, so it
drops into scripts and CI unchanged.

Jobs run in an existing checkout on the device host: `--cwd /path`, mirroring
today's ssh workflow. The job spec reserves a `payload` field so client-side
sync can be added later without breaking clients.

The job environment receives `RC_JOB_ID`, `RC_DEVICE`, and `CUDA_VISIBLE_DEVICES`
set from the assignment. Scheduling a device is meaningless if the process is
free to touch every GPU on the box.

**Identity.** Token plus a claimed label (`--as`), defaulting to
`user@host/<session id>` from the environment. This is what makes `rc ps` and the
dashboard legible — "claude-session-4f2a holding gpubox:gpu0, 14m, ./bench"
rather than an anonymous lock file. Submits carry an idempotency key so an agent
retrying through a network blip does not queue twice.

**Interactive leases.** `rc hold` exists because the scheduler cannot cover
every case — sometimes you need a shell on the box. Same exclusivity, same
visibility, same expiry. Without it people work around the system the moment
they need to poke at something, and stacked jobs return.

## Web dashboard

Served by the controller. The browser never contacts device hosts; the
controller already knows everything because workers report into it. Flow:
worker heartbeat → SQLite → controller → browser.

The page loads a snapshot from `GET /v1/state`, then subscribes to
`GET /v1/events` (SSE) for deltas: device state changes, job transitions,
watchdog trips, and log lines for an open job. A 5s poll backs it up if the
stream drops.

Layout: device grid coloured by state, each tile showing holder, elapsed, the
running command, and watchdog margin. Queue below. Click through to live logs.
Unhealthy devices and watchdog kills pin to the top — those are the states
currently invisible.

**Staleness is a first-class signal.** Every device tile shows the age of its
last heartbeat, so a worker that stopped reporting greys out with "no contact
47s" rather than looking healthy. Job tiles show elapsed against watchdog
margin, so "running 8m of a 30m limit, last output 6m ago" is legible before it
becomes a dead box.

A history view lists past jobs with exit codes and durations, doubling as the
record of which benchmark ran under what conditions.

`rc ps` prints the same information as a table.

## Skill replacement

`sharing-a-gpu-with-flock` becomes `using-the-resource-controller`. Two rules
survive because they do real work:

1. **One job = one whole measurement.** An A/B stays inside a single job, or it
   is void.
2. **A number is valid only if it ran as a scheduled job.**

Everything else — path agreement, `fuser`, kill etiquette, dispatch boilerplate
— drops out, because the system enforces it. The new instruction for unfamiliar
hardware is one line: run `rc describe` before writing commands for a box you
have not used.

## Testing

The correctness core is testable without hardware: an injected clock plus fake
workers, asserting **no two live leases on one device, ever** across crashes,
partitions, reconnects, expiry races, and reservation handling. Table-driven and
fast; this is the suite that matters.

Worker supervision — process groups, kill trees, watchdog trips, log batching —
tests against `sleep` and `yes`. No GPU required.

One end-to-end test runs a real controller, a real worker, and a fake device
through submit → assign → run → release.

## Staging

Each stage is useful on its own.

1. **Core.** Controller, worker, `rc run`, `rc ps`, exclusive leases, no queue.
   Agents stop stacking; you can see who holds what. Fixes today's pain.
2. **Scheduling.** Queue, priorities, watchdogs, `rc kill`, `rc attach`.
3. **Fleet knowledge.** Probes, usage sheets, `rc describe`, `rc hold`.
4. **Operations.** Web dashboard, verify probes, notifications on watchdog trips
   and unhealthy devices.

## Decisions and rationale

| Decision | Why |
|---|---|
| SQLite on one box | Single-writer transaction is the mutex. Correct, simple, inspectable. Controller is a hard dependency for starting work — accepted. |
| Worker dials out | Device hosts need no inbound ports and may be ephemeral. |
| Plain HTTP, no gRPC | Passes through existing tunnels, debuggable with `curl`. Revised from an earlier gRPC proposal as over-engineering for this fleet size. |
| Lease held by worker, not client | Session death stops being lock death — the root cause of today's silent failures. |
| Watchdogs on the worker | A controller outage must not leave jobs running forever. |
| No "is it free?" endpoint | Prevents reintroducing check-then-act at the HTTP layer. |
| Exclusive devices only | Fractional sharing cannot be enforced; a job exceeding its declared VRAM OOMs its neighbour. Schema leaves room to add capacity later. |
| Usage sheet on the host | The manual travels with the machine and survives a controller rebuild. |
| CLI as the agent surface | Teachable in three lines. MCP server possible later over the same API. |
