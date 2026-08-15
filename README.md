# resource-controller

Exclusive leases for shared hardware, across hosts, with the state visible.

Replaces a `flock` file mutex: a central controller owns allocation in a
single SQLite transaction, workers on device hosts supervise the jobs, and
`rc ps` / `rc devices` show who holds what.

## Scope

Exclusive device leases, supervised job execution, live log streaming, fleet
visibility, a queue with priorities, per-device runtime watchdogs, lease
expiry, boot-identity recovery, server-side cancellation (`rc kill`),
re-attaching to a running job's output (`rc attach`), a live SSE event
stream, a read-only web dashboard, lease lifecycle hooks (stop/start a
service like LocalAI or ollama around a job's hold on a device), capability
probes and labels with provenance, selectors (`--select`), per-host usage
sheets and `rc describe`, and `rc hold`/`rc release` for claiming a device
for a human rather than a job.

**Not built yet**, so you will not find them documented below:

| Not built yet | What you do instead today |
|---|---|
| Verify probes between jobs | Nothing checks VRAM was released after a job |
| Webhook notifications | Poll `rc ps` / `rc devices`, or watch `/v1/events` |
| Dashboard actions (kill/hold from the browser) | `rc kill` from a terminal; the dashboard is read-only |

## Controller

```sh
export RC_TOKENS='wtok:worker,ctok:client,atok:admin'
rc serve --addr :8080 --data /var/lib/rc
```

`RC_TOKENS` is a comma-separated list of `token:role` pairs, roles are
`worker`, `client`, or `admin`. It is validated at startup: a malformed
entry, an unknown role, an empty token, or the same token listed twice (even
with different roles) is rejected before the controller binds a socket —
for example:

```
$ RC_TOKENS='wtok:worker,wtok:client' rc serve --addr :8080 --data /var/lib/rc
rc: duplicate token "wtok" in RC_TOKENS
```

`--data` holds the SQLite database and the append-only per-job log files;
back that directory up if you care about job history.

**`rc serve` is doing work even when idle.** Besides answering requests, it
runs two background loops for as long as it's up: a reaper sweeps every 10s
for workers that have gone quiet, and a scheduler sweeps every second for
queued jobs that can now be handed to a device that just freed up (a job
finishing, a worker re-registering). The scheduler is also why a submit onto
a free device is assigned immediately without waiting for that sweep — every
submit makes its own scheduling pass too — but it is the periodic loop that
notices a device freeing up *later* and hands the next queued job to it.

**Upgrade the controller and all workers together — they are not
independently upgradeable.** A stale worker speaking the old wire format
fails to poll a newer controller right away, and any jobs it still had in
flight get marked `lost`, quarantining their devices `unhealthy` until an
admin clears each one. Nothing is double-allocated in the meantime — an
`unhealthy` device is unschedulable — so recovery is just one
`POST /v1/devices/{id}/clear` per affected device.

### Running the controller in Docker

The controller is the piece worth keeping always-on, and it is cgo-free, so
the image is a static binary on Alpine.

```sh
cp .env.example .env      # then edit it — see below
docker compose up -d
docker compose ps         # should report (healthy)
```

`.env` holds the tokens and is gitignored:

```
RC_TOKENS=<worker-token>:worker,<client-token>:client,<admin-token>:admin
```

Generate real values with `openssl rand -hex 24`. The controller refuses to
start without `RC_TOKENS` — an unauthenticated scheduler that can execute
commands on your GPU hosts is not something to fall back to silently.

State lives in the `rc-data` volume (`/var/lib/rc` inside the container).

The port is published on `0.0.0.0`, because workers and clients live on
other machines and have to reach it.

That makes `RC_TOKENS` the only thing between your network and a service
that runs commands on your GPU hosts. Two consequences worth taking
seriously: use generated tokens (`openssl rand -hex 24`), never the values
in `.env.example`; and put the controller behind a tunnel (Cloudflare,
Tailscale), a VPN, or a private network, because bearer tokens cross the
wire in the clear and the controller terminates no TLS of its own. On a machine
with a public interface, bind it back to a private address or a loopback
plus tunnel instead.

**Only the controller belongs in Docker.** A worker must see and signal the
process group that touches the hardware, so it runs on the device host
itself, not in a container.

## Device host

Write `/etc/rc/worker.yaml` (see `examples/worker.yaml`):

```yaml
controller_url: https://rc.internal.example
token: replace-with-worker-token
# host defaults to the machine's hostname; device IDs become <host>:<name>
probe_dir: /etc/rc/probe.d      # optional; default shown
probe_interval: 5m               # optional; default shown
sheet_dir: /etc/rc               # optional; default shown — host.md lives here
devices:
  # Object form: max_runtime is a per-device runtime ceiling the controller
  # enforces at submit time and the worker enforces as a wall-clock watchdog
  # while the job runs. A job that asks for more than this (rc run
  # --max-runtime, or the API's max_runtime_seconds) is REJECTED at submit
  # time, not silently clamped down to fit — see "Guarantees" below.
  # A job that requests no ceiling of its own inherits this one.
  - name: gpu0
    max_runtime: 6h
    # Declared labels: operator-asserted facts a probe cannot detect on its
    # own. See "Labels and probes" below for how these interact with what a
    # probe reports for the same key.
    labels:
      rack: r3
      tier: prod
  # Plain name form still works: it is shorthand for {name: gpu1} with no
  # ceiling, no declared labels, and no hooks, so jobs on gpu1 run unbounded
  # unless they set their own --max-runtime.
  - gpu1
heartbeat_interval: 10s
poll_wait: 30s
```

Then:

```sh
rc worker
# or: rc worker --config /path/to/worker.yaml
```

That file, plus whatever a probe detects at runtime, is the whole of a
node's configuration. There is no device auto-discovery: the controller
knows only the device names you list. If you list `gpu0` and `gpu1` on a
four-GPU box, the other two do not exist as far as the scheduler is
concerned — but VRAM, vendor, driver version and the rest of what a probe
can see about the devices you *did* list are gathered automatically; see
below.

**Name devices after their GPU index.** The worker sets
`CUDA_VISIBLE_DEVICES` from the trailing integer of the device name, so
`gpubox:gpu1` runs its job with `CUDA_VISIBLE_DEVICES=1`. A name with no
trailing integer leaves the variable unset rather than guessing an index —
and an unset `CUDA_VISIBLE_DEVICES` means the job can see every GPU on the
box, which is how two correctly-leased jobs end up on one card. An
operator-supplied value in the job's own environment is never overridden.

The worker registers its host and devices, long-polls the controller for
assignments, spawns each job in its own process group, and streams the
job's combined stdout/stderr back as it runs.

**Stopping or restarting the worker terminates its in-flight jobs — it does
not let them survive the restart.** On shutdown (SIGINT/SIGTERM) the worker
stops taking new work and SIGTERMs the process group of every job it has
running here, escalating to SIGKILL after a 10s grace ceiling if the group
hasn't exited by then. It then waits (up to 45s) for those jobs to actually
die and for their terminal report — job state `killed`, reason `cancelled`
— to reach the controller before the process exits. That ordered shutdown
exists so the controller learns the job's real outcome and releases the
device cleanly, not so the job survives; a `systemctl restart` (or any other
SIGTERM) of `rc worker` during a multi-hour job kills that job. There is no
way to detach a job from a specific worker process and hand it to a
replacement — the worker that started a job is the one supervising it for
its entire life.

**A worker that never got to run that shutdown sequence — killed -9, an OOM
of the worker process itself, a crashed host — leaves the controller
believing the job is still assigned or running.** Registration reconciles
this: when a worker registers, it is announcing (truthfully, since it is a
fresh process) that it has no running jobs, so any job the controller still
has this worker ID down as `assigned` or `running` is marked `lost` and its
lease released. What happens to the *device* next depends on whether the
worker can prove the host actually rebooted:

- **A worker process restart without a reboot quarantines `unhealthy` only
  the devices that had a stranded `assigned`/`running` job on them — a
  device that was idle when the worker died comes back `ready`, same as
  before.** For a device that did have a live job, the worker sends the
  same boot ID (`/proc/sys/kernel/random/boot_id`) it always has, which
  proves nothing about whatever the old process left running — an orphaned
  CUDA process from a `kill -9`'d worker can still be pinning that GPU. That
  device is never handed back to the pool on the strength of "the old
  process is gone now"; it stays `unhealthy` until an operator clears it
  explicitly: `POST /v1/devices/{id}/clear` (that call only succeeds when
  the device is currently `unhealthy` **and** has no live lease).
- **A host that reboots gets its devices back `ready` automatically, however
  long it was down.** A changed boot ID at registration is proof the machine
  actually restarted, so nothing from before the reboot can still be running
  or holding VRAM — a reboot is the one event that positively proves a device
  is clean, not just that a process went quiet. This covers the ordinary
  case of a box rebooted overnight or held down for maintenance, where the
  reaper has long since quarantined every device on it: recovery is decided
  by *why* each device was quarantined, not by whether a job was still in
  flight when the host came back.
  What a reboot does **not** do is resurrect a device that was quarantined by
  a self-reported hardware fault. A reboot proves no process survived; it
  proves nothing about the hardware. So a device the host itself reported
  faulty stays `unhealthy` until a human clears it, and so does any device
  quarantined before this bookkeeping existed (an upgrade from an older
  database records no reason, and an unknown cause is never assumed benign).

A worker with no boot ID at all (the `/proc` file unreadable, e.g. inside
some containers) is treated the same as an unchanged one: no proof, so it
quarantines rather than guesses.

The job's environment receives `RC_JOB_ID` and `RC_DEVICE` (the assigned
device ID), plus `CUDA_VISIBLE_DEVICES` derived by convention: **device
names should end in the GPU's index**, e.g. `gpu0`, `gpu1`. The worker takes
the trailing integer off the device name (the part after `host:`), so a job
assigned `gpubox:gpu1` runs with `CUDA_VISIBLE_DEVICES=1`. If a device's
name has no trailing integer, the worker leaves `CUDA_VISIBLE_DEVICES`
unset rather than guess wrong — silently setting the wrong index would let a
job touch a GPU it was never leased, which is worse than setting nothing. An
operator-supplied `CUDA_VISIBLE_DEVICES` already present in the job's own
`env` is never overridden.

### Labels and probes

Every device carries labels — key/value facts the scheduler can match a
`--select` against — from two sources, both kept and both visible:

- **Declared**: what an operator wrote under a device's `labels:` in
  `worker.yaml` (see the example above). Pushed verbatim on every
  registration; remove a key from the file and restart, and it is gone
  server-side too.
- **Detected**: what a probe found at runtime.

**Detected always wins a same-key conflict for scheduling purposes, but
neither value is discarded.** A `--select rack=r3` still sees the declared
`rack`, but if a probe ever reports its own `rack` label, that detected
value is what a selector actually matches against, and `rc describe` shows
both side by side so a stale or wrong declared value is visible instead of
silently masked.

A device's detected labels come from two kinds of probe, both run under
`probe_interval` (default 5m) and bounded by `probe_timeout` (default 5s):

- **Built-in**, no configuration needed: `cpus`, `mem_total_bytes`,
  `disk_free_bytes`, and `kernel` are host-wide facts gathered directly.
  If `nvidia-smi` is on the worker's `PATH`, it also runs
  `nvidia-smi --query-gpu=name,memory.total,driver_version` and turns each
  row into `gpu<N>.vendor`, `gpu<N>.model`, `gpu<N>.vram` (as a `K/M/G/T`
  quantity, e.g. `24576M`, so selectors can compare it numerically), and
  `gpu<N>.driver` — only for device names this host actually declared as
  `gpu<N>`.
- **Drop-in**: any executable file in `probe_dir` (default `/etc/rc/probe.d`),
  run in sorted filename order on top of the built-ins. Each probe must
  write **one flat JSON object to stdout** — string, number, and boolean
  values are accepted; `null` is dropped; a nested object or array value is
  dropped with a warning; anything that isn't exactly one JSON object
  (garbage, an array, a second value trailing the first) fails the whole
  probe. A bare key (`"vendor"`) is a host-wide fact; a `"<device>.<key>"`
  key (`"gpu0.vendor"`) targets that one device and is accepted only if
  the name after the dot matches a device this host actually declared.

**A probe that fails, times out, or emits something other than that one
flat JSON object costs a label, never a worker.** It is logged and
skipped; whatever else succeeded that pass is kept, and startup,
registration, and every later probe pass proceed regardless — a wedged
`nvidia-smi` or a broken drop-in script never blocks a worker from coming
up or serving jobs.

The one deliberate exception to "a probe that stops reporting a key just
means that fact is gone": if `nvidia-smi` was present on `PATH` when this
worker process started and later disappears (a driver upgrade mid-run is
the canonical case), that device's previously-detected labels are
**preserved** rather than cleared, because wiping them is a fleet-wide
guess and a stale label is scoped to one device. If `nvidia-smi` was never
present since this worker started, its absence is not a failure and
labels clear normally, the same as any other probe that stops reporting.

**Probes, like lease lifecycle hooks, run operator-supplied scripts as the
worker user with no additional sandboxing** — the same blast radius as any
other script the worker executes on your behalf, so trust `probe_dir`
exactly as much as you trust `on_acquire`/`on_release`.

### Selectors

`--select` (on `rc run` and `rc hold`) targets a device by its labels
instead of a device ID, e.g. `--select 'vendor=nvidia,vram>=40G'`: a
comma-separated conjunction of terms, every term must hold. Supported
operators are `=`, `!=`, `>=`, and `<=` (no bare `>`/`<`). When both sides
of a comparison parse as a quantity — a plain number, or one suffixed
`K`/`M`/`G`/`T` (case-insensitive, powers of 1024) — the comparison is
numeric, so `vram>=40G` matches `80G` and `81920M` alike; otherwise it
falls back to a lexicographic string comparison. A term whose key is
absent from a device's labels never matches, `!=` included: an absent
label is not proof the device differs.

**A selector matching no device right now is rejected outright at submit
time — not queued in case one registers later.** A typo in `--select` is
far more common than that bet paying off, and a job queued indefinitely
behind a selector that will never match is indistinguishable from a hang:

```
$ rc run --select 'vendor=intel' -- true
rc: no_matching_device: no device matches the selector: vendor=intel
```

**A selector job sitting at the head of the queue reserves every device it
currently matches, not just one.** Each scheduling pass, a queued selector
job that cannot be placed holds all of its still-unclaimed matching
candidates so a job behind it can never jump ahead onto one of them; a
broad selector (`vram>=1G`, say) waiting for its ceiling or its turn can
therefore hold up every device it matches, not just the one it eventually
lands on. Prefer a selector narrow enough to name only the devices you
actually want in contention with each other.

`rc run --explain --select <selector>` reports which devices match, how
many are currently free, and the queue depth, without submitting anything
— useful for checking a selector before committing a job to it.

### Usage sheets and `rc describe`

A **usage sheet** is a plain Markdown file of host or per-device notes —
whatever an operator wants the next person (or agent) to see before
touching a device: known quirks, contact info, "don't run anything over
4h here", whatever. The worker reads `<sheet_dir>/host.md`, a host-wide
note, and, per device, `<sheet_dir>/host.d/<device-name>.md`, that
device's own; either file simply being absent is not an error. `sheet_dir`
defaults to `/etc/rc`. Each sheet is capped at **64KB**: a worker exceeding
that truncates before pushing it (the controller also enforces the same
cap server-side as a second line of defence), so a hand-edited `host.md`
can never grow the database unboundedly. The cap is per sheet, not per
registration.

**A device with no `host.d/<device-name>.md` of its own falls back to
`host.md`.** Writing one `host.md` for a box is the common case, and
`rc describe` shows it for every device on that host until (and unless)
that device gets a sheet of its own; `sheet_is_host_wide` in the JSON
output (and the "device note"/"host-wide note" label in the text output)
says which one actually landed.

`rc describe <device-id>` is the one command that shows everything known
about a device before you write a command for it: its state and current
holder, every label grouped by key with its source and how long ago it was
last confirmed (a conflicting declared/detected pair is shown side by
side, never one hiding the other), its usage sheet and the sheet's own
age, and recent job history. `-o json` prints the same response as JSON
instead of a table, for scripting. Every age shown — a label's, the
sheet's, the heartbeat's — is "how long ago was this last confirmed", not
"how long has this device existed"; an old label age is your signal that
whatever probe reports it may not have run in a while.

### Lease lifecycle hooks

A device is often not empty just because no job holds it: an inference
server (LocalAI, ollama) can sit on the VRAM a job needs. `on_acquire` /
`on_release` on a device in `worker.yaml` let the worker stop that service
before a job touches the device and start it again once the device is
genuinely idle:

```yaml
hooks:                    # host-level defaults, both optional
  timeout: 60s             # built-in default if omitted
  release_linger: 30s      # built-in default if omitted
devices:
  - name: gpu0
    on_acquire: /etc/rc/hooks/stop-localai.sh
    on_release: /etc/rc/hooks/start-localai.sh
    timeout: 60s            # per-device override of hooks.timeout
    release_linger: 45s     # per-device override of hooks.release_linger
```

`on_acquire` and `on_release` are both optional and independent — a device
can set either, neither, or both. Each is a path to a script, run under the
same process-group supervision a job gets (its own process group, killed
whole if it runs past `timeout`), with `RC_EVENT` (`acquire` or `release`),
`RC_DEVICE`, `RC_JOB_ID`, `RC_SUBMITTER`, and `CUDA_VISIBLE_DEVICES` (derived
the same way a job's is) in its environment.

**Treat `RC_SUBMITTER` (and `RC_JOB_ID`) as untrusted input.** The submitter
is free text chosen by whoever submitted the job; it is passed to the hook
as an environment variable and never through a shell, so nothing is
injectable today, but a hook that interpolates it into a shell command
(`eval`, backticks, an unquoted `$RC_SUBMITTER` in a `sh -c` string) makes
it one. Quote it, or better, only ever log it.

**The release is lingered, not immediate, and that is what makes "held" a
per-device state instead of a per-job event.** When a job ends, the worker
doesn't run `on_release` right away — it arms a timer for `release_linger`
later. If another job lands on the same device before that timer fires, the
pending release is cancelled *and* that next job's own `on_acquire` is
skipped: the device was never actually released, so stopping and restarting
the service in between would be pure churn. Net effect: a burst of
back-to-back jobs on one device produces exactly one `on_acquire` (before
the first job touches it) and one `on_release` (once the device has
genuinely sat idle for `release_linger`) — not one pair per job.

The linger itself never delays scheduling: the controller frees the device
the moment the job's terminal report lands, and a queued job is scheduled
against it immediately, whether or not `on_release` has run yet. **A hook
already executing does hold that device's next job up, though**, for as long
as it runs (up to `timeout`): the worker serialises one device's hooks
against each other, so a job landing while an `on_release` is mid-flight
waits for it to finish before its own `on_acquire` is even considered. That
is what makes "one acquire per burst" decidable at all — the alternative is
an acquire and a release for the same device running at the same time,
fighting over the same service. Devices never wait on each other; only jobs
for the *same* device do. Keep hooks quick, and keep `timeout` no higher
than the delay you are willing to add to a queued job.

**A job whose `on_acquire` is still running when the worker is asked to stop
(`systemctl restart rc-worker`, or `rc kill` on that job) is reported
`killed`, not `failed`, and its device is not quarantined.** An interrupted
hook is our own doing and says nothing about the device. Note that the
service the hook was in the middle of stopping may be left stopped — the
next start's reconciliation pass (below) puts it back.

**An `on_acquire` failure (non-zero exit or a timeout) fails the job before
it ever runs**, with the hook's own output as the failure reason (so it
shows up in `rc ps`), and quarantines the device via
`POST /v1/devices/{id}/fault` — a `fault` quarantine, the one kind a proven
reboot deliberately does **not** clear (see "Device host" above): a reboot
proves no process survived, but proves nothing about failing hardware or a
service the hook could not stop. Only an admin's explicit
`POST /v1/devices/{id}/clear` puts it back in the pool.

**A failed `on_acquire` schedules no `on_release`, and may well leave the
service it was stopping stopped.** The hook failed somewhere — quite
possibly after it had already stopped LocalAI — and the worker will not
guess that it got far enough to be worth undoing: nothing is "held", so
there is nothing to release. Since the device is quarantined at the same
moment, no job lands on it either way. Bringing the service back is part of
clearing the fault: run the release hook by hand, or simply restart the
worker (its startup pass runs every declared `on_release`) once the
underlying problem is fixed.

**An `on_release` failure is logged loudly but never quarantines the
device.** The job that held it is already done, the device is genuinely
free, and a service failing to come back up is an operator problem to
notice — not a reason to pull hardware out of the pool.

**At startup, before its first poll, the worker runs the `on_release` hook
of every device that declares one.** This is unconditional — it runs on
every start, not just after a crash — so a worker that crashed mid-job (or
was `kill -9`'d, or the host itself crashed) heals its own node's stopped
service without anyone having to notice and intervene. **This is exactly
why the release hook must be idempotent — safe to run when the service is
already up:** the worker starting up has no way to know whether the service
it targets is already running or not, and runs the hook regardless. The pass
is bounded as a whole (two minutes, across every device), so a wedged hook
delays this worker's first poll by a known amount instead of holding up
startup indefinitely; whatever it cuts short the next start attempts again.
The heartbeat starts before the pass, so the controller never marks this
worker's own devices unknown while it runs.

**Stopping the worker fires any pending release before it exits.** A job
that ended seconds before a `systemctl stop` leaves a release armed but not
yet fired; the worker runs it on the way out (bounded, so it cannot stretch
the drain indefinitely) rather than leaving the node with the operator's
service stopped and no job running — a state nothing but a restart would
heal.

## Client

```sh
export RC_CONTROLLER=https://rc.internal.example
export RC_TOKEN=ctok

rc devices                              # who holds what, and what has gone quiet
rc ps                                   # running/assigned jobs
rc run -d gpubox:gpu0 --cwd /src -- ./bench --args
```

`rc run` claims the device, blocks, streams the job's combined stdout/stderr
to your terminal, and exits with the job's own exit code — so it drops into
scripts wherever `flock /tmp/gpu -c '...'` used to sit:

```
$ rc run -d gpubox:gpu0 -- sh -c 'echo hello; exit 3'
rc: job ad3cd51f-3593-4a8b-9cf1-61ccbb803464 on gpubox:gpu0
hello
$ echo $?
3
```

**`rc run` blocks by default: if the device is busy, it queues instead of
failing.** It prints the queue position as it changes and starts streaming
as soon as a device frees up and the job is scheduled onto it — this is a
real queue behind the device, not a client-side retry loop:

```
$ rc run -d gpubox:gpu0 -- true
rc: queued at position 1 for gpubox:gpu0
rc: job 24880200-a51a-479e-b668-f9a801b2ad4f on gpubox:gpu0
```

Two flags change that default. `--no-wait` restores the old fail-fast
behaviour — a busy device is refused immediately instead of queuing:

```
$ rc run -d gpubox:gpu0 --no-wait -- true
rc: gpubox:gpu0 is busy and could not be queued
```

`--timeout` bounds how long `rc run` will wait in the queue before giving up
and cancelling the job on its own — useful in scripts that would rather fail
fast than wait indefinitely behind someone else's job:

```
$ rc run -d gpubox:gpu0 --timeout 3s -- true
rc: queued at position 1 for gpubox:gpu0
rc: cancelled queued job 18b9e6e0-4900-47be-b2c0-392f3746b83d
rc: gave up waiting for gpubox:gpu0: context deadline exceeded
```

**Ctrl-C on a QUEUED job cancels it.** Nothing is running yet, so there is
no lease to protect — cancelling is always safe, and `rc run` reports the
cancellation and exits 130:

```
$ rc run -d gpubox:gpu0 -- true
rc: queued at position 1 for gpubox:gpu0
^C
rc: cancelled queued job 65a95e7a-b76b-463a-b718-44c26df58573
```

**Ctrl-C on a RUNNING job only detaches — it does not cancel it.** The
worker, not the client, owns the process and its lease once a job is
running, so losing the terminal (or pressing Ctrl-C on purpose) must never
free a GPU that a process is still using. The job keeps running and keeps
holding the device; `rc run` prints where things stand and exits 130:

```
$ rc run -d gpubox:gpu0 -- sh -c 'sleep 30'
rc: job e8720e80-ccf2-4b06-a442-c1348553585e on gpubox:gpu0
^C
rc: detached from job e8720e80-ccf2-4b06-a442-c1348553585e — it is STILL RUNNING on gpubox:gpu0 and holds it until it finishes. Watch it with: rc ps
```

That distinction — cancel while queued, detach-only while running — is
surprising until you know the reason: nothing is holding a device for a
queued job, but a worker process really is holding one for a running job,
and a dropped terminal must never be indistinguishable from "safe to free
the GPU".

`rc kill` reaches further than Ctrl-C: it works from any terminal (not just
the one that ran `rc run`) and it actually terminates a *running* job, not
just a queued one — the controller flags it and the worker SIGTERMs the
process group, the same path an ordinary shutdown uses.

**But `rc kill` is not a free-for-all: only the job's own submitter, or an
admin token, may kill it.** The controller checks the `submitter` on the
kill request against the job's recorded submitter (`defaultSubmitter()`
embeds `$USER@$HOSTNAME`, plus a session ID when run as an agent) and
refuses everyone else with 403, even from a perfectly valid client token:

```
$ rc kill 707b485b-8a91-4350-a327-d39df63250b3
rc: not_job_owner: only the submitter or an admin may kill this job
```

That matters because the identity that submitted a job is very often not the
operator who later needs to kill it — an agent submitted it under its own
`--as` identity, or its session, and the human at the keyboard is neither.
Two ways around it: assert the submitting identity with `--as` if you know
it —

```
$ rc kill 707b485b-8a91-4350-a327-d39df63250b3 --as agent-x
rc: kill requested for 707b485b-8a91-4350-a327-d39df63250b3
```

— or, the way an operator actually reaches for someone else's stuck job
without knowing or spoofing their identity, use an admin token, which skips
the ownership check entirely:

```
$ RC_TOKEN=<admin-token> rc kill 2ef79507-9cc7-49db-92b7-3ecfc8421d93
rc: kill requested for 2ef79507-9cc7-49db-92b7-3ecfc8421d93
```

The kill is asynchronous for a running job (the worker has to actually stop
the process), so a moment later the job shows up terminated and the device
is back in the pool. The flag rides the worker's existing long poll, so it
normally reaches the worker within milliseconds; if that response is lost it
is re-offered every 30s rather than on every poll, so a kill for a job no
worker is actually running costs one extra wake per interval instead of
spinning the worker's poll loop at its floor rate:

```
$ rc devices
DEVICE       STATE  HOLDER  ELAPSED  COMMAND
gpubox:gpu0  ready  -       -        -
```

`rc attach` re-streams a job's output from the beginning — handy for
watching a job someone else (or your own detached `rc run`) started. It is
read-only: exiting it, by Ctrl-C or otherwise, never affects the job, since
attach never held a lease in the first place:

```
$ rc attach 2cb5dcd8-6081-49d7-9384-e3226ab87120
streaming
done
```

`rc devices` shows the fleet, including devices whose worker has gone quiet:

```
$ rc devices
DEVICE       STATE  HOLDER                    ELAPSED  COMMAND
gpubox:gpu0  busy   mudler@mudler-ubuntu-box  15s      sh -c sleep 15
```

`rc ps` shows the same information job-first: the currently
assigned/running jobs, then the queue behind them with each job's position
in its own device's queue. Queued jobs are listed because their IDs are
otherwise unobtainable, and `rc kill` needs one:

```
$ rc ps
JOB                                   DEVICE       STATE        SUBMITTER                 COMMAND
25147ef7-7bf4-40b9-9034-31e294e5be1a  gpubox:gpu0  running      mudler@mudler-ubuntu-box  sh -c sleep 30
9d1c8a44-0f7a-4a1e-9e2e-2b4f2c0a1a77  gpubox:gpu0  queued (#1)  agent-b@builder           ./train
```

`rc hold` claims a device for a human, not a job — "I need a shell on this
box", not "run this command":

```
$ rc hold gpubox:gpu0 --ttl 30m --reason "manual profiling"
rc: hold e6b8b6b0-8b7f-4e2a-9b8b-9b1c2b3a4d5e granted on gpubox:gpu0, expires around 2026-08-13T13:00:00Z
rc: end it early with `rc release e6b8b6b0-8b7f-4e2a-9b8b-9b1c2b3a4d5e`, or Ctrl-C here
```

**Under the hood a hold is a job with `kind: hold`**, whose command a
worker — never the client — chooses for itself (a sleeper, for its TTL).
That is a deliberate simplification rather than a second lease mechanism:
it reuses the exact same allocation transaction, queue, wall-clock
watchdog, and acquire/release hooks a job already has, so taking a hold
stands a node's inference server down exactly like a job would, and it
needs no new code in `rc ps`, `rc devices`, or the dashboard to show up —
`rc ps` lists it with command `hold`, and `rc devices` shows its holder and
`--reason`. The cost: a hold occupies a real worker process for its
duration and appears in job history like any other job.

`--ttl` is required and is capped by the device's `max_runtime` exactly as
a job's is — rejected, never silently shortened. `--select` works for a
hold too, taking the first matching free device.

**Ctrl-C on a granted hold releases it** — the opposite of `rc run`'s
detach-only behaviour for a running job, because a hold's whole point is
that a human is sitting there, and leaving means they're done with it.
`rc release <job-id>` does the same thing from any terminal, and is a thin
alias over `rc kill`: only the hold's own submitter, or an admin token, may
end it early.

## Dashboard

The controller serves a read-only web dashboard at `/` — the same address
`rc serve --addr` binds, no separate process or port. It shows the same
picture as `rc devices` / `rc ps` in a browser: a card per device (state,
current holder, elapsed time, and a `stale`/`alert` tint once a worker's
heartbeat has gone quiet or a device has been quarantined `unhealthy`), the
queue in priority order, and the currently running jobs. It refreshes on a
poll plus a live SSE nudge, and shows how long ago its own snapshot was
last refreshed so a disconnected browser tab does not quietly look current.

**It is read-only.** There is no kill/hold/submit button anywhere on the
page — the actions that touch a lease or a process still go through `rc
kill` / `rc run` from a terminal. The page asks for a client token before
showing anything, and that token is kept in the tab's `sessionStorage`
only: it is never written to disk, never sent to any other tab or a new
browser session, and is gone the moment the tab is closed. Reloading the
page (in the same tab) keeps it; opening the dashboard in a second tab asks
for the token again.

The one exception to "tokens go in the `Authorization` header, never the
URL" is `GET /v1/events`, the SSE stream the dashboard uses to know when to
refresh: browsers' `EventSource` API cannot set request headers, so that
route alone also accepts `?token=...` as a query parameter. Every other
route — including `/v1/state`, which the dashboard actually reads its data
from — still requires the header and rejects a query-string token.

## Guarantees

- One live lease per device, enforced by a unique index in SQLite plus a
  single allocation transaction — not by convention and not by a file in
  `/tmp`. A second `rc run`/submit against a held device never gets handed
  the device too: by default it joins the queue (see below); with
  `--no-wait`/`NoWait`, the job is still created (it gets a real ID and a
  row in job history) but is immediately cancelled server-side and the
  request answered with 409 `no_device_available` — so a `killed` job with
  `kill_reason: "no-wait: device busy"` in history is expected and not a
  sign anything went wrong; it just means a `--no-wait` submit found the
  device taken.
- Submitting onto a busy device queues it rather than failing, FIFO within a
  priority tier (`--priority`, higher runs sooner, bounded to `-10..10` and
  rejected with 400 `priority_out_of_range` outside it — it is a nudge
  within one device's queue, not a scheduling language) — but **not** FIFO
  *across* tiers: a higher-priority job submitted later jumps ahead of an
  already-queued lower-priority one for the same device, every scheduling
  pass, for as long as higher-priority work keeps arriving. A low-priority
  job behind a steady stream of higher-priority submits can be starved
  indefinitely; there is no aging or fairness mechanism yet. Within its own
  tier, though, the controller schedules a queued job onto its device the
  instant that device frees up — at submit time if it's already free, and
  via a background scheduler loop (`rc serve` runs one every second) once it
  frees up later. `rc kill` on a still-queued job cancels it outright;
  nothing about the queue requires the submitting client to still be
  connected.
- A device can declare a `max_runtime` ceiling in `worker.yaml`. A job that
  asks for more than the ceiling is rejected at submit time — never
  silently clamped down to fit. The worker enforces the resulting limit
  (its own, if set, or the device's) as a wall-clock watchdog once the job
  is running, and separately enforces `--idle-timeout` (no stdout/stderr
  progress for that long); either firing kills the job, marks it `killed`
  with a `kill_reason` naming which limit fired (e.g. `max_runtime exceeded
  (2s)`), and returns the device to the pool.
- Every claimed device carries a lease with an expiry; the controller
  actively reaps an expired lease rather than trusting it forever, so a
  worker that vanishes mid-job without ever reporting back does not pin the
  device indefinitely on the strength of a lease nobody is renewing. A
  worker's heartbeat renews the leases of **the jobs it names in that
  heartbeat**, not every job the controller has down against it: renewal
  follows what the worker is actually supervising, so a job the worker has
  no process for (e.g. one whose assignment response was lost in transit)
  stops being renewed, expires, and is reclaimed — marked `lost` with reason
  `lease expired`, its device quarantined `unhealthy` and clearable by an
  admin. Otherwise a worker that stayed alive but never received a job could
  keep that job's lease renewed forever, and the expiry backstop would never
  fire for the one case that most needs it.
- A disconnected client does not release the device: the worker owns the
  process and reports its outcome directly to the controller.
- A worker that stops reporting has its devices marked `unknown` after 30s
  without a heartbeat, then `unhealthy` after 5 minutes. Neither
  transition ever puts the device back in the pool — silence is never
  treated as proof a device is free. A worker that starts heartbeating
  again restores its `unknown` devices on its own (to `busy` if their lease
  is still live, to `ready` otherwise); a device that reached `unhealthy`
  stays out until an admin token clears it explicitly:
  `POST /v1/devices/{id}/clear`. That call only succeeds when the device is
  currently `unhealthy` **and** has no live lease; called against a device
  in any other state (e.g. `ready`) it refuses with 409 rather than
  contradict the lease table.
- A worker that registers is a fresh process, so registration reconciles:
  any job that worker ID still had `assigned` or `running` is marked
  `lost` and its lease released. Its device's fate then depends on whether
  the registration carries proof the host rebooted (a changed boot ID): if
  so, the device comes back `ready` — a reboot proves nothing survived; if
  not (an ordinary worker-process restart, or a crash with no reboot), the
  device is quarantined `unhealthy` instead, since nothing proves an
  orphaned process isn't still pinning it. Either way this is what makes a
  `kill -9` of `rc worker`, not just its ordinary shutdown, safe to restart
  from — see "Device host" above for the full three-way breakdown.
- Jobs run in their own process group, so a kill takes the whole tree with
  it, including grandchildren that detached from the job's own stdio.
- A device with an `on_acquire` hook that fails is quarantined `unhealthy`
  by the worker itself, via `POST /v1/devices/{id}/fault` (worker-token
  only) — the same store path `SetDeviceState` uses, recorded with
  quarantine reason `fault`. `fault` is the one quarantine reason a proven
  reboot (a changed boot ID at registration) does not clear automatically —
  see "Device host" above — so it always takes an admin's explicit
  `POST /v1/devices/{id}/clear`. The failure reason itself (the hook's tail
  output) travels only in the job's own failure report, surfaced by `rc ps`;
  it is not stored on the device row, which only ever records the fixed
  reason `fault`. That report is retried on the same budget as a job's
  terminal report — it is the only thing standing between a device the hook
  has just called unusable and the next job scheduled onto it — and a
  device ID the controller does not know answers `404`, never a `200` the
  worker would log as a quarantine that never happened.
- A `--select` matching no device is rejected at submit with 400
  `no_matching_device` — never queued on the chance a device registers
  later — and a queued selector job at the head of the queue reserves
  every device it currently matches, not just the one it eventually lands
  on, so later-queued jobs cannot jump onto any of them in the meantime.
  See "Selectors" above.
- A device's declared labels (`worker.yaml`) and detected labels (a probe)
  are both stored and both shown; a detected value wins a same-key
  conflict for scheduling, but a declared value is never overwritten or
  discarded — `rc describe` shows the conflict rather than hiding it. A
  probe that fails, times out, or emits anything other than one flat JSON
  object costs that probe's labels for that pass, never the worker's
  ability to come up or keep serving jobs — with one exception: a device
  whose `nvidia-smi` was present at worker startup and later disappears
  keeps its previously-detected labels instead of losing them, since
  wiping them is a fleet-wide guess for what is really a one-device
  problem. See "Labels and probes" above.
- A hold is a job (`kind: hold`) whose command a worker chooses for
  itself, never the client — a hold submission carrying a command, `cwd`,
  or `env` is refused outright. `--ttl` is required and is capped by the
  device's `max_runtime` exactly as a job's is — rejected, never clamped.
  See "Client" above.
