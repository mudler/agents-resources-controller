<img src="assets/logo.svg" alt="rc" height="52">

# resource-controller

Exclusive leases for shared hardware, across hosts, with the state visible.

Replaces a `flock` file mutex: a central controller owns allocation in a
single SQLite transaction, workers on device hosts supervise the jobs, and
`rc ps` / `rc devices` show who holds what.

```sh
rc run --select 'vram>=40G' -- python train.py
```

That claims a matching GPU somewhere in the fleet, runs the command on the
host that owns it, streams the output back, releases the device, and exits
with the command's exit code.

**New here?** [How it works](#how-it-works) · [Install](docs/install.md) ·
[Guide for agents](docs/agents.md)

## How it works

Three pieces, and the direction of the arrows is the thing to understand:

```
    you / your agent                    ┌──────────────────────┐
    ─────────────────                   │      CONTROLLER      │
      rc run                            │                      │
      rc devices        ── HTTP ───────▶│  owns allocation     │
      rc describe                       │  SQLite, one txn     │
      rc hold                           │  queue + scheduler   │
                                        │  dashboard :8080     │
    browser  ── HTTP ──────────────────▶│                      │
                                        └──────────────────────┘
                                              ▲          ▲
                                              │          │
                            long-poll for work│          │(same, from
                            + heartbeats      │          │ every host)
                                              │          │
                            ┌─────────────────┴──┐  ┌────┴───────────────┐
                            │  WORKER on gpubox-a│  │ WORKER on gpubox-b │
                            │  runs your command │  │                    │
                            │  as a process group│  │  gpu0  gpu1        │
                            │  gpu0 … gpu3       │  │                    │
                            └────────────────────┘  └────────────────────┘
```

**Workers dial out. The controller never connects to your machines.** A worker
runs *on* the device host and long-polls the controller for assignments over
plain HTTP. When one arrives it forks the command locally as a process group,
streams the logs back, and reports the outcome.

That has consequences worth knowing up front:

- **There is no SSH anywhere in this system.** The controller holds no
  credentials for your hosts and never opens a connection to them. Hosts
  behind NAT or a firewall work fine as long as they can reach the
  controller.
- **The command runs on the device host**, not where you typed it. Paths must
  exist there. Nothing is copied for you — this is not a deployment tool.
- **A job runs in the worker's container, as root.** The published worker image
  (`images/rc-worker/`) is Ubuntu 24.04 with a normal toolchain — compiler,
  cmake, python, git, curl — and a job can `apt-get install` anything else. It
  gets the GPU and whatever volumes the operator mounted, not your filesystem.
  Installs persist into later jobs until the pod restarts, so project
  dependencies belong in a virtualenv on shared storage.
- **The worker must see the processes it supervises.** It signals real process
  groups, so it runs either directly on the host or in a container whose
  namespace those processes share — it cannot sit behind an indirection that
  hides them. Running it as a privileged DaemonSet pod with the GPU attached
  works and is how the reference fleet is deployed (see
  `docs/superpowers/plans/2026-08-16-pod-worker-gpu-arbitration.md`); running
  it under systemd on the host works too.
- **A lost client is not a lost job.** If your `rc run` dies, the worker keeps
  running the job and the lease stays valid. Re-attach with `rc attach`.

The lease itself is one SQLite transaction — device `ready → busy`, job
updated, lease row inserted, all or nothing — behind a partial unique index
that permits exactly one live lease per device. That transaction is the whole
guarantee.

### How a machine describes itself

A device is not just a name. Each carries **labels** — some declared by the
operator in the worker's config, some detected at runtime by probes
(`gpu_model`, `driver_version`, `cpus`, `disk_free_bytes`, …) — and each host
can publish a **usage sheet**: free-form Markdown saying how that box is meant
to be used. Reaching it, where the scratch space is, what not to run at the
same time.

So a client asks for what it needs rather than naming a box, and reads the
sheet to learn the rest:

```sh
rc devices --select 'vram>=40G'   # what can do this work, and is it free?
rc describe gpubox-a:gpu0          # labels, provenance, freshness, usage sheet
```

That is the self-discovery path, and it is why an agent needs no hardcoded
inventory. See [Labels and probes](#labels-and-probes) and [Usage sheets and
`rc describe`](#usage-sheets-and-rc-describe).

## Scope

Exclusive device leases, supervised job execution, live log streaming, fleet
visibility, a queue with priorities, per-device runtime watchdogs, lease
expiry, boot-identity recovery, server-side cancellation (`rc kill`),
re-attaching to a running job's output (`rc attach`), a live SSE event
stream, a web dashboard that can kill a job and clear an unhealthy device,
lease lifecycle hooks (stop/start a
service like LocalAI or ollama around a job's hold on a device), capability
probes and labels with provenance, verify probes that quarantine a device a
job left dirty, webhook notifications for the six things worth waking up for,
selectors (`--select`), per-host usage
sheets and `rc describe`, and `rc hold`/`rc release` for claiming a device
for a human rather than a job.

**Deliberately out of scope.** These are not "not yet" — they are decisions,
and nothing below assumes them:

- **No multi-controller replication.** One controller owns allocation, in one
  SQLite database. That is what makes the lease invariant a single
  transaction; it also means the controller is a single point of failure, so
  run it somewhere that stays up and back up `--data`.
- **No fractional device sharing.** A lease is the whole device. There is no
  MIG-style partition, no "half a GPU", no memory-quota scheduling.
- **No shipping code to hosts.** The controller runs the command you give it
  on a host that already has everything that command needs. It is not a
  deployment tool and never copies a payload anywhere.
- **No per-client or per-worker tokens.** `RC_TOKENS` is a small set of
  shared secrets with three roles; identity (`--as`, the `submitter` field)
  is a label, not a credential. Read "`rc kill` checks ownership" under
  "Client" below before you rely on that check for anything.
- **No TLS.** Bearer tokens cross the wire in the clear and the controller
  terminates no TLS of its own; put it behind a tunnel, a VPN, or a private
  network.

**Not built yet**, so you will not find them documented below:

| Not built yet | What you do instead today |
|---|---|
| A `cuda` label from the built-in GPU probe | `nvidia-smi --query-gpu` has no `cuda_version` field (see "Labels and probes" below); write a drop-in probe if you need it |
| A controller-side operator annotation layered over a usage sheet | The spec allows one ("the host file wins on conflict"), but it was never built — a usage sheet is exactly what the host's `host.md`/`host.d/*.md` say, full stop; `host_docs` is keyed `(host, device_id)` with no annotation layer on top |

## Controller

> Setting this up for the first time? [docs/install.md](docs/install.md) is
> the step-by-step version — Docker image, systemd units, and a worker on
> each host. This section is the reference.

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
verify_dir: /etc/rc/verify.d     # optional; default shown — see "Verify probes"
verify_timeout: 30s              # optional; default shown — per verify script
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
SIGTERM) of `rc worker` during a multi-hour job kills that job.

**That 45s is a grace period, not a promise the report always lands.** The
work it covers is stacked, and each piece is bounded separately: killing the
job (~12s), then the verify pass — which deliberately runs on a context
immune to the shutdown, so that a job ending mid-shutdown still gets its
device checked — costing up to ~42s for one hanging script and another ~28s
if the resulting fault report has to retry, and only then the terminal
report's own ~28s budget. Stacked worst case ≈ 110s, well past the 45s
window, at which point the worker exits with the report undelivered.

What that costs is precision about the JOB, not safety of the DEVICE: the
job's true exit code (and any verify-sourced reason) never reach the
controller, and the sweep or the next registration reconciles it as `lost`,
quarantining its device rather than handing it out dirty. Correct, just not
tidy — so do not read the 45s as a promise that a terminal report always
lands. `internal/worker/worker.go`'s `shutdownGrace` comment carries the
per-stage arithmetic.

There is no
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
  `nvidia-smi --query-gpu=name,memory.total,memory.free,driver_version` and
  turns each row into `gpu<N>.vendor`, `gpu<N>.model`, `gpu<N>.vram` and
  `gpu<N>.vram_free` (both as a `K/M/G/T` quantity, e.g. `24576M`, so
  selectors can compare them numerically), and `gpu<N>.driver` — only for
  device names this host actually declared as `gpu<N>`.

  **No `cuda` label is emitted.** `nvidia-smi --query-gpu` has no
  `cuda_version` field; the CUDA version only appears in the plain-text
  header of a bare `nvidia-smi`/`nvidia-smi -q` invocation, once per
  process, not per GPU. Getting it would mean a second nvidia-smi
  invocation per probe pass plus a text-header parse against a format
  NVIDIA does not document as stable — judged not worth it for a label
  this system can do without: a selector like `--select 'cuda>=12'`
  fails loud (rejected at submit — no device matches) rather than
  silently matching on a stale or fragile-parsed value. Write a drop-in
  probe if you need it and are willing to own that parse.
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
means that fact is gone": a source that **fails** — as opposed to running
and reporting nothing — leaves the labels of the devices it covers
**preserved** rather than cleared, because wiping them is a fleet-wide
guess and a stale label is scoped to one device. `nvidia-smi` present on
`PATH` when this worker process started and gone now (a driver upgrade
mid-run is the canonical case) counts as a failure; `nvidia-smi` never
present since this worker started does not, and those labels clear
normally, the same as any other probe that stops reporting.

Preservation is scoped to the failing source and to the devices that
source covers: for `nvidia-smi`, the devices it has reported (or, before
it has ever reported any, those a `gpu<N>` key could name at all); for a
drop-in script, the devices it named the last time it ran successfully in
this worker process. Every other device keeps refreshing, so one broken
drop-in no longer freezes host-wide facts — `disk_free_bytes`,
`mem_total_bytes` — on devices it has nothing to do with. A script that
has not once succeeded since this worker started covers no device, so its
failure preserves nothing: after a worker restart, a probe that is broken
from the outset clears what it used to report rather than freezing it
forever.

A `probe_dir` the worker cannot read at all (a `chmod 000`, a
not-a-directory) counts as every drop-in failing at once, so every device
they cover is preserved — but a `probe_dir` that simply **does not exist**
is a legitimate configuration rather than a failure, so labels there clear
the ordinary way. Note that this is the opposite call from the one `verify_dir` makes
for a file it cannot inspect, and deliberately so: a probe that did not run
costs a label, while a verify script that did not run would otherwise be
recorded as proof a device is clean.

**Probes and verify scripts, like lease lifecycle hooks, run
operator-supplied scripts as the worker user with no additional sandboxing**
— the same blast radius as any other script the worker executes on your
behalf, so trust `probe_dir` and `verify_dir` exactly as much as you trust
`on_acquire`/`on_release`.

### Verify probes: proving a device is clean before the next job

A job exiting is not proof the device it held is usable again: a crashed
trainer can leave VRAM pinned, a driver can wedge, a zombie can keep a
context open. A **verify probe** is a drop-in script that checks, on the
host, after the job's process tree is gone — and quarantines the device if
the answer is no:

```yaml
verify_dir: /etc/rc/verify.d   # optional; default shown
verify_timeout: 30s            # optional; default shown — per script
verify_pass_budget: 2m         # optional; default shown — the whole pass
```

```sh
$ cat /etc/rc/verify.d/10-vram-free.sh
#!/bin/sh
used=$(nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits -i "$CUDA_VISIBLE_DEVICES")
[ "$used" -lt 512 ] && exit 0
echo "${used}MiB still allocated on $RC_DEVICE after job $RC_JOB_ID" >&2
exit 1
```

**The contract is: the exit code is the result, stderr is the reason.**
Nothing on stdout is read (unlike a capability probe, whose stdout is its
whole point). Exit zero means the device is clean. Anything else — a
non-zero exit, a timeout, a script that could not be run at all — fails the
pass, and the tail of that script's stderr (200 characters, the END of it —
shell scripts put their real complaint last) becomes the reason recorded with
the fault. Where that reason then shows up is its own paragraph below; it is
not on the device row.

**A failure quarantines the device** via the same
`POST /v1/devices/{id}/fault` a failed `on_acquire` uses, so it is recorded
with quarantine reason `fault` — the one reason a proven reboot deliberately
does **not** clear (see "Device host" above). It takes an admin's explicit
`POST /v1/devices/{id}/clear`, or the dashboard's `clear` control, to put the
device back in the pool.

**The verify pass runs BEFORE the job's terminal report, and that ordering is
the entire point.** The controller frees a device the instant that report
lands, so verifying afterwards would leave a window — however short — where
a dirty device is schedulable, which is exactly the OOM this feature exists
to prevent. The cost of the ordering is that a slow verify pass delays the
job's terminal report (and therefore the next job on that device) by however
long it takes; that is what `verify_timeout` and `verify_pass_budget` bound.

**The job's own outcome is untouched.** A verify failure says something about
the device, not about the run: the job still reports its real state and its
real exit code.

```
$ rc run -d demobox:gpu0 -- sh -c 'echo trained-a-model; exit 0'
rc: job c1f6f2c3-d1f0-41fb-b4e2-87fc249681d9 on demobox:gpu0
trained-a-model
$ echo $?
0
$ rc devices
DEVICE        STATE      HOLDER  ELAPSED  COMMAND
demobox:gpu0  unhealthy  -       -        -
$ rc run -d demobox:gpu0 --no-wait -- true
rc: demobox:gpu0 is busy and could not be queued
```

(That last message is `rc run`'s one wording for "no device available"; the
device is quarantined, not busy.)

Getting it back is a deliberate act by an admin, after whatever the script
complained about has actually been dealt with:

```
$ curl -sS -X POST -H "Authorization: Bearer $RC_ADMIN_TOKEN" \
    http://rc.internal.example:8080/v1/devices/demobox:gpu0/clear
$ rc devices
DEVICE        STATE  HOLDER  ELAPSED  COMMAND
demobox:gpu0  ready  -       -        -
```

**Where the reason shows up.** The device row records only the fixed reason
`fault`, so `rc devices` shows the state and not the story. The script's own
stderr travels in the worker's log, the controller's log, and — if you have
one configured — the `verify_failed` webhook event:

```
worker     ERROR verify failed; quarantining device device=demobox:gpu0
           job=c1f6f2c3-… reason="verify failed: 10-vram-free.sh: exited 1:
           72G still allocated on demobox:gpu0 after job c1f6f2c3-…"
controller WARN  device quarantined: fault device=demobox:gpu0 reason="verify
           failed: 10-vram-free.sh: exited 1: 72G still allocated…"
```

The rest of the rules:

- **It is off unless scripts exist.** A `verify_dir` that does not exist runs
  nothing and costs nothing — no pass, no delay, no quarantine. That is the
  default state of an unconfigured host.
- **Every executable in `verify_dir` runs, in sorted filename order**, each
  in its own process group under `verify_timeout` (killed whole if it runs
  over), with this environment — the same one a lifecycle hook gets, on top
  of the worker's own:

  ```
  RC_EVENT=verify
  RC_DEVICE=demobox:gpu0
  RC_JOB_ID=a427c77b-e788-43cb-9e4c-36354552e938
  RC_SUBMITTER=alice@lab
  CUDA_VISIBLE_DEVICES=0
  ```

  `RC_SUBMITTER` is the submitter of the job that just finished — whoever's
  run left the device in the state you are about to inspect — so a script can
  name them in the reason it writes to stderr. `CUDA_VISIBLE_DEVICES` is
  derived from the device name's trailing integer, exactly as a job's is, and
  is absent for a device name that has none. Treat both as untrusted input
  for the same reason a hook must (see "Lease lifecycle hooks"): the
  submitter is free text chosen by whoever submitted the job.
- **Every script still runs after an earlier one fails**, but only the FIRST
  failure's reason is kept: one is already enough to quarantine the device.
- **A non-executable file is skipped**, so a `README` sitting alongside the
  scripts is harmless and `chmod -x` disables a script the way it does in any
  other drop-in directory. The skip is **logged at `WARN`** naming the file,
  because the other way to lose the executable bit is by accident — a git
  checkout, an `rsync` without `-p` — and a safety check that quietly stops
  running is worse than one that was never installed:

  ```
  WARN verify script is not executable; skipped, so whatever it checks is NOT being checked
       script=/etc/rc/verify.d/10-vram-free.sh mode=-rw-r--r--
  ```

  A file that cannot even be inspected is a different matter: a
  dangling symlink, or anything else whose `stat` fails, **fails the pass**.
  A verify script that could not be run has proven nothing, and "proved
  nothing" must never be recorded as "verified clean" — the one place this
  behaves deliberately unlike `probe_dir`, where an unreadable drop-in only
  costs a label.
- **`verify_pass_budget` bounds the whole pass**, not each script's own
  timeout summed, and hitting it fails the pass for the same reason: an
  unfinished pass proves nothing. The reason then names the budget rather
  than any one script.
- Verify scripts run on **every** job's exit — success, failure, watchdog
  kill, `rc kill`, and the end of a hold alike. There is no "only on
  failure" mode.

### Selectors

`--select` (on `rc run` and `rc hold`) targets a device by its labels
instead of a device ID, e.g. `--select 'vendor=nvidia,vram>=40G'`: a
comma-separated conjunction of terms, every term must hold. Supported
operators are `=`, `!=`, `>=`, and `<=` (no bare `>`/`<`). When both sides
of a comparison parse as a quantity — a plain number, or one suffixed
`K`/`M`/`G`/`T` (case-insensitive, powers of 1024) — the comparison is
numeric, so `vram>=40G` matches `80G` and `81920M` alike. When NEITHER side
is a quantity it falls back to a lexicographic string comparison, so
`model>=a100` matches `h100`. When exactly one side is a quantity the
comparison is unanswerable and **never matches** — `nvidia-smi` reports
`[N/A]` for total memory on unified-memory parts, and a lexicographic
fallback there made `vram>=40G` match a device whose VRAM is unknown. A term whose key is
absent from a device's labels never matches, `!=` included: an absent
label is not proof the device differs.

**That rule bites dotted version strings, and the surprise is worth naming.**
A multi-part version is not a quantity, so `driver>=500` does not match a
device reporting `driver=580.173.02` — one side parses, the other does not, and
the comparison fails closed. It *does* match `driver=595.78`, which parses as
a number. This is deliberate: `580.173.02` has no defensible numeric value, and
the alternative is the lexicographic accident above. Compare versions with `=`
against a known value, or ask the operator for a label that is a plain number.

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

**There is no controller-side annotation layer over a usage sheet.** The
design allows one — an operator annotation the controller carries on top
of the host's own file, with the host file winning on conflict — but it
was never built: `host_docs` is stored keyed by `(host, device_id)` alone,
with nothing layered above it. Whatever `host.md`/`host.d/<device>.md`
says on the host is exactly what `rc describe` shows; there is no
separate, controller-held annotation to reconcile it against.

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

> Pointing an AI agent at this fleet? Two ways, same material:
>
> - **Install the skill.** [`skills/leasing-a-gpu/`](skills/leasing-a-gpu/SKILL.md)
>   is a ready-made agent skill. Copy or symlink that directory into wherever
>   your agent looks for skills (`~/.claude/skills/`, `~/.codex/skills/`, …) and
>   the agent picks it up on its own whenever a task needs a GPU — no need to
>   remember to paste anything.
> - **Hand over the doc.** [docs/agents.md](docs/agents.md) is the same guidance
>   as prose, for an agent that has no skill mechanism.
>
> Both are fleet-agnostic on purpose: they tell the agent to discover your boxes
> with `rc devices` and `rc describe` rather than hardcoding names. Describe
> what is peculiar about a box — where its shared storage is mounted, how to get
> files onto it — in that host's [usage sheet](#usage-sheets-and-rc-describe),
> and the agent reads it from `rc describe`.
>
> This section is the reference.

```sh
export RC_CONTROLLER=https://rc.internal.example
export RC_TOKEN=ctok
export RC_SUBMITTER=agent-a            # optional: who you are, in rc ps

rc devices                              # who holds what, and what has gone quiet
rc ps                                   # running/assigned jobs
rc run -d gpubox:gpu0 --cwd /src -- ./bench --args
```

`RC_SUBMITTER` sets the identity `--as` would otherwise carry, so a session
says who it is once instead of repeating `--as` on every `run`, `hold`, `kill`
and `release` — forgetting it on the kill is what leaves you unable to stop
your own job. An explicit `--as` still wins. With no identity set at all, `rc`
derives one from `$USER`, the hostname, and `$CLAUDE_SESSION_ID` when present.

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

**`rc kill` checks ownership: only the job's own submitter, or an admin
token, may kill it.** The controller checks the `submitter` on the
kill request against the job's recorded submitter (`defaultSubmitter()`
embeds `$USER@$HOSTNAME`, plus a session ID when run as an agent) and
refuses everyone else with 403, even from a perfectly valid client token:

```
$ rc kill 707b485b-8a91-4350-a327-d39df63250b3
rc: not_job_owner: only the submitter or an admin may kill this job
```

> **Submitter ownership is an accident guard, not authentication.** Read the
> next two paragraphs together: the way past that 403 is to pass `--as` with
> the other submitter's name, and nothing stops anyone from doing it.
> Tokens are shared and per-client tokens are an explicit non-goal (see
> "Deliberately out of scope"), so *anyone holding the client token can claim
> any identity and kill any job*. What this check actually buys you is that
> `rc kill <id>` typed against the wrong job ID fails loudly instead of
> killing a colleague's twelve-hour training run. Treat it as a seatbelt, not
> a lock, on the dashboard as much as on the command line.

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

## Event notifications

The controller can POST operational events to a webhook — the things worth
telling somebody about without them polling `rc devices` all day:

```sh
rc serve --addr :8080 --data /var/lib/rc --webhook-url https://hooks.example/rc
# or: RC_WEBHOOK_URL=https://hooks.example/rc rc serve ...   (the flag wins)
```

With no URL configured, nothing is built and nothing is emitted; that is the
default. On startup the controller says which endpoint it will use:

```
INFO event webhook configured url=https://hooks.example/rc
```

Each event is one `POST` with `Content-Type: application/json` and a flat
JSON object as the body — one event per request, never a batch:

```json
{
  "event": "watchdog_trip",
  "device": "demobox:gpu0",
  "job": "b07cc27e-f04c-4600-8dcc-a13f1557a4d2",
  "reason": "max_runtime exceeded (5s)",
  "at": "2026-08-15T18:40:34.481158937Z"
}
```

`device`, `job` and `reason` are always present as keys and may be empty
strings when they do not apply (a `job_lost` from a sweep knows the job but
not the device it was on). `at` is UTC, stamped when the event was raised,
not when it was delivered.

There are six kinds, and no others:

| `event` | Raised when |
|---|---|
| `watchdog_trip` | A job's terminal report names a watchdog: `max_runtime exceeded (…)` or `idle: no output for …`. A job killed by `rc kill` or by a worker shutting down is not one of these, and neither is a hold reaching its `--ttl` (which is the same wall-clock watchdog doing exactly what was asked, and would be the highest-volume event in the set) |
| `verify_failed` | A verify script failed, quarantining the device. `reason` is the script's own `verify failed: …` text |
| `device_unhealthy` | A device left the pool for any other reason: a failed `on_acquire` hook, an expired lease, a quarantine a sweep could not attribute to a lost worker. A verify failure is reported as `verify_failed` only — never as both, so a consumer counting devices lost cannot double count |
| `worker_lost` | A sweep wrote a worker off after `5m` of silence and quarantined its devices |
| `job_lost` | That same sweep marked the jobs that worker was supervising `lost` |
| `lease_expired` | A lease lapsed with nobody renewing it. The job is named. Its device is quarantined too, and gets its own `device_unhealthy` — but only when that sweep has not already announced that device, and only while `lease_expired` is still the reason on it: a device already out of the pool for another cause keeps that cause and was announced when it happened |

One receiver's log across an afternoon of things going wrong — a verify
script finding a dirty GPU, a job running past its ceiling, a worker
disappearing (which loses its device and its job in the same sweep), and a
lease nobody renewed:

```json
{"event":"verify_failed","device":"demobox:gpu0","job":"c1f6f2c3-…","reason":"verify failed: 10-vram-free.sh: exited 1: 72G still allocated on demobox:gpu0 after job c1f6f2c3-…","at":"2026-08-15T18:39:34.099998734Z"}
{"event":"watchdog_trip","device":"demobox:gpu0","job":"b07cc27e-…","reason":"max_runtime exceeded (5s)","at":"2026-08-15T18:40:34.481158937Z"}
{"event":"worker_lost","device":"demobox:gpu0","job":"","reason":"worker_lost","at":"2026-08-15T18:46:45.411139980Z"}
{"event":"job_lost","device":"","job":"ed87e831-…","reason":"worker lost","at":"2026-08-15T18:46:45.411141132Z"}
{"event":"lease_expired","device":"fakebox:gpu0","job":"c7a211ab-…","reason":"lease expired","at":"2026-08-15T18:53:35.423880246Z"}
{"event":"device_unhealthy","device":"fakebox:gpu0","job":"","reason":"lease_expired","at":"2026-08-15T18:53:35.423881479Z"}
```

**What does not produce an event**: a device demoted to `unknown` after 30s
of silence (the next heartbeat routinely undoes it — this is not an
incident); a job cancelled, killed, or refused by `--no-wait`; a queue that
is merely long; and a worker restarting and reconciling its own stranded jobs
(registration is the worker announcing itself, not the controller
discovering a loss).

**Delivery is best-effort, and deliberately cheap to lose.** One background
goroutine drains a queue of 256 events; each is attempted up to 3 times, 1s
apart and doubling, with each individual POST bounded at 10s. Any non-2xx
response counts as a failure. Once that budget is spent the event is dropped
with a log line and the controller moves on.

**A full queue drops rather than blocks — always.** Events are raised from
request handlers (a worker's fault report, a worker's terminal report) and
from the reaper's sweep, and neither may ever wait on your webhook. If the queue is full when an
event is raised, that event is discarded and counted, and the caller
continues as if nothing happened:

```
WARN notify: dropped event, queue full event=watchdog_trip device=demobox:gpu0
```

That is the guarantee to design your receiver around: **an event that does
not arrive is not a bug you can report**, and an unreachable endpoint costs
you notifications and nothing else — no stalled job, no stranded device, no
slowed sweep. Because delivery is serialised through one goroutine, a slow
endpoint also delays every event behind it, which is why the retry budget is
small. On shutdown the controller spends up to 5s delivering whatever is
still queued, then gives up: a notification is never worth holding a
shutdown open for.

**The webhook carries no authentication of its own.** There is no signature,
no shared secret, no configurable header — the controller POSTs plain JSON.
Point it at something on a private network, or put the only secret you have
in the URL itself, and treat anything arriving there as unauthenticated.

## Dashboard

The controller serves a web dashboard at `/` — the same address
`rc serve --addr` binds, no separate process or port. It shows the same
picture as `rc devices` / `rc ps` in a browser: a card per device (state,
current holder, elapsed time, and a `stale`/`alert` tint once a worker's
heartbeat has gone quiet or a device has been quarantined `unhealthy`), the
queue in priority order, and the currently running jobs. It refreshes on a
poll plus a live SSE nudge, and shows how long ago its own snapshot was
last refreshed so a disconnected browser tab does not quietly look current.

The page asks for a client token before showing anything, and that token is
kept in the tab's `sessionStorage` only: it is never written to disk, never
sent to any other tab or a new browser session, and is gone the moment the
tab is closed. Reloading the page (in the same tab) keeps it; opening the
dashboard in a second tab asks for the token again.

Two kinds of thing on the page change state, and nothing else does. There is
no submit button and no way to *take* a hold from the browser — starting
work still goes through `rc run` / `rc hold` from a terminal.

**Kill a running job.** Fill in the identity box ("you are …") with the
submitter name your jobs run under — the same string `rc run --as` sends and
the `submitter` column shows — and a `kill` control appears on each running
job. It sends that identity, and the controller checks it against the job's
submitter exactly as `rc kill` is checked, so killing someone else's job is
refused with `not_job_owner` and the page shows that refusal in the
controller's own words. The control is still offered on a job you did not
submit (drawn as an outline rather than a filled button, since the
controller will refuse it) because "someone else's job is stuck on the GPU I
need" is precisely when an operator goes looking for it, and a refusal
naming the submitter is more useful than a missing button. The identity is
not a credential and is not stored: a reload keeps the connection and
forgets who you are, and the kill controls stay hidden until you say so
again.

That last point deserves saying plainly rather than being inferred from "not
a credential": **the identity box is an accident guard, not a login.** Typing
someone else's submitter name into it is exactly what `rc kill --as` does,
and the controller cannot tell the difference — see "`rc kill` checks
ownership" above. Anyone who got far enough to paste a working client token
can kill any job on the fleet by claiming its submitter's name.

**And that includes ending a hold, which is where "no hold button" stops
being the whole story.** A hold is a job (`kind: hold`), so it appears in the
running-jobs table like any other and gets the same `kill` control — and
killing a hold *is* `rc release`, the same call under a different name.
You cannot take a hold from the page; you can absolutely end one, subject to
the same submitter check (the person holding a box is usually the person
looking at the page, so this is normally the useful direction).

**Clear an unhealthy device.** An `unhealthy` card carries a `clear` control,
which is the browser equivalent of `POST /v1/devices/{id}/clear` and needs an
**admin** token. The page does not have one and never keeps one: it asks for
it in a prompt at the moment you click, uses it for that single request, and
drops it before the response comes back — never in `sessionStorage`, never in
`localStorage`, never on a variable that outlives the call. Clearing a second
device asks again. So the strongest credential ever resident in the browser
is the client token — which, per the caveat above, is not a small thing to
leave in a tab, but it cannot return a device to the pool. A clear that the
controller refuses is refused with `device_not_cleared`, shown verbatim; the
usual cause is a device that is unhealthy but still holds a live lease, so
deal with the lease first. (The same message covers a device that simply is
not `unhealthy` — the wording names the lease either way.)

Both actions confirm before firing, refresh the page's state on success, and
report the controller's own error text on failure rather than a generic
"failed" — `not_job_owner` and `device_not_cleared` each tell you what to do
next.

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
- A device with verify scripts is proven clean before it goes back in the
  pool, or it does not go back: the pass runs after the job's process tree is
  gone and **before** the terminal report that frees the lease, so a failure
  quarantines the device (`fault`, admin-clearable only) with no window in
  which anything can be scheduled onto it. Anything that stops a script from
  running — a timeout, an unstattable file, the pass budget — counts as a
  failure, never as a pass. The job's own state and exit code are unaffected.
  See "Verify probes" above.
- An operational event (a watchdog trip, a verify failure, a device leaving
  the pool, a lost worker or job, an expired lease) is delivered to a
  configured webhook on a best-effort basis and never at the expense of the
  thing that raised it: a full queue drops the event and a dead endpoint is
  simply given up on. No handler, sweep, or scheduling pass ever waits on a
  webhook. See "Event notifications" above.
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
  ability to come up or keep serving jobs. A probe that *fails*, as opposed
  to one that runs and reports nothing, leaves labels **preserved** rather
  than cleared — scoped to the failing source and to the devices that source
  is understood to cover, not to the whole pass and not to one built-in:
  `nvidia-smi` covers the devices it has reported (or those a `gpu<N>` key
  could name at all), a drop-in covers the devices it named the last time it
  succeeded in this worker process, and every other device on the host keeps
  refreshing normally. See "Labels and probes" above for the full rule,
  including what a source that has never once succeeded covers (nothing).
- A hold is a job (`kind: hold`) whose command a worker chooses for
  itself, never the client — a hold submission carrying a command, `cwd`,
  or `env` is refused outright. `--ttl` is required and is capped by the
  device's `max_runtime` exactly as a job's is — rejected, never clamped.
  See "Client" above.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
