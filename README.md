# resource-controller

Exclusive leases for shared hardware, across hosts, with the state visible.

Replaces a `flock` file mutex: a central controller owns allocation in a
single SQLite transaction, workers on device hosts supervise the jobs, and
`rc ps` / `rc devices` show who holds what.

## Stage 1 scope

Exclusive device leases, supervised job execution, live log streaming, and
fleet visibility. **There is no queue yet** — a busy device is refused
immediately with `no_device_available`.

**Not built yet**, so you will not find them documented below:

| Not in Stage 1 | What you do instead today |
|---|---|
| Web dashboard | `rc devices`, `rc ps`, or `GET /v1/state` |
| Capability probes (`/etc/rc/probe.d/*.sh`) and device labels | List device names in `worker.yaml` |
| Per-host usage sheet (`/etc/rc/host.md`) and `rc describe` | Keep host notes wherever you keep them now |
| Device selectors (`--select 'vram>=40G'`) | Address a device by exact ID: `-d gpubox:gpu0` |
| Queue and priorities | Retry, or pick another device |
| Watchdogs (max runtime, idle timeout) | Nothing stops a hung job holding its GPU |
| `rc kill`, `rc attach`, `rc hold` | Ctrl-C detaches; the job keeps running |
| Verify probes between jobs | Nothing checks VRAM was released after a job |

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
wire in the clear and stage 1 terminates no TLS of its own. On a machine
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
devices:
  - gpu0
  - gpu1
heartbeat_interval: 10s
poll_wait: 30s
```

Then:

```sh
rc worker
# or: rc worker --config /path/to/worker.yaml
```

That file is the whole of a node's configuration in stage 1. There is no
auto-detection: the controller knows only the device names you list, with no
labels, no VRAM figures, no driver versions, and no per-host notes. If you
list `gpu0` and `gpu1` on a four-GPU box, the other two do not exist as far
as the scheduler is concerned.

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
way in stage 1 to detach a job from a specific worker process and hand it to
a replacement — the worker that started a job is the one supervising it for
its entire life.

**A worker that never got to run that shutdown sequence — killed -9, host
power loss, an OOM of the worker process itself — leaves the controller
believing the job is still assigned or running.** Registration reconciles
this: when a worker registers, it is announcing (truthfully, since it is a
fresh process) that it has no running jobs. Any job the controller still has
this worker ID down as `assigned` or `running` is marked `lost`, its lease
is released, and — critically — its device comes back `unhealthy`, never
`ready` and never left `busy` forever. A device is never handed back to the
pool on the strength of "the old process is gone now"; nothing proves an
orphaned process from that process isn't still pinning it. Clearing it is
the same explicit `POST /v1/devices/{id}/clear` used for any other
unhealthy device.

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

If the device is already held, `rc run` fails immediately — stage 1 has no
queue:

```
$ rc run -d gpubox:gpu0 -- true
rc: gpubox:gpu0 is busy (stage 1 has no queue — retry or pick another device)
```

**Ctrl-C detaches, it does not cancel the job.** The worker owns the process
and its lease, not the client that submitted it, so losing the terminal (or
pressing Ctrl-C on purpose) must never free a GPU that a process is still
using. The job keeps running and keeps holding the device; `rc run` prints
where things stand and exits with status 130:

```
$ rc run -d gpubox:gpu0 -- sh -c 'sleep 15'
rc: job bfbf7d29-9306-41da-9a9f-413026b7361e on gpubox:gpu0
^C
rc: detached from job bfbf7d29-9306-41da-9a9f-413026b7361e — it is STILL RUNNING on gpubox:gpu0 and holds it until it finishes. Watch it with: rc ps
```

Server-side cancellation (`rc kill`) does not exist yet — it is a later
stage. Until then, the only ways a device comes back are the job finishing
on its own, or stopping the worker process on the device host — which, as
described above, kills the job rather than merely detaching from it.

`rc devices` shows the fleet, including devices whose worker has gone quiet:

```
$ rc devices
DEVICE       STATE  HOLDER                    ELAPSED  COMMAND
gpubox:gpu0  busy   mudler@mudler-ubuntu-box  15s      sh -c sleep 15
```

`rc ps` shows the same information job-first, for the currently
assigned/running jobs:

```
$ rc ps
JOB                                   DEVICE       STATE    SUBMITTER                 COMMAND
25147ef7-7bf4-40b9-9034-31e294e5be1a  gpubox:gpu0  running  mudler@mudler-ubuntu-box  sh -c sleep 30
```

## Guarantees

- One live lease per device, enforced by a unique index in SQLite plus a
  single allocation transaction — not by convention and not by a file in
  `/tmp`. A second `rc run`/submit against a held device is refused with
  `no_device_available` at allocation time, before any job is created.
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
- A worker that registers is a fresh process, so registration reconciles: any
  job that worker ID still had `assigned` or `running` is marked `lost`, its
  lease released, and its device quarantined `unhealthy` — never handed back
  as `ready`, never left `busy` with nothing left to release it. This is
  what makes a `kill -9` of `rc worker`, not just its ordinary shutdown, safe
  to restart from.
- Jobs run in their own process group, so a kill takes the whole tree with
  it, including grandchildren that detached from the job's own stdio.
