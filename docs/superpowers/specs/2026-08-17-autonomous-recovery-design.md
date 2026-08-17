# Autonomous recovery from quarantine — Design

**Status:** proposed, 2026-08-17

## The problem

A device leaves the pool automatically and returns only by hand.

- `POST /v1/devices/{id}/fault` — a worker can quarantine a device on its own.
- `POST /v1/devices/{id}/clear` — only an **admin token** puts it back.

The fleet can therefore degrade autonomously but cannot recover autonomously.
In a fleet driven by agents, "a human must clear it" is not a recovery path; it
is an outage that waits for someone to notice.

This is not theoretical. On 2026-08-17 three devices were quarantined in one
afternoon, none of them by a hardware problem — all three by ordinary
maintenance (rolling a new worker image).

### Why it happens more often than expected

Two paths quarantine a device, and the first surprises people:

1. **Absence.** `Store.Sweep` demotes a device to `unhealthy` with reason
   `worker_lost` purely because its worker stopped reporting past
   `unhealthyAfter`. **No job need be involved** — an entirely idle device is
   quarantined if its worker is away long enough. This is what took orin out
   while it sat doing nothing.
2. **A job in flight.** A worker that re-registers while the controller still
   has a job assigned to that device quarantines it with reason
   `registration` — "a re-registering worker left a job in flight".

Path 1 makes the cost scale with restart *duration*. The worker image grew from
~150MB to 910MB when it gained a real toolchain, and the resulting 8-minute
image pull turned every rolling restart into a guaranteed quarantine.

### Why the current design is nonetheless right

The controller cannot know what state an interrupted job left the GPU in.
Handing that device to the next job risks two processes on one card, or an OOM
from memory the previous run never released. Quarantine-and-wait is the correct
*default*. The defect is that the only exit is a human.

## The principle already in the code

`internal/store/reaper.go` already distinguishes quarantine causes that a
machine can answer from those it cannot:

```go
// rebootClearableReasons are the quarantine causes a proven boot-ID change
// answers. Anything else — a fault, or an unrecorded cause — survives the
// reboot untouched.
var rebootClearableReasons = []any{
    quarantineWorkerLost, quarantineLeaseExpired, quarantineRegistration,
}
```

A worker reports `BootID()` at registration (`internal/worker/bootid.go`,
stored in `workers.boot_id`). A changed boot ID proves the machine rebooted,
which proves the GPU is clean, so those three reasons clear automatically.

**The channel exists, the principle is established, and there is exactly one
kind of proof implemented — and it costs a full reboot.** This design adds a
second, cheaper proof of the same fact.

## The proof to add

The question a quarantine actually asks is narrow:

> Can any process from the interrupted job still be holding this GPU?

That is answerable without inspecting the hardware at all.

### Containerised worker (the pod/DaemonSet deployment)

Jobs run **inside the worker's own container** as its children. When the
container restarts, its PID namespace is destroyed and every process from the
previous job dies with it. The GPU is released because the processes holding it
no longer exist.

The restart *is* the proof. The worker can establish it cheaply: it is PID 1 in
its own PID namespace, and at startup that namespace contains essentially
nothing but itself.

This is strictly better than probing the hardware:

- It is **definitive**, not heuristic.
- It **works where probing cannot.** `nvidia-smi` reports `[N/A]` for total
  memory on the GB10 and the Thor — the same defect that made `vram>=40G` match
  every device — and orin has no `nvidia-smi` at all. A VRAM-based check would
  be reading a number two boxes cannot report and the third cannot produce.

### Host worker (systemd)

Jobs are children of the worker process on a shared host. A worker restart does
**not** kill them; they are reparented to init and may still be running. The
worker must therefore check for survivors rather than assume, using the
`liveProcExists` / `parseProcStat` helpers added on 2026-08-17 for the
zombie/straggler fix.

If it cannot prove they are gone, it says so, and the device keeps today's
behaviour.

### Deployment mode is DERIVED, never declared

There is deliberately **no configuration option for "I am containerised"**. A
flag that must match reality will eventually be wrong, and the dangerous
direction fails silently: a worker wrongly claiming isolation while running
under systemd would return a device whose previous job is still training on it.

The worker reports what it *observed*, and the observation is conservative by
construction — a false negative costs a manual clear, a false positive is
impossible to reach by misconfiguration because there is nothing to configure.

## Design

### Worker side

At registration the worker includes a claim describing what it can prove about
leftover processes, alongside the existing `boot_id`:

- `isolated` — the worker is PID 1 in a PID namespace containing only itself,
  so no process from any previous job can exist.
- `survivors_checked` / `survivors_found` — for the host case, whether the
  worker was able to look for processes belonging to the interrupted job, and
  what it found.

A worker that cannot determine either reports neither. Silence means "no
proof", which means no auto-recovery.

### Controller side

On registration, a device quarantined for a reason in `rebootClearableReasons`
returns to `ready` if **any** of these holds:

1. the boot ID changed (existing behaviour), or
2. the worker proved isolation, or
3. the worker checked for survivors and found none.

`fault` is **never** auto-cleared. A self-reported hardware problem is not
answered by "no processes are running" — the probe that reported it tested
something this proof does not.

### The one setting, and its direction

`require_manual_clear` (worker config, with an `RC_REQUIRE_MANUAL_CLEAR`
environment override) forces today's behaviour on a host that wants it.

**It can only ever be more conservative.** There is deliberately no setting that
forces auto-recovery, because that is the only direction that can hand out a
device with a live process on it. A misconfigured switch therefore costs a
manual clear and nothing worse.

### Flap protection

A device that auto-recovers more than N times within a window stops
auto-recovering and waits for a human. A card that quarantines, clears, and
quarantines again is describing a real problem, and silently looping hides it.

### Observability

Auto-recovery emits an event through the existing notifier (which already has
`device_unhealthy` and `worker_lost`), so it appears in the dashboard's activity
feed rather than happening invisibly. An operator must be able to answer "why is
this device back?" without reading the controller's logs.

## What this does not do

- It does not remove quarantine. A device with an unexplained fault still waits
  for a human, which is the case where waiting is right.
- It does not verify the hardware is *healthy* — only that nothing from the
  interrupted job survives. Health probes are a separate mechanism
  (`internal/worker/verify.go`), currently configured on no host in the fleet.
- It does not help a worker that never comes back. A box that is off stays out
  of the pool, correctly.

## Operational note, independent of this change

Pre-pulling the worker image before a rolling restart keeps the worker's absence
to seconds instead of minutes, and would have avoided every quarantine on
2026-08-17 on its own. Worth doing regardless of this design.
