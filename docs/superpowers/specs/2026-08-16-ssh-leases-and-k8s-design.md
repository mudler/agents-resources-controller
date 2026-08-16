# SSH leases and Kubernetes GPU arbitration — design

**Status:** approved in brainstorming, not yet planned
**Date:** 2026-08-16

## The problem

Two gaps, found by trying to use the fleet as it exists.

**1. Taking a box interactively is a three-step ritual you can half-do.** To
work on a GPU box by hand today you run `rc hold gpubox:gpu0 --ttl 2h`, then
`ssh gpubox`, then remember `rc release`. Forget the last step and the GPU is
leaked; skip the first and you are on hardware the fleet believes is free,
which is the exact failure this system exists to eliminate. Agents do the same
thing — they `ssh box 'command'` — with the same two ways to get it wrong.

**2. On the boxes that matter, the GPU is already taken.** `orin`, `thor` and
`dgx` are each a single-node k3s cluster dedicated to one workload: a
`local-ai-worker` Deployment, `replicas: 1`, `Recreate`, `runtimeClassName:
nvidia`, reconciled by Flux from `~/_git/infra-flux-kube`. The manifest's own
comment says the quiet part: *"The GPU is exclusive: a rolling update would
briefly run two workers contending for the same device."* Granting a lease on
one of these boxes is meaningless unless LocalAI actually lets go of the GPU.

## What we are building

### `rc ssh` — claim, connect, release, as one act

```sh
rc ssh dgx                          # interactive shell, box locked for the session
rc ssh dgx -- nvidia-smi            # one command, locked for its duration
rc ssh --select 'vram>=40G' -- ./bench   # pick a free box, then connect to it
```

`rc ssh` is the same claim → execute → release shape as `rc run`. The only
variable is **who executes**:

| | `rc run` | `rc ssh` |
|---|---|---|
| Runs the command | the worker, as the worker's user | you, over SSH, as you |
| Interactive TTY | no | yes |
| Survives your disconnect | yes (`rc attach` to rejoin) | no — the connection *is* the process |
| `rc kill` / watchdogs | yes | no: nothing on the box owns it |
| Records a job row | yes | yes — identical |
| Needs inbound reach to the box | no (worker dials out) | yes, from the client |

Everything around execution is shared: the lease, the hooks, the job record,
the audit trail. That is the point — `rc ssh` is not a special case bolted on,
it is the same operation with a different executor.

**It records a real job**, not an opaque hold: command, submitter, start and
end, exit code. So it appears in `rc ps` and in a device's recent-job history
exactly like anything else, and the audit trail is free rather than bespoke.

**It cannot be killed or attached, and says so.** No process on the box belongs
to the controller, so `rc kill` has nothing to signal and `rc attach` has
nothing to rejoin. `rc ps` marks ssh jobs so this is answered on screen rather
than by a confusing refusal.

**Output is captured by default**, teed to the controller's log store the way a
job's output is, with `--no-log` to opt out. An interactive session records
whatever passes through the terminal, which is a deliberate choice about what
the controller's database now contains.

**Role: client.** Same as `rc run`. Restricting it to admin would lock out the
agents that most need it, and it grants nothing on its own — you need your own
SSH key on the box regardless, so the rc token is not what gates the hardware.

### Locking the whole box, atomically

`rc ssh dgx` locks **every device on that host**, all-or-nothing.

This is not convenience. An SSH session gives you access to every GPU on the
machine. A lease covering `dgx:gpu0` while you are free to use `dgx:gpu3`
would have the fleet advertise `gpu3` as available while someone is on it —
the precise lie the system exists to prevent.

For the current fleet this is moot: `orin`, `thor` and `dgx` have one GPU each.
It matters the first time a multi-GPU box appears, which is why it is designed
now and not retrofitted later.

**This is a new shape in the allocation core.** Today `Allocate` claims exactly
one device per transaction, and the guarantee is that transaction plus the
partial unique index `leases_one_live_per_device`. A host claim must acquire
every device's lease inside a single transaction so a partly-claimed box is
impossible — if one device is busy, none are taken. The index continues to do
the real work; what changes is the number of rows the transaction inserts.

### `rc lock` — for when you are already there

`rc hold` with the device inferred from the machine you are sitting on, for
the case where someone SSHed in by hand rather than through `rc ssh`. Same
whole-host semantics.

It is deliberately secondary. `rc ssh` is the controlled path; `rc lock` is the
voluntary one, and its value is that it exists at all — an agent that lands on
a box some other way has a way to be honest about it.

### `rc list`

Rename of `rc devices`. One name for the thing, not an alias beside it.

### Reaching a box: the `ssh` label

`rc ssh` needs to turn `dgx` into something SSH can dial. The fleet already
has the right place to carry that:

```yaml
devices:
  - name: gpu0
    labels: {ssh: dgx.lab, vendor: nvidia, vram: 128G}
```

So `rc ssh --select 'vram>=40G'` can pick a free box *and* know how to reach
it, with no client-side inventory. A device with no `ssh` label cannot be
reached by `rc ssh`, and the error says that rather than guessing at a
hostname.

## Kubernetes: making the lease real on `orin`, `thor`, `dgx`

A lease is a promise the GPU is yours. On these boxes that means LocalAI has to
let go — and give it back afterwards.

### Cordon **and drain**, never scale

```sh
# on_acquire
kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --timeout=2m
# on_release
kubectl uncordon "$NODE"
```

**`cordon` alone is not enough and is the obvious wrong answer.** It marks the
node unschedulable but does nothing to a pod already running — LocalAI would
keep the GPU while rc reported the box as leased. `drain` cordons *and* evicts,
and waits for termination before returning, which is what "the lease is
granted" has to mean.

**Why this beats scaling the Deployment to 0.** Flux reconciles these manifests
every 10 minutes with `prune: true`. A hook that sets `replicas: 0` is drift,
and Flux corrects drift — it would hand the GPU back to LocalAI in the middle
of someone's session. With drain, the Deployment still says `replicas: 1` and
matches git exactly, so **Flux has no opinion at all**: the ReplicaSet simply
cannot place its pod while the node is cordoned. Nothing to suspend, nothing
left dangling if something dies mid-lease.

Scaling would have needed `spec.suspend` on the Kustomization to hold Flux off,
which then leaves drift uncorrected fleet-wide if a release hook never runs.
Rejected for that reason.

### Three things this design has to get right

**`--ignore-daemonsets` is mandatory, not hygiene.** WireGuard runs as a
DaemonSet on all three boxes. A drain without it evicts WireGuard and takes the
network path to the machine with it — from a hook running over that same
network.

**The hook timeout must exceed LocalAI's graceful shutdown.** `drain` waits for
termination; if the hook's timeout fires first, the lease is granted over a GPU
still being released. The default `hook_timeout` is 60s, which a multi-GB CUDA
process may well exceed. These boxes set it explicitly.

**A stuck cordon is worse than the problem.** If `on_release` never runs — the
worker is killed, the box reboots mid-lease — the node stays cordoned and
LocalAI stays down indefinitely and silently. The safety net is the worker
uncordoning on startup, which fits the existing boot-identity recovery path:
the worker already reconciles "what did I think was true before I restarted"
on registration, and "no lease is held, so nothing should be cordoned" is the
same kind of statement.

### Where this lives

The k8s side is **a documented hook recipe in `~/_git/infra-flux-kube`, not
code in rc.** rc already has the mechanism — `on_acquire` / `on_release` per
device, which exist precisely to stop and start a service around a lease. It
should not grow a Kubernetes dependency to serve three boxes; a shell script
with `kubectl` is the correct amount of machinery.

The worker's user needs a kubeconfig with rights to cordon, drain and uncordon
the node it runs on.

## Both mechanisms, advertised to agents

`docs/agents.md` documents both paths, because an agent that reaches a box by
SSH and does not know about `rc ssh` will use a GPU the fleet reports as free:

| | How you ask | When |
|---|---|---|
| **Claim remotely** | `rc run` / `rc hold` — describe what you need, the controller picks | Unattended work, from anywhere |
| **Connect and claim** | `rc ssh` — claim a specific box and get a shell on it | Interactive work, your identity |
| **Claim locally** | `rc lock` — "I am on this machine" | You got here some other way |

## Explicitly not in scope

- **No preemption.** A box held by someone else is not taken from them by
  `rc ssh`; it queues or refuses like any other claim. You cannot sensibly
  interrupt a half-finished training run.
- **No PAM or SSH-session hooks.** Considered and rejected: automatic locking
  on login needs reference counting across concurrent sessions, cannot tell an
  interactive session from `scp`, breaks on `nohup`/tmux detaching, and risks
  locking you out of a box you need to debug. `rc ssh` makes the lease
  lifetime explicit instead of inferring intent.
- **No rc-side Kubernetes integration.** Hooks and `kubectl`, not a controller
  that speaks to clusters.
- **No multi-host claims.** `rc ssh` locks one host completely; it does not
  claim two boxes at once.

## Open questions for the plan

- Does `rc ssh` reuse the existing hold machinery (`kind: hold`) with a new
  kind, or a distinct `kind: ssh`? Leaning `kind: ssh` so `rc ps` can explain
  why it is unkillable.
- How is output teed — line-buffered through the client to the existing log
  endpoint, or a PTY recording? The former is consistent with `rc run`.
- What does `rc ssh` do when the SSH binary is absent, or the `ssh` label is
  missing? Both should fail before the lease is taken, not after.
