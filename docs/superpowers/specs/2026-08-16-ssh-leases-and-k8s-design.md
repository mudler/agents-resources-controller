# Interactive leases and Kubernetes GPU arbitration — design

**Status:** approved in brainstorming, not yet planned
**Date:** 2026-08-16

> Rewritten mid-brainstorm. The first version of this document proposed an
> `rc ssh` command; that was challenged and dropped. Section "Why not
> `rc ssh`" records the reasoning, because the rejected design is the one
> most likely to be proposed again.

## The problem

Two gaps, both found by trying to use the fleet as it exists.

**1. Working on a box by hand is a three-step ritual you can half-do.** Today
you run `rc hold gpubox:gpu0 --ttl 2h`, then `ssh gpubox`, then remember
`rc release`. Forget the last and the GPU is leaked; skip the first and you are
on hardware the fleet believes is free — the exact failure this system exists
to eliminate. Agents do the same thing and get it wrong the same two ways.

**2. On the boxes that matter, the GPU is already taken.** `orin`, `thor` and
`dgx` are each a single-node k3s cluster dedicated to one workload: a
`local-ai-worker` Deployment, `replicas: 1`, `Recreate`, `runtimeClassName:
nvidia`, reconciled by Flux from `~/_git/infra-flux-kube`. The manifest says the
quiet part itself: *"The GPU is exclusive: a rolling update would briefly run
two workers contending for the same device."* A lease on one of these boxes
means nothing unless LocalAI actually lets go.

## `rc run --tty`

Interactive work becomes a flag on the command that already exists:

```sh
rc run --tty -d dgx:gpu0                     # a shell on the box, box locked
rc run --tty --select 'vram>=40G'            # pick a free box, then a shell on it
rc run -d dgx:gpu0 -- ./bench                # unchanged: unattended, supervised
```

Everything `rc run` already does still applies, and that is the whole point of
choosing this over a second command:

- **It queues.** A busy box makes the request wait, printing its position, and
  drops you into the shell when it is yours. `--timeout` bounds the wait,
  `--no-wait` refuses instead. This is the "sit and wait until it's done"
  behaviour, and it comes for free rather than being designed again.
- **It stays supervised.** `rc kill` works. The watchdogs work. `rc attach`
  rejoins if your terminal dies. An interactive session you can kill from your
  laptop when it wedges is strictly better than one you cannot.
- **It needs no inbound access.** The worker dials out; nothing has to reach
  the box from outside.
- **It records the same job row** — command, submitter, exit code, history.

### Why not `rc ssh`

The first draft of this design added `rc ssh <host>`, which shells out to SSH
under the caller's own identity. It was rejected, and the reasoning matters
because it is the obvious idea:

- **It adds no authority.** `rc run` already executes arbitrary commands on the
  box as the worker's user. A TTY is a transport feature, not a privilege
  escalation — which removes the main reason to keep the two separate.
- **It loses supervision.** Nothing on the box owns an SSH process, so
  `rc kill` has nothing to signal and `rc attach` nothing to rejoin. The draft
  spec had a whole section explaining how `rc ps` would display *why* those do
  not work. That wart disappears entirely when the worker owns the process.
- **It requires inbound reachability** from the client to the box, undoing the
  dial-out property that makes NAT'd hosts work.
- **It needs new concepts** — an `ssh` label per device, a second execution
  path, a job kind that cannot be killed.

What `rc ssh` genuinely offered was running as *you*, with your keys and
dotfiles. On a shared multi-user cluster that is a real difference. On this
fleet — one operator and their agents — it is not, and the worker can simply
run as the user that shell should be.

**The cost of this choice, stated plainly:** a TTY needs bidirectional
streaming. Logs flow one way today (worker → controller → client); an
interactive session needs stdin flowing back and terminal-resize signals,
which means a websocket or bidirectional stream through the controller plus
PTY allocation in the worker. This is well-trodden — it is what `kubectl exec`
does — but it is the largest single piece of work in this design, and it is
more work than shelling out to `ssh` would have been. It is work in the right
place: one execution path, still supervised.

## Interactive claims take the whole host

`rc run --tty` locks **every device on that host**, all-or-nothing.

An interactive shell reaches every GPU on the machine no matter how it got
there — `CUDA_VISIBLE_DEVICES` is a suggestion a shell user can override. A
lease covering `dgx:gpu0` while the session can use `dgx:gpu3` would have the
fleet advertise `gpu3` as free while someone is on it. Non-interactive
`rc run` is unchanged: it claims exactly the device it needs.

For the current fleet this is invisible — `orin`, `thor` and `dgx` have one GPU
each. It matters the first time a multi-GPU box appears, which is why it is
designed now rather than retrofitted.

**This is a new shape in the allocation core.** `Allocate` claims exactly one
device per transaction today, and the guarantee is that transaction plus the
partial unique index `leases_one_live_per_device`. A host claim must take every
device's lease inside a single transaction, so a partly-claimed box is
impossible: if one device is busy, none are taken. The index keeps doing the
real work; what changes is how many rows the transaction inserts.

**Starvation is a real risk here.** If a whole-host claim queues while
single-device claims keep arriving, the host claim can wait forever as devices
free up one at a time and are immediately taken. The plan must decide whether a
queued host claim reserves devices as they free. Not reachable on a one-GPU
box; it bites the first time it is not.

## `rc lock` and `rc list`

**`rc lock`** — `rc hold` with the device inferred from the machine you are on,
claiming the whole host. It is for the case where someone reached a box by
plain `ssh` and wants to be honest about it. Deliberately secondary now that
`rc run --tty` is the controlled path; its value is that an agent which lands
on a box some other way has *a* way to declare it.

**`rc list`** — rename of `rc devices`. One name for the thing, not an alias
beside it.

## Kubernetes

### The worker runs as a GPU-enabled DaemonSet

On these three boxes the worker is a pod, not a systemd unit, and jobs run
inside that pod.

This inverts a rule stated elsewhere in the project — "the worker cannot be
containerised" — and the distinction is worth being precise about. That rule is
about a containerised worker supervising **host** processes, which does not
work. A worker that forks jobs **inside its own container** supervises its own
children perfectly well. Both statements are true; they are about different
things.

**A DaemonSet, specifically, and for two reasons that make the whole design
work:**

1. `drain --ignore-daemonsets` evicts LocalAI but **leaves DaemonSet pods
   running**. The worker survives the very drain it triggers. A
   Deployment-based worker would evict itself.
2. DaemonSet pods carry a built-in toleration for
   `node.kubernetes.io/unschedulable`, so the worker **starts on an
   already-cordoned node** — which is exactly the crash-recovery case, and it
   is what makes uncordon-on-start possible.

### Both pods can hold the GPU, and that is why rc is needed

LocalAI gets the GPU through `runtimeClassName: nvidia` and
`NVIDIA_VISIBLE_DEVICES=all`, with **no `nvidia.com/gpu` resource limit**. The
GPU is granted by the container runtime, not the Kubernetes device plugin — so
Kubernetes does not know it is scarce and will happily schedule two pods that
both use it. Exclusivity is a policy enforced by hand (`replicas: 1`,
`Recreate`), not by the scheduler.

So the rc worker pod and LocalAI can both *have* GPU access. They must not both
*use* it, and arbitrating that is exactly rc's job. An idle worker pod holds no
GPU memory; it only matters when a job runs, and a job only runs under a lease.

### Drain on acquire, uncordon on release

```sh
# on_acquire
kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --timeout=5m
# on_release
kubectl uncordon "$NODE"
```

**`cordon` alone is not enough**, and is the obvious wrong answer: it marks the
node unschedulable but does nothing to a pod already running, so LocalAI would
keep the GPU while rc reported the box as leased. `drain` cordons *and* evicts
*and* waits for termination, which is what "the lease is granted" has to mean.

**Why this beats scaling the Deployment to 0.** Flux reconciles these manifests
every 10 minutes with `prune: true`. Setting `replicas: 0` is drift, and Flux
corrects drift — it would hand the GPU back to LocalAI in the middle of a
session. With drain, the Deployment still says `replicas: 1` and matches git
exactly, so **Flux has no opinion at all**: the ReplicaSet simply cannot place
its pod while the node is cordoned. Nothing to suspend, nothing left dangling
if a hook dies. Scaling would have required `spec.suspend` on the Kustomization,
which leaves drift uncorrected fleet-wide if a release hook never runs.

Hooks run inside the worker pod and need a ServiceAccount with RBAC to
`get`/`patch` its node and `create` evictions — the k8s-native way, and better
than a host worker holding a kubeconfig.

### Three things this has to get right

**`--ignore-daemonsets` is mandatory, not hygiene.** WireGuard runs as a
DaemonSet on all three boxes. A drain without it evicts WireGuard and takes the
network path to the machine with it — from a hook running over that network.
It is also what keeps the worker itself alive.

**The hook timeout must exceed LocalAI's graceful shutdown.** `drain` waits for
termination; if the hook's timeout fires first the lease is granted over a GPU
still being released. The default `hook_timeout` is 60s, which a multi-GB CUDA
process may exceed. These boxes set it explicitly.

**A stuck cordon is worse than the problem.** If `on_release` never runs — pod
killed, node rebooted mid-lease — the node stays cordoned and LocalAI stays
down indefinitely and silently. The safety net is the worker uncordoning on
startup, which fits the existing boot-identity recovery path and is reliable
here precisely because a DaemonSet pod can start on a cordoned node.

### Storage

The worker pod mounts a **dedicated subfolder** of the NAS share, not the share
itself:

```yaml
volumes:
  - name: workspace
    hostPath:
      path: /usr/local/nas_share/rc      # NOT /usr/local/nas_share
      type: DirectoryOrCreate
```

`/usr/local/nas_share` is where `cloudconfig/nas-client.yaml` mounts the Samba
share on the node. Jobs get a scratch and data area on shared storage without
handing every job write access to everything else on the NAS. `hostPath` with
`DirectoryOrCreate` matches the pattern LocalAI already uses for its models and
data on the same partition.

## Getting files onto a box

This revisits a stated non-goal. The project says today: *"No shipping code to
hosts. The controller runs the command you give it on a host that already has
everything that command needs. It is not a deployment tool and never copies a
payload anywhere."* That stands for **deployment**; what follows is narrower.

Two paths, by size, and only one of them is new code.

**Large or persistent data: the NAS, with no rc involvement.** The worker pod
mounts `/usr/local/nas_share/rc`. Models, datasets and checkpoints go there by
whatever means already puts things on the NAS, and a job reads them from the
mount. Streaming tens of gigabytes through the controller would be the wrong
shape: it is a scheduler with a SQLite database, not a file server, and every
byte would cross it twice.

**Ad-hoc files: `rc cp`, which falls out of the TTY work for free.** Once the
exec stream is bidirectional, copying a file is `tar` over that stream — pipe a
tarball in, untar on the far side. This is exactly how `kubectl cp` is
implemented, and it means:

```sh
rc cp ./train.py dgx:gpu0:/workspace/       # a script, a config, a patch
```

needs **no new endpoint, no upload storage, and no controller-side buffering**.
It reuses the one execution path this design is already building, and it
inherits the lease: you can only copy to a box you hold.

The line this draws: rc will move a file you hand it, over a lease you hold. It
still does not deploy, does not sync, and is not a package manager. Anything
recurring belongs on the NAS.

`tar` must exist in the worker image, which it does — the image is ours.

## Both mechanisms, advertised to agents

`docs/agents.md` documents both, because an agent that reaches a box some other
way and does not know the local path will use a GPU the fleet reports as free:

| | How you ask | When |
|---|---|---|
| **Claim remotely** | `rc run`, with `--tty` when you want a shell | Almost always |
| **Claim locally** | `rc lock` | You reached the box by plain ssh |

## Explicitly not in scope

- **No preemption.** A box someone else holds is not taken from them; the
  request queues or refuses like any other claim.
- **No PAM or SSH-session hooks.** Considered and rejected: automatic locking
  on login needs reference counting across concurrent sessions, cannot tell an
  interactive session from `scp`, breaks on `nohup`/tmux detaching, and risks
  locking you out of a box you need to debug.
- **No `rc ssh`.** See above.
- **No host access from an interactive session on these three boxes.** The
  shell is in the worker pod: CUDA, the mounted volumes, the NAS subfolder —
  not `systemctl`, `dmesg`, or the node filesystem. Plain `ssh` remains for
  that. A host worker and a pod worker cannot both run on one box, because each
  would declare the same GPU and the fleet would believe there are two.
- **No deployment, sync, or package management.** `rc cp` moves a file you
  name, onto a box you hold. It does not watch directories, reconcile trees, or
  install anything. Recurring or large data belongs on the NAS mount.
- **No rc-side Kubernetes integration.** Hooks and `kubectl`, not a controller
  that speaks to clusters. The k8s side is a manifest and a hook script in
  `~/_git/infra-flux-kube`, not code in rc.

## Open questions for the plan

- **Transport for the TTY**: websocket, or bidirectional chunked HTTP? The
  worker already long-polls over plain HTTP and the controller has no websocket
  dependency today.
- **Does a queued whole-host claim reserve devices as they free**, or retry
  atomically and risk starvation?
- **What happens to a `--tty` job whose client disconnects?** `rc run` keeps
  the job running deliberately. For a shell that means an orphaned session
  holding the box until its watchdog fires — probably right, but it should be
  decided rather than inherited.
- **Does `rc run --tty` imply whole-host**, or must it be asked for? Implying
  it is safer; it also means the same flag claims one device or four depending
  on the box.
