---
name: leasing-a-gpu
description: Use when work needs a GPU on a shared fleet managed by `rc` - training, inference, benchmarks, CUDA builds, or anything that touches VRAM. Claim a device with `rc` before using it, never ssh in and run things directly. Triggers - "run this on the GPU", "train/benchmark/profile on <box>", "is a GPU free", "I need a GPU", any CUDA or nvidia-smi work on a remote box.
---

# Leasing a GPU

The GPUs are shared between you, other agents, and humans. **Before you use one
you must claim it, and while you hold it nobody else gets it.** That is what
`rc` does.

```sh
rc run --select 'class=train' -- python train.py
```

That claims a free device, runs the command on the host that owns it, streams
the output back, releases the device, and exits with your command's exit code.
It drops in wherever `flock /tmp/gpu -c '...'` used to sit.

**Never `ssh` to a GPU box and run something directly.** That bypasses the
lease, and the fleet will report the box as free while you are on it — the
exact failure this system exists to eliminate. There is one legitimate way to
work on a box by hand; see "Interactive work" below.

## Setup

`rc` reads `~/.config/rc/config.yaml`:

```yaml
controller: http://controller-host:8080
token: <your client token>
submitter: agent/<something that identifies you>
```

Environment variables (`RC_CONTROLLER`, `RC_TOKEN`, `RC_SUBMITTER`) override
the file. If `rc` says `unauthorized: unknown or missing token`, that file is
missing or has no token in it.

`submitter` is the name that appears in `rc ps`, and it is what `rc kill`
checks before letting you stop a job. It is a label, not a credential — it
grants you nothing, it makes the fleet legible to whoever is watching it. Set
it to something that identifies *this session*.

## 1. Find out what the fleet actually has

Do not guess at box names or capabilities. Ask:

```sh
rc devices                          # every device, its state, and who holds it
rc devices --select 'class=train'   # only what matches
rc describe <device>                # labels, provenance, freshness, usage notes
rc run --select 'class=train' --explain   # what would match, and the queue depth
```

**`rc describe` is not optional reading for a box you have not used before.**
It shows each label's source (`detected` by a probe vs `declared` by an
operator), how old it is, and the operator's usage sheet — which is where the
box tells you things no label can, such as **where its shared storage is
mounted and how to get files onto it**. Boxes differ: on one fleet
`/workspace` may be a shared network folder, on another the local disk of that
one machine.

## 2. Pick a device

Boxes are usually **not interchangeable** — different GPU, different memory,
different CPU count. Naming the one you want is normal and often correct:

```sh
rc run -d <host>:<device> -- python train.py
```

Use a selector when *any* of several boxes would do, or when you care about a
property rather than a name:

```sh
rc run --select 'class=train' -- ./bench          # whichever is free
rc run --select 'gpu_model=A100' -- ./bench       # by capability
```

The tradeoff: naming a box queues you behind whoever holds it, even if another
box is free. A selector takes the first free match.

Selector operators are `=`, `!=`, `>=`, `<=`. There is no substring match, and
a device that does not carry the label never matches — **including for `!=`**.

A label that reads `[N/A]` is a probe reporting "unknown", not a value. It will
not compare usefully; select on something else.

## 3. Run the work

```sh
rc run --select 'class=train' -- python train.py --epochs 10
rc run -d <host>:<device> -- ./bench           # a specific box
rc run --max-runtime 2h -- ...                 # kill it if it overruns
rc run --idle-timeout 10m -- ...               # kill it if it goes quiet
rc run --timeout 30m -- ...                    # give up waiting in the queue
rc run --no-wait -- ...                        # fail now rather than queue
```

If every matching device is busy, the job **queues** and runs when one frees up
— that is the default and usually what you want. `rc ps` shows the position.

**The command runs on the device host, inside the worker's container.** You get
the GPU and the mounted volumes; you do not get your local filesystem. Nothing
is copied for you — read the box's usage sheet (`rc describe`) to learn where
its shared storage is and how to put files there.

## 4. Interactive work

If you need a shell rather than a single command, take an explicit hold, so the
lease is real and everyone can see it:

```sh
rc hold <host>:<device> --ttl 2h   # blocks, prints the device, holds until Ctrl-C or TTL
# in another terminal: ssh <that host>
rc release <hold-id>               # or Ctrl-C the hold
```

**A hold's TTL is a promise. Keep it short.** A forgotten hold is
indistinguishable from a leak and is the most antisocial thing you can do on a
shared fleet.

## 5. Watch and clean up

```sh
rc ps                  # what is running and queued, and whose it is
rc attach <job-id>     # re-attach to a running job's output
rc kill <job-id>       # stop a job you submitted
```

`rc run` releases the device for you. A hold does not.

## Rules

1. **Never bypass the lease.** No work on a GPU box outside `rc run` or an
   `rc hold` you are holding.
2. **Name a box when the box matters; use a selector when it does not.** Do not
   name one out of habit — that queues you behind its holder while another box
   sits free.
3. **Always bound long work** with `--max-runtime`. A runaway job holds a GPU
   the whole fleet is waiting for.
4. **Release what you take.**
5. **Do not clear an unhealthy device.** A quarantined device means something
   went wrong — usually pinned VRAM or a worker that died mid-job. Clearing it
   needs an admin token and is a human's call. Report it and use another device.
6. **A rejected submit is information.** `no_matching_device` means the fleet
   cannot do what you asked. Do not retry in a loop; widen the selector or say
   so.

## When something goes wrong

| What you see | What it means | What to do |
|---|---|---|
| `unauthorized: unknown or missing token` | No config file or no token in it | Check `~/.config/rc/config.yaml` |
| `no_matching_device` | Nothing carries those labels | Widen the selector; `rc devices` to see what exists |
| Job sits `queued` | Every matching device is busy | Normal. `rc ps` shows position; `--timeout` if you cannot wait |
| `is busy and could not be queued` | `--no-wait` and it was not free | Drop `--no-wait`, or pick another device |
| `not_job_owner` | The job is not yours | Leave it alone |
| `killed`, `max_runtime exceeded` | Your own ceiling stopped it | Raise `--max-runtime` or make it finish sooner |
| `killed`, `idle: no output for ...` | It went quiet too long | Expected for a genuinely quiet job — `--idle-timeout 0` |
| `lost` | The worker died mid-job | The box went away. `rc devices`, then tell the operator |
| Device `unhealthy` | Quarantined, not schedulable | `rc describe` says why. A human must clear it |
| `/workspace` looks empty | Often a storage mount that failed on the host | Read the usage sheet in `rc describe`; tell the operator |

## What this does not do

- It does not copy your code or data anywhere.
- It does not give you a fraction of a GPU. A lease is the whole device.
- It does not protect you from someone who ignores it. It is cooperative: the
  lease means something because everyone goes through it.

Fuller reference: [`docs/agents.md`](../../docs/agents.md).
