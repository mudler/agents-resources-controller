# Using shared GPUs from an agent

This page is written for an AI coding agent. If you are a human setting the
system up, read [install.md](install.md) first; this is what you hand to the
agent afterwards.

Everything here uses the `rc` CLI. There is a REST API underneath it
(`docs/api.md`), but the CLI is the supported surface and it is what the rest
of this page uses.

---

## The one paragraph version

The GPUs are shared. Before you use one you must claim it, and while you hold
it nobody else gets it. `rc run -- <your command>` does the whole thing:
claims a free device, runs your command on the host that owns it, streams the
output back, releases the device, and exits with your command's exit code. If
every device is busy it waits in a queue. **Do not `ssh` to a GPU box and run
things directly** — that bypasses the lease and you will collide with whoever
holds it.

```sh
rc run --select 'vram>=40G' -- python train.py
```

That is the whole interface for most work.

---

## Setup, once per shell

```sh
export RC_CONTROLLER=http://rc.internal.example:8080   # ask the operator
export RC_TOKEN=<client token>                          # ask the operator
export RC_SUBMITTER="claude/$(hostname)/session-$$"     # optional, see below
```

`RC_SUBMITTER` is the name that shows up in `rc ps` and on the dashboard, and
it is what `rc kill` checks before letting you kill a job. Set it to something
that identifies *this* agent session. It is a label, not a credential — it
does not grant you anything, it just makes the fleet legible to the human
watching it. If you do not set it, `rc` derives one from user, host and
session.

Check it works:

```sh
rc devices
```

---

## 1. Find out what is available

**Do not hardcode a device name.** Ask for what your work needs and let the
controller choose. Hosts get added, renamed and taken out for maintenance.

```sh
rc devices                              # everything, and who holds what
rc devices --select 'vram>=40G'         # only what matches
rc describe gpubox-a:gpu0               # one device, in full
```

`rc describe` is the important one when you are deciding. It prints the
device's state, who holds it, every label with **where the label came from and
how old it is**, the host's usage sheet, and recent jobs. Freshness matters:
a label is only as true as its last probe, and the age tells you whether to
trust it.

Selectors match on labels:

| Example | Meaning |
|---|---|
| `--select 'vram>=40G'` | at least 40 GiB of VRAM |
| `--select 'vendor=nvidia,class=train'` | both must hold (comma is AND) |
| `--select 'rack!=b12'` | not in that rack |
| `--select 'disk_free_bytes>=500G'` | sizes accept K/M/G/T suffixes |

The operators are `=`, `!=`, `>=`, `<=` — there is no substring match, and a
device that does not carry the label at all never matches, **including for
`!=`**. `rack!=b12` means "has a rack label, and it is not b12", not "is not
known to be in b12".

Labels come from two places: **declared** ones the operator wrote in the
worker's config, and **detected** ones a probe found at runtime
(`gpu_model`, `driver_version`, `cpus`, `mem_total_bytes`, `disk_free_bytes`,
and whatever site-specific probes the operator added). `rc describe` shows
which is which.

Before committing to a selector, ask what it would actually match:

```sh
rc run --select 'vram>=80G' --explain
```

That prints which devices match, how many are free right now, and how deep the
queue is — then exits without submitting. Use it when a job is expensive to
get wrong, or when you are about to wait a long time.

If nothing matches, the submit is **rejected immediately** rather than queued
forever:

```
rc: no_matching_device: no device matches the selector: vram>=200G
```

That is a real answer: no machine here can do this. Widen the selector or tell
the human.

## 2. Read the host's own instructions

Every host can publish a usage sheet — free-form Markdown the operator wrote,
shown by `rc describe` and on the dashboard. **Read it before you use a
machine.** It is where the operator explains things this system cannot infer:

```
USAGE SHEET
  host-wide note, updated 2h ago
  # gpubox-a — shared training box

  Four A100s on NVLink. Do not run more than two multi-GPU jobs at once;
  the PCIe switch saturates and everything slows down.

  Reach it with `ssh gpubox-a.lab`. Scratch space is /scratch (2TB NVMe,
  wiped Sundays). Datasets are already at /data/imagenet — do not re-download.
```

This is how a machine describes itself: how to reach it, what is already on
it, what not to do. If a sheet tells you to do something differently from this
page, **the sheet wins** — it was written by someone who knows that box.

## 3. Claim it and do the work

```sh
rc run --select 'vram>=40G' -- python train.py --epochs 10
```

What happens: the controller picks a matching free device and hands the job to
the worker on that host; the worker runs your command **on that host**, in a
process group of its own, with the device's `CUDA_VISIBLE_DEVICES` already
set; output streams back to your terminal; when the command exits the device
is released. `rc run` exits with your command's exit code, so it slots
straight into scripts wherever `flock` used to sit.

Useful flags:

```sh
rc run -d gpubox-a:gpu0 -- ...          # a specific device, when you must
rc run --cwd /src -- ./bench            # working directory on the device host
rc run --max-runtime 2h -- ...          # kill it if it runs longer
rc run --idle-timeout 10m -- ...        # kill it if it stops producing output
rc run --no-wait -- ...                 # fail now rather than queue
rc run --timeout 30m -- ...             # give up waiting after 30m
rc run --priority 5 -- ...              # jump the queue (-10..10)
```

**The command runs on the device host, not where you typed it.** Paths must
exist *there*. Nothing is copied for you — this is not a deployment tool. If
your code is not on that host, get it there first (the usage sheet usually
says how).

### If you need the device for several commands

Do not call `rc run` repeatedly and hope you get the same device — you will
not, and between calls someone else can take it. Either run one command that
does everything:

```sh
rc run --select 'vram>=40G' -- bash -c 'python prep.py && python train.py && python eval.py'
```

...or take an explicit hold:

```sh
rc hold --select 'vram>=40G' --ttl 2h
```

`rc hold` claims a device and **blocks**, printing the device it got. It holds
until you Ctrl-C, until the TTL expires, or until someone runs `rc release`.
While you hold it, `ssh` to that host and work directly — that is the
legitimate way to use a box interactively, because the lease is real and
everyone else can see it.

...or, if what you actually want is a terminal on the box, ask for one
directly:

```sh
rc run --tty --select 'vram>=40G'
```

`--tty` is the controlled path and is better than `hold` + `ssh` for anything
interactive: the shell runs under the lease, it queues like any other job,
and it is **supervised** — `rc kill` ends it, the watchdogs bound it, and its
children die with it instead of outliving your session and keeping the GPU.
An interactive job's output goes to your terminal and is not kept, so
`rc attach` has nothing to show for it afterwards.

### If you need a file on the box

```sh
rc cp ./train.py gpubox:gpu0:/workspace/       # up
rc cp gpubox:gpu0:/workspace/out.json ./       # back
```

For a **script, a config, a patch** — something you name, once. The copy runs
under a lease, so you can only copy to a device that is free for you to take,
and a box someone else holds is refused by name.

**Not for models, datasets or checkpoints.** Every byte crosses the
controller twice and it is a scheduler, not a file server; tens of gigabytes
through it is the wrong shape. Large or persistent data belongs on whatever
shared storage the host mounts — the host's usage sheet (`rc describe`) says
where that is, and if it does not, ask rather than pushing it through `rc cp`.

**A hold's TTL is a promise. Keep it short and extend deliberately.** A
forgotten hold is indistinguishable from a leak to everyone else, and it is
the most antisocial thing you can do here.

## 4. Watch and clean up

```sh
rc ps                          # what is running and queued, and whose it is
rc attach <job-id>             # re-attach to a running job's output
rc kill <job-id>               # stop a job you submitted
rc release <hold-id>           # end a hold early
```

`rc kill` only lets you kill a job whose submitter matches yours. Be aware of
what that check is and is not: with a shared client token, anyone can claim
any identity, so it is a **guard against killing the wrong job by accident,
not authentication**. Do not kill jobs that are not yours.

---

## Rules

1. **Never bypass the lease.** No `ssh` to a GPU host to run work directly
   unless you are holding that device. The whole point is that the lease is
   the only way to know a GPU is free.
2. **Ask for capabilities, not names.** `--select 'vram>=40G'`, not
   `-d gpubox-a:gpu0`, unless something genuinely requires that exact box.
3. **Read the usage sheet before you use a host.** It is the operator talking
   to you directly.
4. **Always bound long work** with `--max-runtime`. A runaway job holds a GPU
   the whole fleet is waiting for.
5. **Release what you take.** `rc run` does it for you; a hold does not.
6. **Do not clear an unhealthy device.** If a device is quarantined it is
   because something went wrong — usually a job before yours left memory
   pinned. Clearing it needs an admin token and is a human's call. Report it
   and pick another device.
7. **A rejected submit is information, not an obstacle.** "no device matches"
   means this fleet cannot do what you asked. Do not retry in a loop; widen
   the selector or say so.

## When something goes wrong

| What you see | What it means | What to do |
|---|---|---|
| `no_matching_device` | No device has the labels you asked for | Widen the selector, or `rc devices` to see what exists |
| Job sits `queued` | Every matching device is busy | Normal. `rc ps` shows position. Use `--timeout` if you cannot wait |
| `is busy and could not be queued` | `--no-wait`, and the device was not free | Drop `--no-wait`, or pick another device |
| `not_job_owner` | The job's submitter is not you | It is not yours. Leave it alone |
| Job ends `killed`, reason `max_runtime exceeded` | Your own ceiling stopped it | Raise `--max-runtime`, or make the job finish sooner |
| Job ends `killed`, reason `idle: no output for ...` | It produced no output for too long | Expected for a genuinely quiet job — set `--idle-timeout 0` |
| Job ends `lost` | The worker died mid-job | The host went away. Check `rc devices`, tell the operator |
| Device shows `unhealthy` | Quarantined; not schedulable | `rc describe` shows why. A human must clear it |

## What this does not do

- It does not deploy, sync or reconcile anything. `rc cp` moves a file you
  name, onto a box you hold, once — nothing recurring, nothing large.
- It does not install anything on the device host.
- It does not give you fractions of a GPU. A lease is the whole device.
- It does not protect you from someone who ignores it. It is cooperative:
  a lease is only meaningful because everyone goes through it.
