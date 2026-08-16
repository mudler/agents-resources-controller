# Installing

Two things to install, and they are not the same shape:

| | Where it runs | How to install |
|---|---|---|
| **Controller** | one always-on machine | Docker image (or a binary) |
| **Worker** | every GPU host | a binary on the host itself |
| **`rc` CLI** | wherever you submit work from | the same binary |

It is one binary — `rc serve`, `rc worker`, and the client commands are all
the same program. The controller has an image because it is a long-lived
service; the worker deliberately does not (see below).

---

## 1. The controller

### With Docker (recommended)

```sh
mkdir rc && cd rc

# Generate real tokens. Do not ship the examples.
cat > .env <<EOF
RC_TOKENS=$(openssl rand -hex 24):worker,$(openssl rand -hex 24):client,$(openssl rand -hex 24):admin
EOF

curl -fsSLO https://raw.githubusercontent.com/mudler/resource-controller/master/docker-compose.yml
docker compose up -d
```

That pulls `ghcr.io/mudler/resource-controller` and serves on
`:8080`. Published tags:

| Tag | What it is |
|---|---|
| `edge` | current `master` |
| `1.2.3`, `1.2`, `1` | a released version |
| `latest` | the newest release |

`edge` moves whenever `master` does. Pin a version tag if you care about
that.

State lives in the `rc-data` volume: `rc.db` plus one log file per job. **Back
it up.** One controller owns allocation, so it is a single point of failure by
design — see "Deliberately out of scope" in the [README](../README.md).

### Without Docker

```sh
go install github.com/mudler/resource-controller/cmd/rc@latest   # -> $GOPATH/bin/rc
```

or build from a checkout:

```sh
git clone https://github.com/mudler/resource-controller
cd resource-controller
CGO_ENABLED=0 go build -o rc ./cmd/rc
```

The project is cgo-free (`modernc.org/sqlite`), so that produces a static
binary you can copy to any Linux box of the same architecture.

```sh
export RC_TOKENS='<worker-token>:worker,<client-token>:client,<admin-token>:admin'
rc serve --addr 0.0.0.0:8080 --data /var/lib/rc
```

A systemd unit:

```ini
# /etc/systemd/system/rc-controller.service
[Unit]
Description=resource-controller
After=network-online.target

[Service]
ExecStart=/usr/local/bin/rc serve --addr 0.0.0.0:8080 --data /var/lib/rc
Environment=RC_TOKENS=<worker-token>:worker,<client-token>:client,<admin-token>:admin
User=rc
Restart=always
RestartSec=2
StateDirectory=rc

[Install]
WantedBy=multi-user.target
```

### Tokens

`RC_TOKENS` is a comma-separated list of `token:role` pairs. Three roles:

| Role | Can do |
|---|---|
| `worker` | register a host, take assignments, report results |
| `client` | list, describe, submit, kill **your own** jobs |
| `admin` | everything, including clearing a quarantined device |

The controller refuses to start without it — an unauthenticated scheduler that
runs commands on your GPU hosts is not something to fall back to silently.

**These are shared secrets crossing the wire in the clear.** There is no TLS
and no per-client tokens. Put the controller behind a tunnel, a VPN, or a
private network. Give agents the *client* token only.

### Check it

```sh
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/v1/state   # 401 = up and routing
```

Then open `http://localhost:8080` and paste the client token.

---

## 2. A worker, on each GPU host

**The worker is not containerised, and this is deliberate.** It supervises the
processes that touch the hardware — it has to see them, signal them, and kill
their process group. A worker in a container supervising host processes is not
something this design supports.

Put the binary on the host:

```sh
# same binary as the controller
go install github.com/mudler/resource-controller/cmd/rc@latest
# or scp the static binary you built
```

Write `/etc/rc/worker.yaml`:

```yaml
controller_url: http://rc.internal.example:8080
token: <worker-token>
# host defaults to the machine's hostname; device IDs become <host>:<name>
devices:
  - name: gpu0
    max_runtime: 4h        # a ceiling this device enforces on every job
    labels: {vendor: nvidia, class: train}
  - name: gpu1
    max_runtime: 4h
    labels: {vendor: nvidia, class: train}
```

**Name devices after their GPU index.** The worker sets
`CUDA_VISIBLE_DEVICES` from the trailing integer, so `gpu1` runs its job with
`CUDA_VISIBLE_DEVICES=1`. There is no auto-discovery: the controller knows
only the devices you list.

Run it:

```sh
rc worker --config /etc/rc/worker.yaml
```

A systemd unit:

```ini
# /etc/systemd/system/rc-worker.service
[Unit]
Description=resource-controller worker
After=network-online.target

[Service]
ExecStart=/usr/local/bin/rc worker --config /etc/rc/worker.yaml
Restart=always
RestartSec=2
# Runs jobs as this user. It needs access to the GPUs and to whatever the
# jobs need — and note that jobs, probes, hooks and verify scripts all run
# as this user. Choose it accordingly.
User=rc

[Install]
WantedBy=multi-user.target
```

Confirm from anywhere with the client token:

```sh
rc devices
```

### Worth setting up while you are there

Optional, but this is where most of the value is:

- **A usage sheet.** `/etc/rc/host.md` — free-form Markdown explaining how
  this box is meant to be used. It shows up in `rc describe` and on the
  dashboard, and it is how an agent learns to reach the machine and what is
  already on it. Write the things you would otherwise have to tell someone.
- **Probes.** Executables in `/etc/rc/probe.d/` that print one flat JSON
  object of facts (`{"rack":"b12"}`). They become labels agents can select
  on. GPU model, driver version, CPU count, memory and free disk are already
  detected without you doing anything.
- **Verify probes.** Executables in `/etc/rc/verify.d/` that run after each
  job and exit non-zero if the device is not clean — the classic case being
  VRAM still pinned after a crash. A failure takes the device **out of the
  pool** before the next job can land on it.
- **Lease hooks.** `on_acquire` / `on_release` per device, to stop and start
  a service (LocalAI, ollama) around a job's hold on the GPU.

See the [README](../README.md) for each of these in detail.

> Probes, hooks and verify scripts are operator-supplied code, run as the
> worker's user with no sandboxing. Trust those directories exactly as much
> as you trust the worker account.

---

## 3. Clients and agents

Anywhere you submit work from:

```sh
export RC_CONTROLLER=http://rc.internal.example:8080
export RC_TOKEN=<client-token>
export RC_SUBMITTER="claude/$(hostname)/session-$$"   # optional; who you are

rc devices
rc run --select 'vram>=40G' -- python train.py
```

For AI agents, hand them [agents.md](agents.md) — it is written to be read by
an agent and covers discovery, claiming, holds, and the rules that keep a
shared fleet working.

---

## Upgrading

Controller: `docker compose pull && docker compose up -d`. Migrations run at
startup and are append-only, so an older database is brought forward
automatically. **Back up `--data` first anyway.**

Workers: replace the binary and restart. A worker restart does not disturb a
running job's *lease* — the controller reconciles what the worker reports on
re-registration — but the job's process group is killed with the worker, so
drain the host first if that matters.

Upgrade the controller before the workers.
