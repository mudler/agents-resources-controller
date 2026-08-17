# Cloud-init examples for a device host

Two drop-in configs for a [Kairos](https://kairos.io) GPU box running the `rc`
worker. Neither is required to run `rc` — they solve problems that showed up
running the reference fleet, and they are here because both were expensive to
diagnose the first time.

They are cloud-config (yip) files. On Kairos they belong in `/oem`, which
survives image upgrades, and the numeric prefix sets the order:

```sh
sudo install -m 0600 -o root -g root 95-resilience.yaml /oem/95_resilience.yaml
sudo kairos-agent run-stage boot     # apply without rebooting
```

`/system/oem` is reset on upgrade, so local configuration goes in `/oem` as
`9x_*.yaml`. On a non-Kairos host, ignore the wrapper and take the units and
scripts out of the `files:` blocks — nothing in either file is Kairos-specific
beyond the packaging.

---

## `95-resilience.yaml` — keep the box reachable and out of reclaim stall

Three things that a GPU box under load turns out to need.

**A default-route guard.** systemd-networkd puts a link into a *terminal*
`failed` state when a netlink transaction times out — no retry, no DHCP
renewal, no route re-add, ever. Under memory pressure this happened twice in
three days on the reference fleet:

```
08:05:59  systemd-networkd: enP7s7: Could not set route: Connection timed out
08:05:59  systemd-networkd: enP7s7: Failed
```

The address and its on-link route survive, so the LAN keeps working and nothing
looks wrong until you need the internet — that box was unreachable over
WireGuard for 65 minutes before anyone noticed. A one-minute timer asks
networkd to reconfigure the link and, failing that, installs a worse-metric
static route from networkd's own recorded DHCP lease. It removes that fallback
again once networkd is healthy, because a fallback pointing at a gateway that
has since changed is worse than none.

**zram swap.** A box with no swap has nowhere to put anonymous pages, so
reclaim can only spin on the page cache; parallel CUDA compiles drove
sustained reclaim stall, and that stall is what timed out the netlink
transaction above. The size is a cap, not an allocation — an idle zram device
costs almost nothing. Defers to Jetson's `nvzramconfig.service` where that
exists, because whoever loads the `zram` module first wins and the loser's
`num_devices` is silently ignored.

**No IPv6 RA on the LAN NIC.** Only if your LAN advertises IPv6 with no IPv6
default route, as the reference one does: it buys nothing, it was the trigger
for the link failure above, and it makes container pulls fail whenever a
registry resolves to AAAA only. **Delete this stanza if your network actually
routes IPv6.**

## `91-nas-workspace.yaml` — shared storage for `/workspace`

Mounts an SMB share so jobs on different boxes see the same `/workspace`.
`rc` does not copy files for you, so shared storage is how data gets to a job
and results get back.

**Replace `NAS_IP_HERE` and `NAS_PORT_HERE`**, and set real credentials if your
share needs them. Two traps encoded in the comments, both of which cost a day:

- `mount.cifs` (the userspace helper) is missing from many minimal images even
  though the kernel module is present, and the error is a misleading "cannot
  mount read-only". The helper here calls `mount(2)` directly.
- `forceuid`/`forcegid` make an in-container `chown` a silent no-op, so getting
  the mount's `uid`/`gid` right is the only thing that makes the share writable
  by the worker.

Point the worker's workspace at a **dedicated subdirectory** of the share
rather than its root, so jobs get shared storage without write access to
everything else on it.
