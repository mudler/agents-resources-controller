# rc-worker

`ghcr.io/mudler/rc-worker` is the `rc` binary plus `kubectl`, packaged for
running `rc worker` as a Kubernetes pod (specifically, a DaemonSet on
GPU-carrying nodes).

## Why a separate image

The controller image (`ghcr.io/mudler/resource-controller`) is Alpine plus a
static, cgo-free Go binary — no `kubectl`, no `curl`, nothing beyond what
`rc serve` needs. That's deliberate: the controller has no business talking
to Kubernetes.

A pod worker's lease hooks do need `kubectl` — they drain and uncordon the
node the pod is running on when a lease is acquired or released. Rather than
teach `rc` about Kubernetes, this image just adds `kubectl` alongside the
same `rc` binary. Hooks are ordinary executables; this image is what makes
`kubectl` one of the things they can call.

The Kubernetes *logic* — `kubectl drain --ignore-daemonsets`, `kubectl
uncordon`, and so on — lives in the infra repo's hook scripts, not in this
image and not in `rc` itself.

## Build

```
FROM ghcr.io/mudler/resource-controller:edge
```

This image is built from the controller image, so a publish run builds the
controller first and this second — a fresh `:edge` controller image must
exist before this one can pull it.

## kubectl version

`KUBECTL_VERSION` (build arg, default `v1.36.3`) must track the k3s server
version within one minor — the client skew policy allows one minor of drift
in either direction. The fleet this image targets runs k3s v1.36.2–v1.36.3.
Bump `KUBECTL_VERSION` deliberately when the boxes move to a new minor;
don't let it drift silently behind.

`TARGETARCH` is supplied by buildx per platform so a single Dockerfile
produces the right `kubectl` binary for both `linux/amd64` and
`linux/arm64` — never hardcode an architecture here.

## Entrypoint

```
ENTRYPOINT ["rc"]
CMD ["worker", "--config", "/etc/rc/worker.yaml"]
```

Run `rc worker --config /etc/rc/worker.yaml` by default; override `CMD` for
anything else (e.g. `rc --help`, or `kubectl` directly via
`--entrypoint kubectl`).
