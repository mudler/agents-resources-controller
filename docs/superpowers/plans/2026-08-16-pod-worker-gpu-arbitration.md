# Pod worker and GPU arbitration on orin/thor/dgx — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `orin`, `thor` and `dgx` leasable through rc, with LocalAI evicted from the GPU for the life of a lease and restored afterwards.

**Architecture:** An rc worker runs on each box as a GPU-enabled **DaemonSet**, and jobs run inside that pod. Its `on_acquire` hook drains the node — evicting the LocalAI Deployment while leaving DaemonSets (itself, WireGuard) running — and `on_release` uncordons. The Deployment is never modified, so Flux sees no drift and has nothing to reconcile.

**Tech Stack:** k3s (single-node, one per box), Flux CD, NVIDIA container runtime, `kubectl` in a purpose-built image, `ghcr.io/mudler/resource-controller` as the rc binary source.

**Spec:** `docs/superpowers/specs/2026-08-16-ssh-leases-and-k8s-design.md`

**No rc code changes.** Everything here is a Dockerfile, manifests, and hook scripts. It uses `rc run`, `on_acquire`/`on_release` and labels exactly as they exist — deliberately, so the drain/uncordon design is proven on real hardware before the `--tty` work builds on it.

## Global Constraints

These were established by a pre-flight scan of the live fleet. Several correct
an earlier draft of this plan; each one is a fact about the environment, not a
preference.

- **`manifests/` is Flux-managed and Flux tracks `main`.** Deploying means
  **committing to `main`**, not running `kubectl apply`. Hand-applying would
  create drift against the repo whose entire job is to be the source of truth.
  A push to `main` deploys immediately — so deploy **one box at a time** and
  verify before moving on.
- **No kubeconfigs exist for these clusters, and none should be created.**
  Where cluster state genuinely must be read, **SSH to the box and use
  `k3s kubectl`**, which is root-local on a k3s node. No cluster-admin
  credential leaves a machine.
- **The SSH user differs per box, and the node name is never the box name.**
  Verified live:

  | box | ssh target | k8s node name | k3s |
  |---|---|---|---|
  | dgx | `mudler@10.9.0.7` | `kairos-17dd` | v1.36.3+k3s1 |
  | thor | `kairos@10.9.0.28` | `kairos-4db2` | v1.36.2+k3s1 |
  | orin | `kairos@10.9.0.6` | `agx-orin` | v1.36.3+k3s1 |

  Hooks must therefore take the node name from the downward API
  (`spec.nodeName`), never from `host:` in the worker config — those are
  different strings on every box. All three are arm64.
- **These clusters have no SOPS.** Secrets are applied out-of-band and never
  committed, following `manifests/dgx/wireguard.yaml`, which records its own
  recreate command in a comment. Do the same.
- **`--ignore-daemonsets` is mandatory on every drain.** WireGuard is a
  DaemonSet on all three boxes; evicting it removes the network path to the
  machine — from a hook running over that path. It is also what keeps the rc
  worker itself alive.
- **Never modify the `local-ai-worker` Deployment.** Not `replicas`, not
  annotations. It must keep matching git exactly so Flux has no drift to
  correct. Arbitration happens at the node level.
- **Mount only `/usr/local/nas_share/rc`**, never `/usr/local/nas_share`.
- **Do not commit `cloudconfig/nas-client.yaml`** — it is untracked work in
  progress belonging to the operator. Stage files by explicit path; never
  `git add -A` in the infra repo.
- The controller is at `http://10.9.0.23:8080` over WireGuard.

---

## File Structure

| File | Repo | Responsibility |
|---|---|---|
| `images/rc-worker/Dockerfile` | resource-controller | rc + kubectl, multi-arch. New. |
| `.github/workflows/publish.yml` | resource-controller | Also build and push the worker image. |
| `manifests/<box>/rc-worker.yaml` | infra-flux-kube | Namespace, ServiceAccount, RBAC, ConfigMap, DaemonSet. New, one per box. |
| `manifests/<box>/kustomization.yaml` | infra-flux-kube | Add `rc-worker.yaml`. |
| `docs/services.md` | infra-flux-kube | Record the fleet and what a lease costs. |
| `docs/install.md` | resource-controller | Document the pod deployment option. |

`<box>` order is `dgx`, then `thor`, then `orin`. One at a time: a mistake costs one box, not three.

**Why the image is built in the rc repo, not the infra repo.** The infra repo's
`.github/workflows/image.yaml` only triggers on **tags and pull requests**
(branch builds are commented out), publishes to `registry.c3os.io` behind
credentials this plan cannot assume, and its matrix entries are
`linux/amd64` — while all three boxes are arm64. rc's own `publish.yml`
already builds `linux/amd64,linux/arm64` and pushes to ghcr on every push to
master using `GITHUB_TOKEN`. Adding one image there is a few lines; making the
infra pipeline do it is a new registry credential, a platform change, and a
release process. The image is packaging, not Kubernetes code in rc — the
`--ignore-daemonsets` logic still lives in the infra repo's hooks.

---

### Task 1: The worker image

**Files:**
- Create: `images/rc-worker/Dockerfile`, `images/rc-worker/README.md` (resource-controller)
- Modify: `.github/workflows/publish.yml`

**Interfaces:**
- Produces: `ghcr.io/mudler/rc-worker:edge`, `linux/amd64,linux/arm64`, containing `/usr/local/bin/rc` and `/usr/local/bin/kubectl`.

The rc image is Alpine plus a static Go binary — no `kubectl`, no `curl`. The hooks need `kubectl`, so this composes the two rather than teaching rc about Kubernetes.

- [ ] **Step 1: Write the Dockerfile**

```dockerfile
# The rc worker, plus the kubectl its lease hooks need.
#
# rc itself knows nothing about Kubernetes and should not: hooks are ordinary
# executables, and this image is what makes `kubectl` one of the things they
# can call. The Kubernetes *logic* (drain, --ignore-daemonsets, uncordon)
# lives in the infra repo's hook scripts, not here and not in rc.
FROM alpine:3.21 AS kubectl

# Must track the k3s server within one minor. The fleet runs v1.36.2/v1.36.3
# (verified on all three boxes), so this is v1.36.x — NOT an older default.
# Bump deliberately when the boxes move.
ARG KUBECTL_VERSION=v1.36.3
ARG TARGETARCH

RUN apk add --no-cache curl \
 && curl -fsSLo /kubectl \
      "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" \
 && chmod 0755 /kubectl

FROM ghcr.io/mudler/resource-controller:edge

# The base image runs as the unprivileged `rc` user; adding a file needs root.
USER root
COPY --from=kubectl /kubectl /usr/local/bin/kubectl
USER rc

ENTRYPOINT ["rc"]
CMD ["worker", "--config", "/etc/rc/worker.yaml"]
```

`TARGETARCH` is set by buildx per platform, so one Dockerfile yields the right kubectl for both architectures. Do not hardcode `arm64` — the amd64 image would silently ship an arm64 binary.

- [ ] **Step 2: Add it to the publish workflow**

In `.github/workflows/publish.yml`, after the existing controller build, add a
second metadata + build pair for `ghcr.io/${{ github.repository_owner }}/rc-worker`
using `context: .`, `file: images/rc-worker/Dockerfile`, the same
`platforms: linux/amd64,linux/arm64`, and the same tag rules (`type=ref,event=branch`,
`type=raw,value=edge` on master, semver on tags).

The worker image depends on the controller image built earlier in the same
run, so it must come after it — a fresh `edge` must exist before this pulls it.

- [ ] **Step 3: Build locally for arm64 and verify both binaries**

```bash
cd ~/_git/resource-controller
docker buildx build --platform linux/arm64 -f images/rc-worker/Dockerfile -t rc-worker:test . --load
docker run --rm --platform linux/arm64 rc-worker:test --help | head -3
docker run --rm --platform linux/arm64 --entrypoint kubectl rc-worker:test version --client=true
```

Expected: rc prints its command list; kubectl prints a client version and the
architecture matches. If `--load` refuses the cross-platform image, use
`--output=type=docker` or verify on the box after publishing.

- [ ] **Step 4: Write `images/rc-worker/README.md`**

What it is, why it exists separately from the controller image, that
`KUBECTL_VERSION` must track k3s within one minor, and that the Kubernetes
logic lives in the infra repo's hooks rather than here.

- [ ] **Step 5: Commit, push, and confirm it publishes**

```bash
git add images/rc-worker .github/workflows/publish.yml
git commit -m "images: rc worker with kubectl, for Kubernetes lease hooks"
git push origin master
gh run watch    # or: gh run list --workflow=publish.yml --limit 1
```

Then confirm the tags exist and are multi-arch — the boxes cannot pull what
was never pushed, and this is the failure that would leave every pod in
`ImagePullBackOff`:

```bash
TOKEN=$(curl -s "https://ghcr.io/token?scope=repository:mudler/rc-worker:pull" | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
curl -s -H "Authorization: Bearer $TOKEN" https://ghcr.io/v2/mudler/rc-worker/tags/list
curl -s -H "Authorization: Bearer $TOKEN" -H "Accept: application/vnd.oci.image.index.v1+json" \
  https://ghcr.io/v2/mudler/rc-worker/manifests/edge | grep -o '"architecture":"[a-z0-9]*"' | sort -u
```

Expected: `edge` present, and both `amd64` and `arm64` listed.

**The package must also be public**, or the boxes cannot pull it without a
pull secret. New ghcr packages default to private: check
`https://github.com/users/mudler/packages/container/rc-worker/settings` and
make it public, or the plan needs an `imagePullSecret` on every box.

---

### Task 2: The dgx manifests

**Files:**
- Create: `manifests/dgx/rc-worker.yaml` (infra-flux-kube)
- Modify: `manifests/dgx/kustomization.yaml`

**Interfaces:**
- Consumes: `ghcr.io/mudler/rc-worker:edge` from Task 1.
- Produces: a registered `dgx:gpu0` device once Flux applies it and the Secret from Task 3 exists.

Everything for one box in one file, following how `localai-worker.yaml` and `wireguard.yaml` are each self-contained.

- [ ] **Step 1: Write the manifest**

Header comment first, recording the out-of-band Secret exactly as
`wireguard.yaml` does for its private key:

```yaml
# The rc worker for this box: it registers dgx:gpu0 with the controller at
# 10.9.0.23:8080 and runs leased jobs inside this pod.
#
# The worker token lives in a Secret applied out-of-band, never in git (this
# cluster has no SOPS — see clusters/dgx/manifests.yaml). Recreate it with:
#
#   ssh mudler@10.9.0.7 'sudo k3s kubectl -n rc create secret generic rc-worker-token \
#     --from-literal=token=<worker-token>'
#
# The token is the `worker`-role entry in the controller's RC_TOKENS
# (~/rc-deploy/.env on the controller host).
---
apiVersion: v1
kind: Namespace
metadata:
  name: rc
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: rc-worker
  namespace: rc
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: rc-worker-drain
# `kubectl drain` is not one API call: it lists the pods on the node, asks what
# controls each one (to skip DaemonSet-managed pods), patches the node to
# cordon it, and creates an Eviction per remaining pod. Missing any of these
# gives a hook that cordons and then cannot evict — the worst outcome, since
# the GPU is neither free nor available to LocalAI.
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/eviction"]
    verbs: ["create"]
  - apiGroups: ["apps"]
    resources: ["daemonsets", "replicasets", "statefulsets"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: rc-worker-drain
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: rc-worker-drain
subjects:
  - kind: ServiceAccount
    name: rc-worker
    namespace: rc
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: rc-worker
  namespace: rc
data:
  worker.yaml: |
    controller_url: http://10.9.0.23:8080
    host: dgx
    devices:
      - name: gpu0
        # NOTE: the key is `timeout`, not `hook_timeout` — see DeviceConfig in
        # internal/worker/config.go. It must exceed the drain's own
        # --timeout=5m below: the hook is not done until LocalAI has actually
        # terminated, and a lease granted before that is a lease over a GPU
        # still being released. The built-in default is 60s, which a multi-GB
        # CUDA process will exceed.
        timeout: 6m
        # release_linger delays on_release after a job ends, and cancels it if
        # another job lands first. It matters more here than anywhere else in
        # rc: LocalAI takes minutes to reload its models, so without it two
        # jobs thirty seconds apart each pay a full LocalAI restart in between
        # and then evict it again. Task 4 measures the real restart time —
        # revisit this with that number.
        release_linger: 2m
        on_acquire: /etc/rc/on-acquire.sh
        on_release: /etc/rc/on-release.sh
        labels:
          vendor: nvidia
          gpu_model: GB10
          class: train
          k8s: "true"

  on-acquire.sh: |
    #!/bin/sh
    # Take the GPU away from Kubernetes for the life of this lease.
    #
    # drain, not cordon: cordon only marks the node unschedulable and does
    # nothing to an already-running pod, so LocalAI would keep the GPU while
    # rc reported the box as leased. drain cordons AND evicts AND waits for
    # termination, which is what "the lease is granted" has to mean.
    #
    # --ignore-daemonsets is not hygiene. WireGuard is a DaemonSet, and
    # evicting it removes the network path to this machine from this script,
    # which runs over that path. It is also what keeps THIS pod alive: the rc
    # worker is a DaemonSet precisely so it survives the drain it triggers.
    #
    # The local-ai-worker Deployment is never touched. It keeps saying
    # replicas: 1 and keeps matching git, so Flux sees no drift; its
    # ReplicaSet simply cannot place a pod on a cordoned node.
    set -eu
    echo "rc: draining ${NODE_NAME} for ${RC_DEVICE} (job ${RC_JOB_ID}, ${RC_SUBMITTER:-unknown})"
    kubectl drain "${NODE_NAME}" \
      --ignore-daemonsets \
      --delete-emptydir-data \
      --timeout=5m
    echo "rc: ${NODE_NAME} drained; GPU is free"

  on-release.sh: |
    #!/bin/sh
    # Give the GPU back. Idempotent on purpose: uncordoning an uncordoned node
    # is a no-op, which matters because this also runs on paths where the
    # acquire hook may not have completed.
    set -eu
    echo "rc: uncordoning ${NODE_NAME} after ${RC_DEVICE}"
    kubectl uncordon "${NODE_NAME}"
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: rc-worker
  namespace: rc
spec:
  selector:
    matchLabels:
      app: rc-worker
  template:
    metadata:
      labels:
        app: rc-worker
    spec:
      serviceAccountName: rc-worker
      # A DaemonSet for two load-bearing reasons:
      #
      # 1. `drain --ignore-daemonsets` leaves DaemonSet pods running, so this
      #    worker survives the drain its own hook triggers. A Deployment would
      #    evict itself halfway through granting a lease.
      # 2. DaemonSet pods carry a built-in toleration for
      #    node.kubernetes.io/unschedulable, so this also starts on an ALREADY
      #    CORDONED node — exactly the crash-recovery case the initContainer
      #    below depends on.
      runtimeClassName: nvidia
      # The GPU comes from the runtime, not the device plugin — there is no
      # nvidia.com/gpu limit anywhere on these boxes — so this pod and
      # local-ai-worker can both SEE the GPU. They must not both USE it, and
      # that is what the lease is for: nothing in Kubernetes is arbitrating.
      initContainers:
        # If the pod died mid-lease the node is still cordoned and LocalAI is
        # locked out indefinitely and silently, which is worse than the problem
        # this solves. Uncordon on start.
        #
        # Safe, and the reason matters: any job this worker was running was a
        # child process inside this container, so it died with the pod.
        # Nothing is using the GPU by the time this runs.
        - name: uncordon
          image: ghcr.io/mudler/rc-worker:edge
          command: ["/usr/local/bin/kubectl"]
          args: ["uncordon", "$(NODE_NAME)"]
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
      containers:
        - name: worker
          image: ghcr.io/mudler/rc-worker:edge
          args: ["worker", "--config", "/etc/rc/worker.yaml"]
          env:
            # The hooks drain THIS node; they need to know which one it is.
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: RC_TOKEN
              valueFrom:
                secretKeyRef:
                  name: rc-worker-token
                  key: token
            - name: NVIDIA_VISIBLE_DEVICES
              value: "all"
            - name: NVIDIA_DRIVER_CAPABILITIES
              value: "compute,utility"
          volumeMounts:
            - name: config
              mountPath: /etc/rc
            - name: workspace
              mountPath: /workspace
      volumes:
        - name: config
          configMap:
            name: rc-worker
            # The hooks are executed, so the bit has to be set.
            defaultMode: 0755
        - name: workspace
          hostPath:
            # A DEDICATED SUBFOLDER of the NAS share, never the share itself:
            # jobs get shared storage without write access to everything else
            # on the NAS. /usr/local/nas_share is where cloudconfig/nas-client
            # mounts the Samba share on the node.
            path: /usr/local/nas_share/rc
            type: DirectoryOrCreate
```

- [ ] **Step 2: Add to the kustomization**

Add `rc-worker.yaml` to `resources` in `manifests/dgx/kustomization.yaml`.

- [ ] **Step 3: Validate before it can reach a cluster**

Flux applies whatever lands on `main`, so validation happens **here**, not after:

```bash
cd ~/_git/infra-flux-kube
kubectl kustomize manifests/dgx > /tmp/dgx-rendered.yaml && echo "kustomize OK"
grep -c "kind: DaemonSet" /tmp/dgx-rendered.yaml     # expect 2: wireguard + rc-worker
grep -n "nas_share" /tmp/dgx-rendered.yaml            # must be /usr/local/nas_share/rc
./scripts/validate.sh 2>/dev/null || echo "(no validate.sh hook for this path)"
```

- [ ] **Step 4: Commit — this is the deploy**

Stage by explicit path. Never `git add -A` here: `cloudconfig/nas-client.yaml` is the operator's untracked work.

```bash
git add manifests/dgx/rc-worker.yaml manifests/dgx/kustomization.yaml
git commit -m "dgx: run the rc worker as a GPU-enabled DaemonSet"
git push origin main
```

- [ ] **Step 5: Watch Flux apply it**

```bash
ssh mudler@10.9.0.7 'sudo k3s kubectl -n flux-system get kustomization manifests'
ssh mudler@10.9.0.7 'sudo k3s kubectl -n rc get pods -o wide'
```

Expected within ~10 minutes (or force it: `ssh mudler@10.9.0.7 'sudo k3s kubectl -n flux-system annotate kustomization manifests reconcile.fluxcd.io/requestedAt=$(date +%s) --overwrite'`).

The pod will be **CreateContainerConfigError** until Task 3 creates the Secret. That is expected, not a failure.

---

### Task 3: The worker token, and first registration

**Files:** none — one out-of-band action per box.

- [ ] **Step 1: Read the worker token from the controller**

```bash
grep RC_TOKENS ~/rc-deploy/.env | tr ',' '\n' | awk -F: '/worker/{print $1}' | sed 's/.*=//'
```

- [ ] **Step 2: Create the Secret on dgx**

```bash
ssh mudler@10.9.0.7 "sudo k3s kubectl -n rc create secret generic rc-worker-token \
  --from-literal=token='<worker-token>'"
ssh mudler@10.9.0.7 'sudo k3s kubectl -n rc delete pod -l app=rc-worker'   # pick it up now
```

- [ ] **Step 3: Confirm registration from the controller, not the cluster**

The controller is the authority on whether this worked:

```bash
export RC_CONTROLLER=http://127.0.0.1:8080 RC_TOKEN=<client-token>
rc list
rc describe dgx:gpu0
```

Expected: `dgx:gpu0  ready`, with declared labels (`gpu_model=GB10`, `class=train`) **and** detected ones (`cpus`, `kernel`, `mem_total_bytes`) — detected labels prove the worker is genuinely probing, not just registered.

If it is not there:

```bash
ssh mudler@10.9.0.7 'sudo k3s kubectl -n rc logs ds/rc-worker --tail=40'
```

---

### Task 4: Prove the arbitration on real hardware

**Files:** none — this is verification, and it is the point of the plan.

Everything before this is plumbing. This is where we learn whether draining a Kairos k3s node actually frees the GPU and gives it back.

- [ ] **Step 1: Record the resting state**

```bash
ssh mudler@10.9.0.7 'sudo k3s kubectl get pods -A -o wide; echo "---"; sudo k3s kubectl get node -o jsonpath="{.items[0].spec.unschedulable}"; echo'
```

Expected: `local-ai-worker` Running, `wireguard` Running, `rc-worker` Running, unschedulable empty.

- [ ] **Step 2: Take a lease and watch LocalAI leave**

```bash
export RC_CONTROLLER=http://127.0.0.1:8080 RC_TOKEN=<client-token>
rc run -d dgx:gpu0 --as "$USER@lab" -- sh -c '
  echo "--- GPU ---"; nvidia-smi --query-gpu=name,memory.used --format=csv
  echo "--- who else is on it ---"; nvidia-smi --query-compute-apps=pid,used_memory --format=csv
  echo "--- workspace ---"; touch /workspace/hello && ls -la /workspace
  sleep 90'
```

From a second terminal, while it runs:

```bash
ssh mudler@10.9.0.7 'sudo k3s kubectl get pods -A; echo "---"; sudo k3s kubectl get node -o jsonpath="{.items[0].spec.unschedulable}"; echo'
```

Six separate claims, each checked:
- `local-ai-worker` is **gone or Pending**, not Running.
- `wireguard` is **still Running** — if not, the drain took the network with it.
- `rc-worker` is **still Running** — it survived its own drain.
- the node reports `true` for unschedulable.
- the job's `nvidia-smi` shows **no other compute apps** on the GPU.
- `/workspace/hello` exists, and appears on the NAS under the `rc` subfolder.

- [ ] **Step 3: Watch it come back, and time it**

```bash
time ssh mudler@10.9.0.7 'sudo k3s kubectl -n local-ai rollout status deploy/local-ai-worker --timeout=10m'
ssh mudler@10.9.0.7 'sudo k3s kubectl get node -o jsonpath="{.items[0].spec.unschedulable}"; echo'
```

Expected: unschedulable empty again, LocalAI back to Running on its own.
**Record the elapsed time** — it is the real cost of a lease on this box, it
belongs in the docs, and it is the number `release_linger` should be tuned to.

- [ ] **Step 4: Prove Flux never noticed**

```bash
ssh mudler@10.9.0.7 'sudo k3s kubectl -n local-ai get deploy local-ai-worker -o jsonpath="{.spec.replicas}"; echo
sudo k3s kubectl -n flux-system get kustomization manifests'
```

Expected: `replicas` is `1` throughout — including during the lease — and the
Kustomization reports Ready with no drift. This is the claim that justified
drain over scaling; verify it rather than assume it.

- [ ] **Step 5: Prove the crash path**

The failure mode that matters is a stuck cordon leaving LocalAI down forever.

```bash
rc run -d dgx:gpu0 -- sleep 300 &
sleep 25
ssh mudler@10.9.0.7 'sudo k3s kubectl -n rc delete pod -l app=rc-worker'
sleep 45
ssh mudler@10.9.0.7 'sudo k3s kubectl get node -o jsonpath="{.items[0].spec.unschedulable}"; echo
sudo k3s kubectl -n rc get pods; sudo k3s kubectl -n local-ai get pods'
```

Expected: the node is uncordoned by the initContainer and LocalAI schedules
again. Confirm the replacement worker pod **started while the node was
cordoned** — that is the DaemonSet toleration doing its job. If it sits
Pending, the GPU is stranded after every worker crash and this design needs
rethinking before it goes to two more boxes.

- [ ] **Step 6: Write down what you observed**

Paste the real output of steps 2, 3 and 5 into the task report. Every claim in
this plan should become an observation.

---

### Task 5: thor

**Files:**
- Create: `manifests/thor/rc-worker.yaml`
- Modify: `manifests/thor/kustomization.yaml`

- [ ] **Step 1: Copy dgx's manifest and change only what differs**

`host: thor`, and `gpu_model` — take the real value from `rc describe` after
registration rather than guessing. Do **not** vary the RBAC, hooks or
DaemonSet structure between boxes: three copies differing only in identity are
easy to reason about; three that differ subtly are not.

- [ ] **Step 2: Validate, commit, push, and create the Secret**

Same as Tasks 2 and 3, with `ssh kairos@10.9.0.28`.

- [ ] **Step 3: Repeat Task 4's six checks on thor**

`thor` and `dgx` share an image policy (`versions/localai-worker.yaml`) and
move together, so a drain that behaves differently on one is worth knowing
about. Record the LocalAI restart time here too — it will differ.

---

### Task 6: orin

**Files:**
- Create: `manifests/orin/rc-worker.yaml`
- Modify: `manifests/orin/kustomization.yaml`

`orin` is reached as `kairos@10.9.0.6` (not `mudler`), and its node is named
`agx-orin`. Otherwise identical to thor.

- [ ] **Step 1: Write the manifest** (as Task 5, `host: orin`)

- [ ] **Step 2: Validate, commit, push**

- [ ] **Step 3: Create the Secret and repeat Task 4's six checks**

Same as Tasks 3 and 4, with `ssh kairos@10.9.0.6`.

---

### Task 7: Documentation

**Files:**
- Modify: `docs/services.md`, `README.md` (infra-flux-kube)
- Modify: `docs/install.md` (resource-controller)

- [ ] **Step 1: Record the fleet in the infra repo**

The controller's address and dashboard; which boxes carry an rc worker; that
taking a lease evicts LocalAI and **how long it measurably takes to return**;
the out-of-band token recreate command.

- [ ] **Step 2: Add the Kubernetes option to rc's install guide**

`docs/install.md` says the worker is not containerised and explains why. That
is still true for a worker supervising **host** processes, and now incomplete.
Add the pod deployment as a second, explicitly-scoped option: jobs run inside
the pod, so they get CUDA and the mounted volumes but not the host; and a box
runs one worker or the other, never both, because two workers declaring the
same GPU would have the fleet believe there are two.

- [ ] **Step 3: Commit both repos**

```bash
git -C ~/_git/infra-flux-kube add docs/services.md README.md
git -C ~/_git/infra-flux-kube commit -m "docs: the rc fleet and what a lease costs"
git -C ~/_git/resource-controller add docs/install.md
git -C ~/_git/resource-controller commit -m "docs: running the worker as a Kubernetes pod"
```

---

## Self-Review Notes

**Spec coverage.** Implements the spec's Kubernetes section in full: DaemonSet
worker (Task 2), drain-not-scale with `--ignore-daemonsets` (Task 2), the
hook-timeout-exceeds-drain rule (Task 2), uncordon-on-start (Task 2's
initContainer), and the dedicated NAS subfolder (Task 2). `rc run --tty`,
whole-host claims, `rc cp`, `rc list` and `rc lock` are deliberately absent —
they are the next two plans, and none is needed to make these boxes leasable.

**Corrections this revision makes to the first draft**, all from the pre-flight
scan:

- Deployment is **git → Flux**, not `kubectl apply`. The earlier draft
  hand-applied manifests to a cluster whose whole purpose is that git does
  that, which would have created drift against the source of truth.
- Cluster reads are **SSH + `k3s kubectl`**, not local kubeconfigs. None exist,
  and none should be created.
- The image is built by **rc's** pipeline. The infra repo's builds only on tags
  and PRs, targets `linux/amd64`, and needs `registry.c3os.io` credentials this
  plan cannot assume — while rc's already builds arm64 to ghcr on every push.
- The first draft's DaemonSet referenced an image **nothing ever published**.
  Every pod would have sat in `ImagePullBackOff`.

**One deviation from the spec, recorded as a decision.** The spec has
uncordon-on-start riding "the worker's boot-identity recovery". rc has no
startup-hook mechanism, and adding one to serve three boxes would be the wrong
place for it — so this uses a Kubernetes initContainer, which needs no rc
change and fits better. The safety argument is unchanged and is written into
the manifest.

**Known risks.**

- **Task 4 step 5 is the most likely to fail and the most consequential.** If a
  DaemonSet pod cannot start on a cordoned node, the GPU is stranded after any
  worker crash. Everything else here has a manual recovery; that does not.
- **Pushing to `main` deploys.** A broken manifest is live, not caught locally.
  Mitigated by rendering with `kubectl kustomize` first, by Flux's `wait: true`,
  and by doing one box at a time.
- **`--delete-emptydir-data` permanently deletes that data.** It is included
  because drain refuses outright when a pod uses an `emptyDir`, and a hook that
  refuses has already cordoned. Nothing on these boxes appears to use one
  today; a future workload landing here would.
- **`--force` is deliberately absent.** It would evict bare pods and delete
  them permanently with no controller to recreate them. Better a loud failure.
- **`release_linger: 2m` is a guess** until Task 4 step 3 measures the real
  LocalAI restart time. Revisit it with that number.
- **kubectl/k3s version skew.** `KUBECTL_VERSION` is pinned at v1.36.3 against
  a fleet running v1.36.2–v1.36.3. Nothing checks this automatically, and an
  earlier draft of this plan pinned v1.31.4 — five minors behind — which would
  have been well outside the supported skew.
- **The ghcr package must be public**, or every box needs a pull secret this
  plan does not create.
