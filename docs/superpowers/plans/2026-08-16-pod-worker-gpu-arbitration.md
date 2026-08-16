# Pod worker and GPU arbitration on orin/thor/dgx — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `orin`, `thor` and `dgx` leasable through rc, with LocalAI evicted from the GPU for the life of a lease and restored afterwards.

**Architecture:** An rc worker runs on each box as a GPU-enabled **DaemonSet**, and jobs run inside that pod. Its `on_acquire` hook drains the node — evicting the LocalAI Deployment while leaving DaemonSets (itself, WireGuard) running — and `on_release` uncordons. The Deployment is never modified, so Flux sees no drift and has nothing to reconcile.

**Tech Stack:** k3s (single-node, one per box), Flux CD, NVIDIA container runtime, `kubectl` in a purpose-built image, `ghcr.io/mudler/resource-controller` as the rc binary source.

**Spec:** `docs/superpowers/specs/2026-08-16-ssh-leases-and-k8s-design.md`

**This plan needs no changes to the rc codebase.** Everything here is manifests, a small image, and hook scripts in `~/_git/infra-flux-kube`. It uses `rc run`, `on_acquire`/`on_release` and the label system exactly as they already exist. That is deliberate: it proves the drain/uncordon design on real hardware before the `--tty` work builds on top of it.

## Global Constraints

- **All three boxes are arm64.** `dgx` is a GB10, `thor` and `orin` are Jetson-class. Any image must be `linux/arm64`. `ghcr.io/mudler/resource-controller` is already multi-arch.
- **These clusters have no SOPS.** `clusters/dgx/manifests.yaml` says so explicitly. Secrets are **applied out-of-band and never committed**, following the pattern in `manifests/dgx/wireguard.yaml` — which documents its own recreate command in a comment. Do the same.
- **`--ignore-daemonsets` is mandatory on every drain.** WireGuard runs as a DaemonSet on all three boxes; evicting it removes the network path to the machine, from a hook running over that path. It is also what keeps the rc worker itself alive.
- **Never modify the `local-ai-worker` Deployment.** Not `replicas`, not annotations. It must keep matching git exactly so Flux has no drift to correct. Arbitration happens at the node level.
- **Mount only `/usr/local/nas_share/rc`**, never `/usr/local/nas_share`. Jobs get a dedicated folder, not the whole NAS.
- The controller is reachable at `http://10.9.0.23:8080` over WireGuard (this machine is `10.9.0.23`; `dgx` is `10.9.0.7`).

---

## File Structure

| File | Responsibility |
|---|---|
| `images/rc-worker/Dockerfile` | rc binary + kubectl, arm64. New. |
| `manifests/<box>/rc-worker.yaml` | ServiceAccount, RBAC, ConfigMap (worker config + hooks), DaemonSet. New, one per box. |
| `manifests/<box>/kustomization.yaml` | Add `rc-worker.yaml`. |
| `docs/services.md` | Record the controller and what the worker does. |

`<box>` is `dgx`, then `thor`, then `orin`. They are deployed one at a time, in that order, so a mistake costs one box rather than three.

---

### Task 1: The worker image

**Files:**
- Create: `images/rc-worker/Dockerfile`, `images/rc-worker/README.md`

**Interfaces:**
- Produces: an image containing `/usr/local/bin/rc` and `/usr/local/bin/kubectl`, runnable as `rc worker --config /etc/rc/worker.yaml`.

The rc image is Alpine plus a static Go binary — no `kubectl`, and no `curl`. The hooks need `kubectl`, so this composes the two rather than teaching rc about Kubernetes.

- [ ] **Step 1: Write the Dockerfile**

```dockerfile
# The rc worker, plus the kubectl its lease hooks need.
#
# rc itself knows nothing about Kubernetes and should not: the hooks are
# ordinary executables, and this image is what makes `kubectl` one of the
# things they can call. Keeping the composition here rather than in rc's own
# image means rc does not grow a Kubernetes dependency to serve three boxes.
FROM ghcr.io/mudler/resource-controller:edge AS rc

FROM alpine:3.21

ARG KUBECTL_VERSION=v1.31.4

RUN apk add --no-cache ca-certificates tzdata curl bash \
 && curl -fsSLo /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/arm64/kubectl" \
 && chmod 0755 /usr/local/bin/kubectl \
 && kubectl version --client=true

COPY --from=rc /usr/local/bin/rc /usr/local/bin/rc

# Jobs run as this user inside the pod. It needs to own the NAS subfolder
# mount, so the numeric id matters — it is what appears on the NAS.
RUN adduser -D -u 10001 rc
USER rc

ENTRYPOINT ["rc"]
CMD ["worker", "--config", "/etc/rc/worker.yaml"]
```

- [ ] **Step 2: Build it for arm64 and verify both binaries**

```bash
cd ~/_git/infra-flux-kube
docker buildx build --platform linux/arm64 -t rc-worker:test images/rc-worker --load
docker run --rm --platform linux/arm64 rc-worker:test --help | head -3
docker run --rm --platform linux/arm64 --entrypoint kubectl rc-worker:test version --client=true
```

Expected: `rc --help` prints the command list; `kubectl version` prints a client version. If `--load` refuses a cross-platform image, push to a registry instead and pull it on the box.

- [ ] **Step 3: Write `images/rc-worker/README.md`**

Record: what the image is, that `KUBECTL_VERSION` is a build arg, that it must track the k3s server version within one minor, and that the rc binary comes from the published multi-arch image rather than being rebuilt here.

- [ ] **Step 4: Commit**

```bash
git add images/rc-worker
git commit -m "images: rc worker with kubectl for lease hooks"
```

---

### Task 2: RBAC, so the hooks can drain their own node

**Files:**
- Create: `manifests/dgx/rc-worker.yaml` (ServiceAccount + ClusterRole + ClusterRoleBinding sections only; the DaemonSet arrives in Task 4)

**Interfaces:**
- Produces: ServiceAccount `rc-worker` in namespace `rc`, bound to a ClusterRole permitting cordon, eviction, and the reads `kubectl drain` performs.

`kubectl drain` is not one API call. It lists pods on the node, asks what controls each one (to decide whether a pod is DaemonSet-managed and therefore skippable), patches the node to cordon it, and creates an Eviction for each remaining pod. The role has to cover all of that or the hook fails halfway, having already cordoned.

- [ ] **Step 1: Write the manifest**

```yaml
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
rules:
  # Cordon and uncordon: a patch on the node's spec.unschedulable.
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "patch"]
  # drain lists what is on the node, then evicts it.
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/eviction"]
    verbs: ["create"]
  # drain asks what owns each pod so it can skip DaemonSet-managed ones.
  # Without these it refuses rather than guessing.
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
```

- [ ] **Step 2: Apply and verify each permission**

```bash
kubectl --kubeconfig ~/.kube/dgx apply -f manifests/dgx/rc-worker.yaml
for v in "patch nodes" "create pods/eviction" "list pods" "get daemonsets.apps"; do
  echo -n "$v -> "
  kubectl --kubeconfig ~/.kube/dgx auth can-i $v \
    --as=system:serviceaccount:rc:rc-worker
done
```

Expected: `yes` for all four. A `no` here becomes a hook that cordons and then cannot evict, which is the worst of both.

- [ ] **Step 3: Commit**

```bash
git add manifests/dgx/rc-worker.yaml
git commit -m "dgx: rbac for the rc worker to drain its own node"
```

---

### Task 3: The hooks and the worker config

**Files:**
- Modify: `manifests/dgx/rc-worker.yaml` (add a ConfigMap)

**Interfaces:**
- Consumes: the ServiceAccount from Task 2.
- Produces: ConfigMap `rc-worker` with keys `worker.yaml`, `on-acquire.sh`, `on-release.sh`, mounted at `/etc/rc/`.

- [ ] **Step 1: Add the ConfigMap**

```yaml
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
    sheet_dir: /etc/rc/sheets
    devices:
      - name: gpu0
        # NOTE the key is `timeout`, not `hook_timeout` — see DeviceConfig in
        # internal/worker/config.go. Longer than the drain's own --timeout=5m
        # below: the hook is not finished until LocalAI has actually
        # terminated, and a lease granted before that is a lease over a GPU
        # still being released. The built-in default is 60s, which a multi-GB
        # CUDA process will exceed.
        timeout: 6m
        # release_linger delays on_release after a job ends, and cancels it
        # outright if another job lands on this device first. It matters more
        # here than anywhere else in rc: LocalAI takes minutes to reload its
        # models, so without a linger, two agent jobs thirty seconds apart pay
        # for a full LocalAI restart in between and then evict it again. Two
        # minutes means back-to-back work does not thrash the GPU. The default
        # is 30s; raise it here deliberately.
        release_linger: 2m
        on_acquire: /etc/rc/on-acquire.sh
        on_release: /etc/rc/on-release.sh
        labels:
          vendor: nvidia
          gpu_model: GB10
          class: train
          k8s: "true"

  on-acquire.sh: |
    #!/bin/bash
    # Take the GPU away from Kubernetes for the life of this lease.
    #
    # drain, not cordon: cordon only marks the node unschedulable and does
    # nothing to a pod that is already running, so LocalAI would keep the GPU
    # while rc reported the box as leased. drain cordons AND evicts AND waits
    # for termination, which is what "the lease is granted" has to mean.
    #
    # --ignore-daemonsets is not hygiene. WireGuard is a DaemonSet and evicting
    # it removes the network path to this machine, from this script, which is
    # running over that path. It is also what keeps THIS pod alive: the rc
    # worker is a DaemonSet precisely so it survives the drain it triggers.
    #
    # The local-ai-worker Deployment is never modified. It keeps saying
    # replicas: 1 and keeps matching git, so Flux sees no drift and has nothing
    # to reconcile; its ReplicaSet simply cannot place a pod on a cordoned node.
    set -euo pipefail
    echo "rc: draining ${NODE_NAME} for ${RC_DEVICE} (job ${RC_JOB_ID}, ${RC_SUBMITTER:-unknown})"
    kubectl drain "${NODE_NAME}" \
      --ignore-daemonsets \
      --delete-emptydir-data \
      --timeout=5m
    echo "rc: ${NODE_NAME} drained; GPU is free"

  on-release.sh: |
    #!/bin/bash
    # Give the GPU back. Idempotent: uncordoning an uncordoned node is a no-op,
    # which matters because this also runs on paths where the acquire hook may
    # not have completed.
    set -euo pipefail
    echo "rc: uncordoning ${NODE_NAME} after ${RC_DEVICE}"
    kubectl uncordon "${NODE_NAME}"
```

- [ ] **Step 2: Check the hook environment is what the scripts assume**

`RC_DEVICE`, `RC_JOB_ID` and `RC_SUBMITTER` come from rc (see `internal/worker/hooks.go`'s `hookEnv`). `NODE_NAME` does not — it comes from the downward API in Task 4. Confirm rc's names before relying on them:

```bash
grep -n "RC_EVENT\|RC_DEVICE\|RC_JOB_ID\|RC_SUBMITTER" \
  ~/_git/resource-controller/internal/worker/hooks.go
```

Expected: all four are set by `hookEnv`. If a name differs, fix the scripts, not rc.

- [ ] **Step 3: Commit**

```bash
git add manifests/dgx/rc-worker.yaml
git commit -m "dgx: rc worker config and drain/uncordon lease hooks"
```

---

### Task 4: The DaemonSet

**Files:**
- Modify: `manifests/dgx/rc-worker.yaml` (add the DaemonSet), `manifests/dgx/kustomization.yaml`

**Interfaces:**
- Consumes: the ServiceAccount and ConfigMap above.
- Produces: a running rc worker on `dgx` that registers `dgx:gpu0` with the controller.

- [ ] **Step 1: Add the DaemonSet**

```yaml
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
      # A DaemonSet for two reasons, and both are load-bearing:
      #
      # 1. `drain --ignore-daemonsets` leaves DaemonSet pods running, so this
      #    worker survives the very drain its own hook triggers. A Deployment
      #    would evict itself halfway through granting a lease.
      # 2. DaemonSet pods carry a built-in toleration for
      #    node.kubernetes.io/unschedulable, so this also starts on an ALREADY
      #    CORDONED node — which is exactly the crash-recovery case the
      #    initContainer below handles.
      runtimeClassName: nvidia
      # The GPU comes from the runtime, not the device plugin (no
      # nvidia.com/gpu limit anywhere on these boxes), so this pod and
      # local-ai-worker can both SEE the GPU. They must not both USE it, and
      # that is what the lease is for — nothing in Kubernetes is arbitrating.
      initContainers:
        # If the pod died mid-lease, the node is still cordoned and LocalAI is
        # locked out indefinitely and silently — worse than the problem this
        # solves. Uncordon on start.
        #
        # This is safe, and the reason is worth stating: any job this worker
        # was running was a child process inside this container, so it died
        # with the pod. Nothing is using the GPU by the time this runs.
        - name: uncordon
          image: ghcr.io/mudler/rc-worker:edge
          command: ["/bin/bash", "-c"]
          args:
            - |
              set -euo pipefail
              echo "rc: startup uncordon of ${NODE_NAME}"
              kubectl uncordon "${NODE_NAME}"
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
            # The hooks are executed, so they need the bit set. A ConfigMap
            # mount is read-only, which is fine — nothing should rewrite them.
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

- [ ] **Step 2: Create the token Secret out-of-band**

These clusters have no SOPS, so this follows `manifests/dgx/wireguard.yaml`'s pattern — applied by hand, never committed, with the recreate command recorded in a comment at the top of `rc-worker.yaml`:

```bash
kubectl --kubeconfig ~/.kube/dgx -n rc create secret generic rc-worker-token \
  --from-literal=token="$(grep RC_TOKENS ~/rc-deploy/.env | tr ',' '\n' | awk -F: '/worker/{print $1}' | sed 's/.*=//')"
```

Add that command to the manifest's header comment, exactly as WireGuard's does.

- [ ] **Step 3: Add to the kustomization and deploy**

```bash
# add rc-worker.yaml to resources in manifests/dgx/kustomization.yaml
kubectl --kubeconfig ~/.kube/dgx apply -k manifests/dgx
kubectl --kubeconfig ~/.kube/dgx -n rc rollout status ds/rc-worker --timeout=5m
kubectl --kubeconfig ~/.kube/dgx -n rc logs ds/rc-worker | tail -20
```

Expected: `registered worker_id=... host=dgx devices=[{Name:gpu0 ...}]`.

- [ ] **Step 4: Confirm the controller sees it**

```bash
RC_CONTROLLER=http://127.0.0.1:8080 RC_TOKEN=<client> rc list
RC_CONTROLLER=http://127.0.0.1:8080 RC_TOKEN=<client> rc describe dgx:gpu0
```

Expected: `dgx:gpu0  ready`, with labels including `gpu_model=GB10` and the detected ones (`cpus`, `kernel`, `mem_total_bytes`).

- [ ] **Step 5: Commit**

```bash
git add manifests/dgx/rc-worker.yaml manifests/dgx/kustomization.yaml
git commit -m "dgx: run the rc worker as a GPU-enabled DaemonSet"
```

---

### Task 5: Prove the arbitration on real hardware

**Files:** none — this task is verification, and it is the point of the whole plan.

Everything before this is plumbing. This is where we find out whether draining a Kairos k3s node actually frees the GPU and gives it back.

- [ ] **Step 1: Record the resting state**

```bash
kubectl --kubeconfig ~/.kube/dgx get pods -A -o wide
kubectl --kubeconfig ~/.kube/dgx get node -o jsonpath='{.items[0].spec.unschedulable}'; echo
```

Expected: `local-ai-worker` Running, `wireguard` Running, `rc-worker` Running, node not cordoned (empty output).

- [ ] **Step 2: Take a lease and watch LocalAI leave**

```bash
export RC_CONTROLLER=http://127.0.0.1:8080 RC_TOKEN=<client>
rc run -d dgx:gpu0 --as "$USER@lab" -- bash -c '
  echo "--- inside the job ---"; nvidia-smi --query-gpu=name,memory.used --format=csv
  echo "--- who else is on the GPU ---"; nvidia-smi --query-compute-apps=pid,used_memory --format=csv
  echo "--- workspace ---"; touch /workspace/hello && ls -la /workspace
  sleep 60'
```

While it runs, from another terminal:

```bash
kubectl --kubeconfig ~/.kube/dgx get pods -A
kubectl --kubeconfig ~/.kube/dgx get node -o jsonpath='{.items[0].spec.unschedulable}'; echo
```

Expected, and each of these is a separate claim to check:
- `local-ai-worker` is **gone or Pending**, not Running.
- `wireguard` is **still Running** — if it is not, the drain took the network with it.
- `rc-worker` is **still Running** — it survived its own drain.
- the node reports `true` for unschedulable.
- the job's `nvidia-smi` shows **no other compute apps** on the GPU.
- `/workspace/hello` was created, and appears on the NAS under the `rc` subfolder.

- [ ] **Step 3: Watch it come back**

After the job exits:

```bash
kubectl --kubeconfig ~/.kube/dgx get node -o jsonpath='{.items[0].spec.unschedulable}'; echo
kubectl --kubeconfig ~/.kube/dgx -n local-ai rollout status deploy/local-ai-worker --timeout=5m
```

Expected: unschedulable is empty again, and LocalAI returns to Running on its own. Note how long it takes — a multi-GB CUDA image reloading its models is the real cost of a lease on these boxes, and it belongs in the docs.

- [ ] **Step 4: Prove Flux never noticed**

```bash
flux --kubeconfig ~/.kube/dgx get kustomization manifests
kubectl --kubeconfig ~/.kube/dgx -n local-ai get deploy local-ai-worker -o jsonpath='{.spec.replicas}'; echo
```

Expected: the Kustomization is `Ready`/applied with no drift reported, and `replicas` is still `1` throughout — including during the lease. This is the claim that justified drain over scaling; verify it rather than assume it.

- [ ] **Step 5: Prove the crash path**

The failure mode that matters is a stuck cordon leaving LocalAI down forever.

```bash
# take a lease, then kill the worker mid-lease
rc run -d dgx:gpu0 -- sleep 300 &
sleep 20
kubectl --kubeconfig ~/.kube/dgx -n rc delete pod -l app=rc-worker
# the DaemonSet recreates it; the initContainer should uncordon
sleep 30
kubectl --kubeconfig ~/.kube/dgx get node -o jsonpath='{.items[0].spec.unschedulable}'; echo
kubectl --kubeconfig ~/.kube/dgx -n local-ai get pods
```

Expected: the node is uncordoned by the initContainer and LocalAI schedules again. Confirm the new worker pod started **despite the node being cordoned when it was created** — that is the DaemonSet toleration doing its job, and if it did not work the pod would sit Pending forever with the GPU stranded.

- [ ] **Step 6: Write down what you observed**

Append the actual output of steps 2, 3 and 5 to `docs/services.md` under a `dgx` section. Every claim in this plan should become an observation, and the LocalAI restart time is an operational fact worth knowing before someone takes a lease casually.

---

### Task 6: Roll out to thor and orin

**Files:**
- Create: `manifests/thor/rc-worker.yaml`, `manifests/orin/rc-worker.yaml`
- Modify: both `kustomization.yaml` files

- [ ] **Step 1: Copy and adjust for thor**

Copy `manifests/dgx/rc-worker.yaml` and change only:
- `host: thor` in `worker.yaml`
- `gpu_model` to thor's actual model — take it from `rc describe` after registration rather than guessing
- anything the box genuinely differs on

Do **not** vary the RBAC, the hooks, or the DaemonSet structure between boxes. Three copies that differ only in identity are easier to reason about than one abstraction; three copies that differ subtly are not.

- [ ] **Step 2: Deploy thor and repeat Task 5's verification on it**

The same six checks. `thor` and `dgx` share an image policy (`versions/localai-worker.yaml`) so they move together; a drain that behaves differently on one is worth knowing about.

- [ ] **Step 3: Then orin, the same way**

`orin` is on WireGuard as `10.9.0.6` and was recently moved (`68aad97`); confirm it can reach `10.9.0.23:8080` before deploying:

```bash
kubectl --kubeconfig ~/.kube/orin -n rc logs ds/rc-worker | grep -i "register\|refused\|timeout"
```

- [ ] **Step 4: Commit**

```bash
git add manifests/thor manifests/orin
git commit -m "thor, orin: run the rc worker as a GPU-enabled DaemonSet"
```

---

### Task 7: Document it

**Files:**
- Modify: `docs/services.md`, `README.md` (infra repo)
- Modify: `~/_git/resource-controller/docs/install.md`

- [ ] **Step 1: Record the fleet in the infra repo**

In `docs/services.md`: the controller's address and dashboard, which boxes carry an rc worker, that taking a lease evicts LocalAI and how long it takes to come back, and the out-of-band token recreate command.

- [ ] **Step 2: Add a Kubernetes section to rc's install guide**

`docs/install.md` currently says the worker is not containerised and explains why. That is still true for a worker supervising **host** processes — and now incomplete. Add the pod deployment as a second, explicitly-scoped option: jobs run inside the pod, so they get CUDA and the mounted volumes but not the host, and a box may run one worker or the other, never both, because two workers declaring the same GPU would have the fleet believe there are two.

- [ ] **Step 3: Commit both repos**

```bash
git -C ~/_git/infra-flux-kube add docs README.md && \
  git -C ~/_git/infra-flux-kube commit -m "docs: the rc fleet and what a lease does to LocalAI"
git -C ~/_git/resource-controller add docs/install.md && \
  git -C ~/_git/resource-controller commit -m "docs: running the worker as a Kubernetes pod"
```

---

## Self-Review Notes

**Spec coverage.** This plan implements the spec's Kubernetes section in full: DaemonSet worker (Task 4), drain-not-scale with `--ignore-daemonsets` (Task 3), the hook-timeout-exceeds-drain rule (Task 3), uncordon-on-start via initContainer (Task 4), and the dedicated NAS subfolder (Task 4). The spec's `rc run --tty`, whole-host claims, `rc cp`, `rc list` and `rc lock` are **deliberately not here** — they are the next two plans, and none of them is needed to make these boxes leasable.

**One deviation from the spec, stated so it is a decision rather than a gap.** The spec's uncordon-on-start rides "the worker's boot-identity recovery". rc has no startup-hook mechanism, and adding one to serve three boxes would be the wrong place to put it — so this plan uses a Kubernetes initContainer instead, which needs no rc change and is the more natural fit. The safety argument is unchanged and is written into the manifest: any job the worker was running was a child inside its container and died with the pod, so nothing holds the GPU when the initContainer runs.

**Known risks.**

- **Task 5 step 5 is the one most likely to fail**, and it is the one that matters: if a DaemonSet pod cannot start on a cordoned node, the GPU is stranded after any worker crash. Everything else has a manual recovery; that one does not.
- **`--delete-emptydir-data` is included** because drain refuses outright when a pod uses an `emptyDir`, and a hook that refuses has already cordoned. It permanently deletes that data. Nothing on these boxes appears to use `emptyDir`, so if drain would have succeeded without it the flag costs nothing — but it is a live risk if a future workload lands on one of these nodes.
- **`--force` is deliberately absent.** It would evict bare pods, deleting them permanently with no controller to recreate them. Better that a drain fails loudly and someone looks.
- **`release_linger` is a guess.** Two minutes is chosen so consecutive jobs
  do not each pay a LocalAI restart, but the right value is however long
  LocalAI actually takes to become useful again — which Task 5 step 3
  measures. Revisit it with that number rather than leaving it at a guess.
- **kubectl/k3s version skew.** `KUBECTL_VERSION` is pinned in the image and must stay within one minor of the k3s server. Nothing checks this automatically.
