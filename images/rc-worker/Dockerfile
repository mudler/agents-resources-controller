# The rc worker for a Kubernetes node, plus the kubectl its lease hooks need.
#
# UBUNTU 24.04, MATCHING THE HOSTS — and the base choice is the whole reason
# this image exists separately from the controller's. The NVIDIA container
# runtime injects the driver's userspace into a GPU container: nvidia-smi,
# libcuda, the lot. Those binaries are built against the HOST's glibc, so the
# container's base is not a free choice.
#
# Alpine broke this outright: musl leaves the injected binaries present but
# unloadable, and the symptom is a confusing "fork/exec /usr/bin/nvidia-smi: no
# such file or directory" — the missing dynamic loader, not a missing file.
#
# debian:bookworm-slim worked but was a mismatch waiting to happen. The rule is
# that the container's glibc must be at least as new as the host's, because the
# injected libraries are the host's; older-into-newer is fine, newer-into-older
# is not. Measured on the fleet:
#
#   dgx   Ubuntu 24.04.4  glibc 2.39
#   thor  Ubuntu 24.04.4  glibc 2.39
#   orin  Ubuntu 22.04.5  glibc 2.35
#
# bookworm's 2.36 sits BELOW dgx and thor. ubuntu:24.04 (2.39) is at or above
# every box, and is what NVIDIA's own CUDA images and the Jetsons' L4T stack are
# built on.
#
# Verified on dgx: the working local-ai-worker container is Ubuntu, has
# /usr/bin/nvidia-smi injected, and reports "NVIDIA GB10".
#
# rc itself is a static CGO_ENABLED=0 binary, so it runs unchanged on any base.
FROM alpine:3.21 AS fetch

# Must track the k3s server within one minor. The fleet runs v1.36.2/v1.36.3,
# verified on all three boxes.
ARG KUBECTL_VERSION=v1.36.3
ARG TARGETARCH

RUN apk add --no-cache curl \
 && curl -fsSLo /kubectl \
      "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" \
 && chmod 0755 /kubectl

FROM ghcr.io/mudler/resource-controller:edge AS rc

FROM ubuntu:24.04

# A JOB RUNS IN THIS CONTAINER, so this image is the environment every job
# gets. It used to carry only ca-certificates and tzdata, which made `rc run`
# almost unusable: no compiler, no python, no git, not even curl. The job
# history shows exactly what that cost — of the first 24 jobs ever submitted,
# nearly all were agents probing to find out what was here ("=== toolchain in
# worker container ===", "for t in gcc g++ cc cmake ninja make nvcc python3"),
# after which they gave up and took a `hold` to ssh to the host instead. Holds
# consumed 133 minutes of device time against 2 minutes for real jobs. The
# fleet's whole scheduling story was being bypassed because the container
# could not build anything.
#
# So: the toolchain a job plausibly needs, and apt available as root for the
# rest.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates tzdata \
      git curl wget openssh-client \
      build-essential cmake ninja-build pkg-config \
      python3 python3-pip python3-venv \
      jq rsync tar xz-utils unzip procps less vim-tiny \
 && rm -rf /var/lib/apt/lists/*

COPY --from=rc /usr/local/bin/rc /usr/local/bin/rc
COPY --from=fetch /kubectl /usr/local/bin/kubectl

# ROOT, DELIBERATELY. Jobs need to install what this image does not ship, and
# `apt-get install` as an unprivileged user is not a thing. The alternative was
# uid 10001 plus sudo, which is the same privilege with an extra word in front
# of every command.
#
# The blast radius is a container, not the box: the only host path mounted here
# is /workspace. Note that root is why the NAS stays writable regardless of the
# mount's forceuid/forcegid — see cloudconfig/nas-client.yaml in infra-flux-kube.
#
# The real cost is not privilege, it is lifetime: this pod is long-lived, so
# anything a job installs persists into every later job until the pod restarts.
# Convenient (one agent's cmake is the next agent's cmake) and occasionally
# not (a global pip install can break the next job). Documented in each host's
# usage sheet.
USER root

ENTRYPOINT ["rc"]
CMD ["worker", "--config", "/etc/rc/worker.yaml"]
