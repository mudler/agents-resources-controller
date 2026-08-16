# The rc worker for a Kubernetes node, plus the kubectl its lease hooks need.
#
# GLIBC, NOT ALPINE — and this is the whole reason this image exists separately
# from the controller's. The NVIDIA container runtime injects the driver's
# userspace into a GPU container: nvidia-smi, libcuda, the lot. Those are
# glibc-linked, so under musl they are present but unloadable, and the first
# symptom is a confusing "fork/exec /usr/bin/nvidia-smi: no such file or
# directory" — the missing dynamic loader, not a missing file. Jobs would then
# fail to use the GPU at all, which is the entire point of the box.
#
# Verified on dgx: the working local-ai-worker container is Ubuntu, has
# /usr/bin/nvidia-smi injected, and reports "NVIDIA GB10". A musl base does
# not.
#
# rc itself is a static CGO_ENABLED=0 binary, so it runs unchanged here.
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

FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -u 10001 -m -s /usr/sbin/nologin rc

COPY --from=rc /usr/local/bin/rc /usr/local/bin/rc
COPY --from=fetch /kubectl /usr/local/bin/kubectl

USER rc

ENTRYPOINT ["rc"]
CMD ["worker", "--config", "/etc/rc/worker.yaml"]
