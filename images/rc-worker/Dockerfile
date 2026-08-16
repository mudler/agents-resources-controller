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
