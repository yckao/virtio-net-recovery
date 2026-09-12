#!/bin/sh
set -eu
detach=
if [ "${1:-}" = "--detach" ]; then
    detach=--detach
    shift
fi
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 [--detach] --pid HOST_QEMU_PID | --domain-regex REGEX [agent options]" >&2
    exit 2
fi
case "$1" in
    [0-9]*) pid=$1; shift; set -- --pid "$pid" "$@" ;;
esac
mkdir -p /var/lib/vhost-watch
set -- "${VHOST_WATCH_IMAGE:-ghcr.io/yckao/virtio-net-recovery:main}" "$@"
if [ -n "${VHOST_WATCH_CPUSET:-}" ]; then
    set -- --cpuset-cpus "$VHOST_WATCH_CPUSET" "$@"
fi
exec "${CONTAINER_ENGINE:-podman}" run $detach --rm --name "${VHOST_WATCH_NAME:-vhost-watch}" --privileged --pid=host --network=none \
    --log-driver=k8s-file --log-opt=max-size=10mb \
    --read-only --security-opt label=disable \
    -v /sys/kernel/btf:/sys/kernel/btf:ro \
    -v /run/libvirt/qemu:/run/libvirt/qemu:ro \
    -v /var/lib/vhost-watch:/state:rw \
    "$@"
