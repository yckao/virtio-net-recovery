#!/bin/sh
set -eu
detach=
if [ "${1:-}" = "--detach" ]; then
    detach=--detach
    shift
fi
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 [--detach] --pid HOST_QEMU_PID --vhost-fd FD [fault options]" >&2
    exit 2
fi
mkdir -p /var/lib/vhost-watch
exec "${CONTAINER_ENGINE:-podman}" run $detach --rm --name "${VHOST_FAULT_NAME:-vhost-fault}" \
    --privileged --pid=host --network=none --read-only --security-opt label=disable \
    --tmpfs /tmp:rw,size=256m \
    -v /lib/modules:/lib/modules:ro -v /usr/src:/usr/src:ro \
    -v /sys/kernel/btf:/sys/kernel/btf:ro \
    -v /run/libvirt/qemu:/run/libvirt/qemu:ro \
    -v /var/lib/vhost-watch:/state:rw \
    "${VHOST_FAULT_IMAGE:-ghcr.io/yckao/virtio-net-recovery-fault:main}" "$@"
