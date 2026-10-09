#!/bin/sh
set -eu
# Run a locally built image in the host PID namespace. Mount host procfs at its
# canonical path: the backend rejects a procfs/syscall PID namespace mismatch.
image=${VHOST_IMAGE:-vhost-agent:review}
exec docker run --rm --pid=host --privileged --network=host \
  -v /proc:/proc -v /sys:/sys -v /run/libvirt/qemu:/run/libvirt/qemu:ro \
  -v /run/vhost-agent:/run/vhost-agent "$image" "$@"
