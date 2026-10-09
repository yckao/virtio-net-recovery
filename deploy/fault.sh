#!/bin/sh
set -eu
# Explicit experimental image; never used by the production agent entrypoint.
image=${VHOST_FAULT_IMAGE:-vhost-faultlab:review}
exec docker run --rm --pid=host --privileged --network=host \
  -v /proc:/proc -v /sys:/sys -v /lib/modules:/lib/modules:ro \
  -v /usr/src:/usr/src:ro -v /run/vhost-agent:/run/vhost-agent "$image" "$@"
