# virtio-net-recovery

A Go Host agent for virtio-net / vhost-net TX queues that may stop making progress. It observes queue progress and can write a verified kick to the queue's eventfd to wake vhost. This is a recovery workaround; it does not identify or fix the underlying cause.

The agent runs in a container on the QEMU Host. It supports multiple QEMU PIDs, libvirt domain name regular expressions, automatic recovery, and manual one-shot recovery. A separate container injects a bounded lost wakeup entirely on the Host. Neither requires software installation in the guest.

## Requirements

- Linux x86-64, QEMU with vhost-net, little-endian split virtqueues, and kernel BTF (`/sys/kernel/btf`). Packed rings and IOMMU-translated rings are unsupported.
- Rootful Podman, Host PID namespace, and permission to use BPF, `pidfd_getfd`, and `process_vm_readv`. The supplied launchers use privileged containers.
- For domain regex selection, mount the libvirt runtime XML directory (normally `/run/libvirt/qemu`). This is read-only discovery and requires no libvirt socket access.
- For fault injection, matching Host kernel headers under `/lib/modules` and `/usr/src`, loadable kernel modules, and a matching compiler. The fault image includes GCC 12; the verified kernel is Ubuntu `6.8.0-52-generic`. Other kernels require validation of their vhost internals. Kernel lockdown or module-signing policy may prevent injection.

The current repository and GHCR packages are private. Authenticate before pulling with a token authorized to read these packages:

```sh
# Read GHCR_TOKEN from your credential manager; do not put the token in this file.
printf '%s' "$GHCR_TOKEN" | sudo podman login ghcr.io -u YOUR_GITHUB_USER --password-stdin
sudo podman pull ghcr.io/yckao/virtio-net-recovery:main
sudo podman pull ghcr.io/yckao/virtio-net-recovery-fault:main
```

## Select and inspect VMs

Run the supplied launchers on the Host. `--pid` accepts repeated flags or a comma-separated list. Explicit PIDs and regex matches form a deduplicated union. Regex uses Go syntax; anchor it when selecting exact names.

```sh
sudo env VHOST_WATCH_NAME=vhost-list ./deploy/run.sh \
  --domain-regex '^worker-' --list

sudo env VHOST_WATCH_NAME=vhost-queues ./deploy/run.sh \
  --pid 1234 --list-queues
```

`--list-queues` reports the current QEMU `vhost_fd` for each configured TX slot. A vhost FD is process-local and can change after restart or device reconfiguration; it is not a guest queue number. Select it from a fresh listing.

## Start the agent

Observe without writing recovery kicks:

```sh
sudo ./deploy/run.sh --detach --pid 1234,5678 --mode observe
```

Enable guarded recovery for matching domains:

```sh
sudo ./deploy/run.sh --detach --domain-regex '^worker-' \
  --mode guarded --batch-rings --interval 0.02 --threshold 0.04
```

The example checks ring progress every 20 ms, then confirms a suspected stall against live vhost state twice before writing. A shared BPF snapshot probe serves all selected VMs. Recovery verifies the current QEMU process, vhost attachment, and eventfd identity. It then checks for consumed/used progress. This confirms Host queue progress; service health needs a separate application check.

Per-queue defaults limit guarded recovery to one attempt every 30 seconds and three attempts per hour. Domain selection refreshes every five seconds (`--target-interval 5s`) and follows domain restarts. Explicit PIDs never follow a reused PID. Each VM has a separate observer lock and recovery state; use the same `/var/lib/vhost-watch` state directory for all containers.

Polling and extra kicks have overhead. The 20 ms configuration is an experiment setting, not a throughput or latency guarantee. Five VMs with 60 RX/TX queue pairs each have not been qualified. `--trace-stages` adds traffic-path probes and should be reserved for diagnostics. `--mode periodic --kick-interval 0.1` sends unconditional kicks and is not the recommended default.

```sh
sudo podman logs -f vhost-watch
sudo podman stop --time 10 vhost-watch
```

## Manual recovery: run once

Write one verified kick to every selected TX slot and exit:

```sh
sudo env VHOST_WATCH_NAME=vhost-recovery ./deploy/run.sh \
  --domain-regex '^worker-' --once
```

Restrict manual recovery to one current TX slot in one VM:

```sh
sudo env VHOST_WATCH_NAME=vhost-recovery ./deploy/run.sh \
  --pid 1234 --once --vhost-fd 42
```

`--once` deliberately bypasses stall detection and the observer lock, so it can run alongside an observing agent. It reports per-target results as JSON and exits nonzero for unavailable targets or failed writes. A successful write is not proof that application traffic recovered.

## Host fault injection

Use a test VM with active guest TX traffic. Keep an independent management path available. Start with an observing agent so automatic recovery does not hide the stall. No guest fault module is used.

1. Use `--list-queues` to select a current TX `vhost_fd`.
2. Start the fault container, selecting exactly one VM and one FD:

```sh
sudo ./deploy/fault.sh --detach --pid 1234 --vhost-fd 42 \
  --delay 2s --window 500ms --drops 1 --recover-after 30s
sudo podman logs -f vhost-fault
```

The controller builds a small kernel module against the Host headers. During the bounded window, it suppresses at most `--drops` wakeups at the selected TX waiter's `vhost_poll_wakeup` entry. Other waiters are unaffected. Dropping a wakeup may leave pending descriptors without further notifications; a busy queue can also make progress and fail to stall. A `dropped` count alone is not proof of a persistent stall. Check ring progress after `disarmed`, then verify guest/application behavior.

The default drops one wakeup. For a stronger controlled injection, increase `--drops`, for example to `1000`; the window still bounds the interruption. Delay and window are each limited to 60 seconds. The kernel enforces the window and schedules probe removal independently of the controller. The controller removes the module after the window, then waits for queue progress or the recovery deadline.

3. While the queue remains stalled, trigger `--once --vhost-fd` as shown above and verify traffic resumes.
4. Stop the injector when finished:

```sh
sudo podman stop --time 10 vhost-fault
```

The controller sends a verified recovery kick on normal stop or when `--recover-after` expires. `--recover-after 0` disables the automatic deadline and waits for manual recovery or stop. The recovery deadline starts after the injection window ends. An experiment with no matching dropped wakeup exits nonzero.

After a forced kill or controller crash, the kernel window still expires, but the module may remain loaded and retain the target vhost file. Remove it explicitly, then perform manual recovery with a freshly selected PID/FD:

```sh
sudo ./deploy/fault.sh --cleanup
sudo env VHOST_WATCH_NAME=vhost-recovery ./deploy/run.sh --pid 1234 --once
```

`--cleanup` only removes the module; it does not guess a recovery target. A Host permits one fault controller at a time. Do not restart or hot-unplug the selected VM device during an injection experiment.

## Build and images

```sh
go test -race ./...
make build
sudo podman build --target agent -f deploy/Containerfile -t localhost/vhost-watch:dev .
sudo podman build --target fault -f deploy/Containerfile -t localhost/vhost-fault:dev .
```

`make build` requires Go 1.25 or later, Clang, libbpf headers, and a Linux x86-64 build environment. Override `VHOST_WATCH_IMAGE` or `VHOST_FAULT_IMAGE` to run local images. Override `VHOST_WATCH_NAME` for concurrent inspections or one-shot calls.

GitHub Actions run Go race tests and vet, compile BPF and the Host module, and build both Linux amd64 images. Pushes to `main`, version tags, and manual runs publish to GHCR using `GITHUB_TOKEN`, then pull each digest and verify its revision, CLI, and package visibility. Tags include `main`, full `sha-<commit>`, and version tags for `v*` releases. Private repository publication requires private package visibility; publication does not make either package public.
