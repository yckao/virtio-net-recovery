# virtio-net-recovery

A Go Host agent for virtio-net / vhost-net TX queues that may stop making progress. It observes queue progress and can write a verified kick to the queue's eventfd to wake vhost. This is a recovery workaround; it does not identify or fix the underlying cause.

The agent runs in a container on the QEMU Host. It supports multiple QEMU PIDs, libvirt domain name regular expressions, automatic recovery, and manual one-shot recovery. A separate container injects a bounded lost wakeup entirely on the Host. Neither requires software installation in the guest.

## Requirements

- Linux x86-64, QEMU with vhost-net, little-endian split virtqueues, and kernel BTF (`/sys/kernel/btf`). Packed rings and IOMMU-translated rings are unsupported.
- Rootful Podman, Host PID namespace, and permission to use BPF, `pidfd_getfd`, and `process_vm_readv`. The examples use privileged containers.
- For domain regex selection, mount the libvirt runtime XML directory (normally `/run/libvirt/qemu`). This is read-only discovery and requires no libvirt socket access.
- For fault injection, matching Host kernel headers under `/lib/modules` and `/usr/src`, loadable kernel modules, and a matching compiler. The fault image includes GCC 12; supported Host kernels include Ubuntu `6.8.0-52-generic` and `6.17.0-20-generic`. Other kernels require validation of their vhost internals. Kernel lockdown or module-signing policy may prevent injection.

Public repository publication requires existing public GHCR packages; CI never changes package visibility. Private installations require authentication. Log in using a GitHub token with `read:packages` access as the password when prompted:

```sh
sudo podman login ghcr.io -u YOUR_GITHUB_USER
sudo podman pull ghcr.io/yckao/virtio-net-recovery:main
sudo podman pull ghcr.io/yckao/virtio-net-recovery-fault:main
```

## Select and inspect VMs

Run these one-line commands directly on the QEMU Host; no launcher scripts or repository checkout are needed. Create the shared state directory once with `sudo mkdir -p /var/lib/vhost-watch`. `--pid` accepts repeated flags or a comma-separated list. Explicit PIDs and regex matches form a deduplicated union. Regex uses Go syntax; anchor it when selecting exact names.

```sh
sudo podman run --rm --name vhost-list --privileged --pid=host --network=none --read-only --security-opt label=disable -v /run/libvirt/qemu:/run/libvirt/qemu:ro ghcr.io/yckao/virtio-net-recovery:main --domain-regex '^worker-' --list

sudo podman run --rm --name vhost-queues --privileged --pid=host --network=none --read-only --security-opt label=disable -v /sys/kernel/btf:/sys/kernel/btf:ro -v /var/lib/vhost-watch:/state:rw ghcr.io/yckao/virtio-net-recovery:main --pid 1234 --list-queues
```

`--list-queues` reports the current QEMU `vhost_fd` for each configured TX slot. A vhost FD is process-local and can change after restart or device reconfiguration; it is not a guest queue number. Select it from a fresh listing.

## Four modes

| Mode | Behavior |
| --- | --- |
| `observe` (default) | Poll queues and report candidates; never write a recovery kick. |
| `recover` | Use the same polling and candidate checks, validate live process/attachment/eventfd identity, write a kick when the checks pass, then observe backend consumption and used-ring progress. |
| `kick` | Manually write one verified kick to each selected TX slot and exit. This intentionally bypasses candidate detection. |
| `trace` | Diagnose the initially selected targets with additional eventfd/vhost stage probes for an explicit bounded duration. It never automatically kicks. |

Build the current source before using this interface; an already published image may contain an older CLI:

```sh
sudo podman build --target agent -f deploy/Containerfile -t localhost/vhost-watch:dev .
sudo mkdir -p /var/lib/vhost-watch
```

### Observe and recover

Observe selected PIDs with single-line JSON stdout collected by Podman journald:

```sh
sudo podman run --detach --rm --name vhost-watch --privileged --pid=host --network=none --read-only --security-opt label=disable -v /sys/kernel/btf:/sys/kernel/btf:ro -v /var/lib/vhost-watch:/state:rw --log-driver=journald localhost/vhost-watch:dev --pid 1234,5678 --mode observe
```

Enable recovery for selected running domains:

```sh
sudo podman run --detach --rm --name vhost-watch --privileged --pid=host --network=none --read-only --security-opt label=disable -v /sys/kernel/btf:/sys/kernel/btf:ro -v /var/lib/vhost-watch:/state:rw -v /run/libvirt/qemu:/run/libvirt/qemu:ro --log-driver=journald localhost/vhost-watch:dev --domain-regex '^worker-' --mode recover
```

Use one observing or recovering agent per selected process and share the state directory between containers. Domain selection refreshes every five seconds (`--target-interval 5s`) and can follow domain restarts. Explicit PIDs never follow a reused PID. Stop an existing observer before switching its selected processes to recovery.

Both modes batch known user-ring reads every `--interval` (default **0.1 seconds**) and refresh the full QEMU FD inventory every `--inventory-interval` (default **5 seconds**). New or changed attachments can wait for the next inventory refresh. These defaults are candidate operating settings, not a guarantee against false kicks or a qualification of production overhead. A 25 ms cadence or a second live confirmation needs separate evidence before changing the policy.

A candidate needs outstanding descriptors across at least two observations without completion progress, followed by a live snapshot showing unconsumed descriptors. The queued bit being zero does **not** prove the vhost worker is idle: Linux clears that bit before calling the work handler. The live checks refuse queued work; absence of queued work still does not establish worker idleness or the underlying cause of a stall.

Every recovery attempt refreshes FD inventory and validates the QEMU process, pinned vhost attachment, waiter/backend and nonblocking eventfd. Attempts and failures are paced from the end of validation, so actual retry gaps can exceed the requested interval. Recover retries while a validated candidate persists. A successful eventfd write, backend descriptor consumption, used-ring progress, and a verification timeout are distinct observations. Progress after a kick does **not** prove a lost notification, packet delivery, or application recovery. `--verify-timeout` (default 5 seconds) reports a write without observed progress; it does not end recovery retries.

Healthy summaries use cached user indices, not fresh kernel consumption state. Ring indices are 16-bit: a full wrap between samples can alias unchanged indices, so equal values do not prove no work occurred. `--summary-interval` controls summary cadence. The required low-frequency `vhost_net_ioctl` snapshot probe is shared across targets. Observe and recover do not load or attach the traffic-dependent diagnostic stage programs.

```sh
sudo podman logs -f vhost-watch
sudo podman stop --time 10 vhost-watch
```

Stopping detaches probes and releases process locks. Repeat the start command to restart a container created with `--rm`. Journald retention is controlled by the Host's journal configuration.

### Candidate journal

Each queue keeps the latest **16 existing poll samples** in a fixed-size memory ring. A candidate opens one episode per queue and emits these pre/current snapshots with an `event_id`; later snapshot, write and closing records use the same ID. IDs combine a random process prefix with a process-wide sequence. The journal adds no extra sampling or disk files. Podman sends each single-line JSON stdout record to journald.

An episode has a **30-second diagnostic deadline**, enforced at the next completed cycle, including unavailable-poll checks; scheduling or live validation can delay its wall-clock close. It also closes after a candidate has been absent for **1 second**, or when its identity changes, its queue becomes unavailable, its indices become inconsistent, or the agent stops. Routine reopening waits **1 second** after closing. A paced recovery attempt starting a new verification may open an episode immediately so its first write has an active origin ID; observe never uses this exception. Subsequent snapshots and write totals share routine aggregation of one record per second, with immediate candidate, first-write and closing records. These journal limits do not delay recovery attempts. Closing reports distinguish Host descriptor consumption (`last_avail`), used-ring completion progress, both, quiet without observed progress, timeout, identity change, unavailability, invalid ring and stop. Write success, write error and validation refusal have separate totals and fixed reasons.

Snapshots identify cached user indices versus a live snapshot. Host descriptor consumption and queued-work values are meaningful only when their freshness flags are set; consumption does not prove backend transmission or receiver delivery. `after_write` means only that the latest sample was taken after the first accepted write; progress can predate that write and does not prove causation or a lost notification. The **30-second episode timeout** bounds diagnostics; the separate `--verify-timeout` bounds the wait before reporting `recovery_unconfirmed`. Recovery verification retains the first write's baseline and originating `event_id` across episode closure or reopening. `progress_after_kick` and `recovery_unconfirmed` refer to that origin, and summaries retain the pending verification ID and timeout state. Neither timeout stops recovery retries.

### Metrics

Metrics are disabled by default. To expose `/metrics` on the Host's loopback interface, explicitly use Host networking and provide `--metrics-address`:

```sh
sudo podman run --detach --rm --name vhost-watch-metrics --privileged --pid=host --network=host --read-only --security-opt label=disable -v /sys/kernel/btf:/sys/kernel/btf:ro -v /var/lib/vhost-watch:/state:rw --log-driver=journald localhost/vhost-watch:dev --pid 1234,5678 --mode observe --metrics-address 127.0.0.1:9475
```

The exporter binds before target discovery or BPF setup; a bind failure aborts startup. All selected VMs share totals, while current queue coverage, open episodes and polling gaps aggregate only active workers. Labels use fixed reason, write result and progress outcome values; identity, PID, event ID and raw indices are excluded. A successful write and later queue progress remain separate observations.

`candidates_total` counts diagnostic episode openings. `decisions_total` records one final fixed live/pre-write verdict per live confirmation call; syscall write failures and cancellation retain the last live verdict, while unavailable or changed attachments report their respective reason. `writes_total` counts every accepted kick, attempted syscall failure, or safety/cancellation refusal independently of journal rate limits. A stdout failure is not a kick failure or refusal. Manual kick contributes write results only; trace contributes polling health and queue coverage without creating candidates or writing kicks.

`progress_total` counts **only closed diagnostic episodes** with `consumption`, `used`, `both`, `timeout`, or `identity_change` outcomes, at most once per episode. Quiet, unavailable, invalid-ring and stopped closures have no progress counter. Verification timeout warnings and later `progress_after_kick` reports use their own first-write baseline and do not increment this counter again. Episode progress can predate a write and does not establish causation.

`poll_errors_total` counts cycles with at least one polling/observation error, including partial coverage failures, but not stdout failures, kick syscall failures or normal cancellation. Queue gauges describe the latest completed cycle. `poll_gap_seconds` reports the largest completed cycle gap or age since the last completed poll; `poll_max_gap_seconds` also includes current age. Never-polled workers do not contribute an age, and retired workers immediately stop contributing gauges.

### Manual kick

Write one verified kick to every selected TX slot and exit:

```sh
sudo podman run --rm --name vhost-kick --privileged --pid=host --network=none --read-only --security-opt label=disable -v /sys/kernel/btf:/sys/kernel/btf:ro -v /var/lib/vhost-watch:/state:rw localhost/vhost-watch:dev --pid 1234 --mode kick
```

Restrict the write to one current TX slot using a fresh `--list-queues` result:

```sh
sudo podman run --rm --name vhost-kick --privileged --pid=host --network=none --read-only --security-opt label=disable -v /sys/kernel/btf:/sys/kernel/btf:ro -v /var/lib/vhost-watch:/state:rw localhost/vhost-watch:dev --pid 1234 --mode kick --vhost-fd 42
```

Kick retains live identity and eventfd validation, bypasses the observer lock, and can run alongside an observing agent. It reports per-target JSON results and exits nonzero for unavailable targets or failed writes. A successful write alone is not recovery confirmation.

### Bounded diagnostic trace

Trace requires an explicit `--duration` between **1 and 300 seconds**, uses the targets selected at startup, and caps shared JSON stdout at **8 MiB**, stopping before a partial line would be emitted:

```sh
sudo podman run --rm --name vhost-trace --privileged --pid=host --network=none --read-only --security-opt label=disable -v /sys/kernel/btf:/sys/kernel/btf:ro -v /var/lib/vhost-watch:/state:rw --log-driver=journald localhost/vhost-watch:dev --pid 1234 --mode trace --duration 30
```

Only trace loads and attaches stage probes for `eventfd_signal_mask`, `eventfd_write`, `vhost_poll_wakeup`, and `handle_tx_kick` entry/return. The diagnostics are aggregate stage counters, not a full guest/KVM event timeline. Missed kretprobe returns can affect the active-handler counter; it is not exact proof of worker idleness. Trace adds traffic-dependent overhead and cannot diagnose lost notification conclusively.

Agent support is limited to Linux amd64, little-endian split rings, matching vhost BTF, and available probe symbols. Fault-module build compatibility with a kernel does not qualify trace on that kernel. Kernel changes, lockdown, BPF/kprobe restrictions or missing module BTF can prevent loading or attaching probes. The agent reports these failures and cleans up its links; it does not load modules or change security settings. Kernel integration and traffic overhead need separate controlled validation from hosted CI.

### Migration from the older CLI

- `--once` and `--rescue` remain aliases for `--mode kick`. An explicitly supplied different mode is rejected.
- `--trace-stages` remains an alias for `--mode trace`, requiring the same explicit duration. An explicitly supplied different mode is rejected. Combining kick and trace aliases is rejected.
- `--mode guarded` and `--mode periodic` are rejected with migration guidance. Use `recover` for candidate-based recovery or `kick` for an intentional manual write.
- `--threshold`, `--cooldown`, `--max-recoveries`, `--kick-interval`, and `--batch-rings` are rejected whenever explicitly supplied, including their old default values. Observe/recover batch known queue reads automatically; `--interval` controls candidate observation and retry pacing.

Sub-500 ms application recovery, below-one-percent throughput regression, physical dual-25 Gbps ECMP behavior, and uninterrupted BFD/BGP sessions remain qualification targets. Large simultaneous faults and a Host-wide attempt cap have not been qualified. Continuous suppression of every wakeup, including recovery kicks, cannot be bypassed by this mechanism. Measure healthy traffic and application/session behavior separately before choosing a cadence.

## Host fault injection

Use a test VM with active guest TX traffic. Keep an independent management path available. Start with an observing agent so automatic recovery does not hide the stall. No guest fault module is used.

1. Use `--list-queues` to select a current TX `vhost_fd`.
2. Start the fault container, selecting exactly one VM and one FD:

```sh
sudo podman run --detach --rm --name vhost-fault --privileged --pid=host --network=none --read-only --security-opt label=disable --tmpfs /tmp:rw,size=256m -v /lib/modules:/lib/modules:ro -v /usr/src:/usr/src:ro -v /sys/kernel/btf:/sys/kernel/btf:ro -v /var/lib/vhost-watch:/state:rw ghcr.io/yckao/virtio-net-recovery-fault:main --pid 1234 --vhost-fd 42 --delay 2s --window 500ms --drops 1 --recover-after 30s
sudo podman logs -f vhost-fault
```

The controller builds a small kernel module against the Host headers. During the bounded window, it suppresses at most `--drops` wakeups at the selected TX waiter's `vhost_poll_wakeup` entry. Other waiters are unaffected. Dropping a wakeup may leave pending descriptors without further notifications; a busy queue can also make progress and fail to stall. A `dropped` count alone is not proof of a persistent stall. Check ring progress after `disarmed`, then verify guest/application behavior.

The default drops one wakeup. For a stronger controlled injection, increase `--drops`, for example to `1000`; the window still bounds the interruption. Delay and window are each limited to 60 seconds. The kernel enforces the window and schedules probe removal independently of the controller. The controller removes the module after the window, then waits for queue progress or the recovery deadline.

3. While the queue remains stalled, trigger `--mode kick --vhost-fd` as shown above and verify traffic resumes.
4. Stop the injector when finished:

```sh
sudo podman stop --time 10 vhost-fault
```

The controller sends a verified recovery kick on normal stop or when `--recover-after` expires. `--recover-after 0` disables the automatic deadline and waits for manual recovery or stop. The recovery deadline starts after the injection window ends. An experiment with no matching dropped wakeup exits nonzero.

After a forced kill or controller crash, the kernel window still expires, but the module may remain loaded and retain the target vhost file. Remove it explicitly, then perform manual recovery with a freshly selected PID/FD:

```sh
sudo podman run --rm --name vhost-fault-cleanup --privileged --pid=host --network=none --read-only --security-opt label=disable -v /var/lib/vhost-watch:/state:rw ghcr.io/yckao/virtio-net-recovery-fault:main --cleanup
sudo podman run --rm --name vhost-recovery --privileged --pid=host --network=none --read-only --security-opt label=disable -v /sys/kernel/btf:/sys/kernel/btf:ro -v /var/lib/vhost-watch:/state:rw ghcr.io/yckao/virtio-net-recovery:main --pid 1234 --once
```

`--cleanup` only removes the module; it does not guess a recovery target. A Host permits one fault controller at a time. Do not restart or hot-unplug the selected VM device during an injection experiment.

## Build and images

```sh
go test -race ./...
make build
sudo podman build --target agent -f deploy/Containerfile -t localhost/vhost-watch:dev .
sudo podman build --target fault -f deploy/Containerfile -t localhost/vhost-fault:dev .
```

`make build` requires Go 1.25 or later, Clang, libbpf headers, and a Linux x86-64 build environment. To use local images, replace the GHCR image reference in a command with the corresponding `localhost/...:dev` tag. Choose a different `--name` when running concurrent inspections or one-shot calls.

GitHub Actions run Go race tests and vet, compile BPF and the Host module, and build both Linux amd64 images. Pushes to `main`, version tags, and manual runs on `main` publish to GHCR using `GITHUB_TOKEN` after validating the local image and current package visibility, then pull each digest and verify its revision, CLI, and current visibility again. Tags include `main`, full `sha-<commit>`, and version tags for `v*` releases. Public repositories require existing public packages; private repositories require private packages. A missing public package stops before publication because [GHCR defaults first publication to private](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#pushing-container-images). Initial public publication and any visibility change require separate owner approval; this workflow does not bootstrap public packages or change sharing.
