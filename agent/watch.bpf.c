// SPDX-License-Identifier: GPL-2.0
#include <linux/bpf.h>
#include <linux/ptrace.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "wire.h"

/* Minimal CO-RE declarations. Offsets come from the running kernel/module BTF. */
#define CORE __attribute__((preserve_access_index))
struct wait_queue_head { unsigned int unused; } CORE;
struct wait_queue_entry { unsigned int unused; } CORE;
struct vhost_work { unsigned long flags; } CORE;
struct vhost_poll {
    struct wait_queue_head *wqh;
    struct wait_queue_entry wait;
    struct vhost_work work;
} CORE;
struct eventfd_ctx { struct wait_queue_head wqh; int id; } CORE;
struct file { void *private_data; } CORE;
struct vhost_virtqueue {
    unsigned int num;
    void *avail, *used;
    struct file *kick;
    struct vhost_poll poll;
    unsigned short last_avail_idx, last_used_idx;
    void *private_data;
    unsigned long long acked_features;
    _Bool is_le;
} CORE;
struct vhost_dev { struct vhost_virtqueue **vqs; } CORE;
struct vhost_net { struct vhost_dev dev; } CORE;

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY); __uint(max_entries, 1);
    __type(key, __u32); __type(value, __u32);
} owner SEC(".maps");
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY); __uint(max_entries, 1);
    __type(key, __u32); __type(value, struct snapshot);
} sample SEC(".maps");
#define HASH(name, value_type) \
struct { __uint(type, BPF_MAP_TYPE_HASH); __uint(max_entries, 4096); \
    __type(key, __u64); __type(value, value_type); } name SEC(".maps")
HASH(stats, struct counters);
HASH(contexts, __u64);
HASH(works, __u64);
HASH(waits, __u64);
HASH(inflight, __u64);

SEC("kprobe/vhost_net_ioctl")
int BPF_KPROBE(snapshot_tx, struct file *file, unsigned int command)
{
    __u32 zero = 0, *pid = bpf_map_lookup_elem(&owner, &zero);
    if (!pid || bpf_get_current_pid_tgid() >> 32 != *pid || command != 0x8008af00)
        return 0; /* VHOST_GET_FEATURES is read-only and does not require ownership. */
    struct vhost_net *net = BPF_CORE_READ(file, private_data);
    struct vhost_virtqueue **vqs = BPF_CORE_READ(net, dev.vqs), *vq = 0;
    bpf_probe_read_kernel(&vq, sizeof(vq), vqs + 1); /* vhost-net slot 1 is TX. */
    if (!vq) return 0;
    struct snapshot s = {};
    s.timestamp_ns = bpf_ktime_get_ns();
    s.vq = (__u64)vq;
    s.num = BPF_CORE_READ(vq, num);
    s.avail = (__u64)BPF_CORE_READ(vq, avail);
    s.used = (__u64)BPF_CORE_READ(vq, used);
    struct file *kick = BPF_CORE_READ(vq, kick);
    s.kick_file = (__u64)kick;
    struct eventfd_ctx *efd = BPF_CORE_READ(kick, private_data);
    s.ctx = (__u64)efd;
    s.eventfd_id = BPF_CORE_READ(efd, id);
    s.backend = (__u64)BPF_CORE_READ(vq, private_data);
    s.features = BPF_CORE_READ(vq, acked_features);
    s.last_avail = BPF_CORE_READ(vq, last_avail_idx);
    s.last_used = BPF_CORE_READ(vq, last_used_idx);
    s.little_endian = BPF_CORE_READ(vq, is_le);
    s.work_flags = BPF_CORE_READ(vq, poll.work.flags);
    s.work = (__u64)__builtin_preserve_access_index(&vq->poll.work);
    s.wait = (__u64)__builtin_preserve_access_index(&vq->poll.wait);
    s.poll_wqh = (__u64)BPF_CORE_READ(vq, poll.wqh);
    s.ctx_wqh = (__u64)__builtin_preserve_access_index(&efd->wqh);
    bpf_map_update_elem(&sample, &zero, &s, BPF_ANY);
    if (s.ctx && s.backend && s.poll_wqh == s.ctx_wqh) {
        struct counters initial = {};
        bpf_map_update_elem(&stats, &s.vq, &initial, BPF_NOEXIST);
        bpf_map_update_elem(&contexts, &s.ctx, &s.vq, BPF_ANY);
        bpf_map_update_elem(&works, &s.work, &s.vq, BPF_ANY);
        bpf_map_update_elem(&waits, &s.wait, &s.vq, BPF_ANY);
    }
    return 0;
}

static __always_inline struct counters *by_context(__u64 ctx)
{
    __u64 *vq = bpf_map_lookup_elem(&contexts, &ctx);
    return vq ? bpf_map_lookup_elem(&stats, vq) : 0;
}

SEC("kprobe/eventfd_signal_mask")
int BPF_KPROBE(signal_event, struct eventfd_ctx *efd)
{
    struct counters *s = by_context((__u64)efd);
    if (s) { __sync_fetch_and_add(&s->signals, 1); s->last_signal_ns = bpf_ktime_get_ns(); }
    return 0;
}

SEC("kprobe/eventfd_write")
int BPF_KPROBE(write_event, struct file *file)
{
    struct counters *s = by_context((__u64)BPF_CORE_READ(file, private_data));
    if (s) { __sync_fetch_and_add(&s->writes, 1); s->last_write_ns = bpf_ktime_get_ns(); }
    return 0;
}

SEC("kprobe/vhost_poll_wakeup")
int BPF_KPROBE(wake_event, void *wait, unsigned int mode, int sync, void *key)
{
    if (!((__u64)key & 1)) return 0;
    __u64 addr = (__u64)wait, *vq = bpf_map_lookup_elem(&waits, &addr);
    struct counters *s = vq ? bpf_map_lookup_elem(&stats, vq) : 0;
    if (s) { __sync_fetch_and_add(&s->wakeups, 1); s->last_wakeup_ns = bpf_ktime_get_ns(); }
    return 0;
}

SEC("kprobe/handle_tx_kick")
int BPF_KPROBE(handler_enter, void *work)
{
    __u64 addr = (__u64)work, tid = bpf_get_current_pid_tgid();
    __u64 *vq = bpf_map_lookup_elem(&works, &addr);
    struct counters *s = vq ? bpf_map_lookup_elem(&stats, vq) : 0;
    if (s) {
        bpf_map_update_elem(&inflight, &tid, vq, BPF_ANY);
        __sync_fetch_and_add(&s->active, 1);
        __sync_fetch_and_add(&s->handlers, 1);
        s->last_handler_ns = bpf_ktime_get_ns();
    }
    return 0;
}

SEC("kretprobe/handle_tx_kick")
int handler_exit(struct pt_regs *ctx)
{
    __u64 tid = bpf_get_current_pid_tgid(), *vq = bpf_map_lookup_elem(&inflight, &tid);
    struct counters *s = vq ? bpf_map_lookup_elem(&stats, vq) : 0;
    if (s) {
        __sync_fetch_and_sub(&s->active, 1);
        s->last_handler_ns = bpf_ktime_get_ns();
    }
    bpf_map_delete_elem(&inflight, &tid);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
