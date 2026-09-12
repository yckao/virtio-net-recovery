#ifndef VHOST_WATCH_WIRE_H
#define VHOST_WATCH_WIRE_H

struct snapshot {
    unsigned long long timestamp_ns, vq, kick_file, ctx, avail, used;
    unsigned long long backend, features, work_flags, work, wait, poll_wqh, ctx_wqh;
    unsigned int num, eventfd_id;
    unsigned short last_avail, last_used;
    unsigned char little_endian;
    unsigned char reserved[3];
};

struct counters {
    unsigned long long signals, writes, wakeups, handlers;
    unsigned long long last_signal_ns, last_write_ns, last_wakeup_ns, last_handler_ns;
    unsigned long long active;
};

#endif
