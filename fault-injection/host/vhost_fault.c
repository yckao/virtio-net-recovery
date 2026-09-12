// SPDX-License-Identifier: GPL-2.0-only
// A bounded, single-waiter lost-wakeup injection for x86-64 vhost-net.
#include <linux/module.h>
#include <linux/kprobes.h>
#include <linux/file.h>
#include <linux/fs.h>
#include <linux/poll.h>
#include <linux/workqueue.h>
#include <linux/ktime.h>
#include <linux/uaccess.h>

#ifndef CONFIG_X86_64
#error "This injector supports x86-64 only"
#endif

static int target_fd = -1;
static unsigned long target_wait;
static unsigned int delay_ms = 1000, window_ms = 500, max_drops = 1;
module_param(target_fd, int, 0000);
module_param(target_wait, ulong, 0000);
module_param(delay_ms, uint, 0400);
module_param(window_ms, uint, 0400);
module_param(max_drops, uint, 0400);

static struct file *pinned_file;
static u64 starts_ns, ends_ns;
static atomic64_t matched = ATOMIC64_INIT(0), dropped = ATOMIC64_INIT(0);
static bool active;
module_param(active, bool, 0400);

static int counter_get(char *buf, const struct kernel_param *kp)
{
	return scnprintf(buf, PAGE_SIZE, "%lld\n", atomic64_read(kp->arg));
}
static const struct kernel_param_ops counter_ops = { .get = counter_get };
module_param_cb(matched, &counter_ops, &matched, 0400);
module_param_cb(dropped, &counter_ops, &dropped, 0400);

static int gate(struct kprobe *p, struct pt_regs *regs)
{
	u64 now;
	s64 count;

	if (regs->di != target_wait || !(regs->cx & EPOLLIN))
		return 0;
	now = ktime_get_ns();
	if (now < starts_ns || now >= ends_ns)
		return 0;
	atomic64_inc(&matched);
	count = atomic64_read(&dropped);
	while (count < max_drops) {
		s64 old = atomic64_cmpxchg(&dropped, count, count + 1);
		if (old == count) {
			// vhost_poll_wakeup's fourth argument is the poll key. A zero
			// key takes its normal mask-mismatch return without queueing work.
			regs->cx = 0;
			break;
		}
		count = old;
	}
	return 0;
}

// Keep the probe unoptimized so argument modification has one predictable path.
static void gate_post(struct kprobe *p, struct pt_regs *regs, unsigned long flags) {}
static struct kprobe probe = {
	.pre_handler = gate, .post_handler = gate_post,
};

static int resolve_gate_address(void)
{
	struct kprobe lookup = { .symbol_name = "vhost_poll_wakeup" };
	u8 bytes[9];
	unsigned int offset = 0;
	int err = register_kprobe(&lookup);

	if (err)
		return err;
	probe.addr = lookup.addr;
	err = copy_from_kernel_nofault(bytes, probe.addr, sizeof(bytes));
	unregister_kprobe(&lookup);
	if (err)
		return err;
	if (!memcmp(bytes, "\xf3\x0f\x1e\xfa", 4))
		offset = 4; /* ENDBR64 */
	// Never aggregate our post-handler with entry probes using ftrace. Older
	// kernels can corrupt that aggregate when post-handler flags change. Skip
	// the verified five-byte fentry call/NOP, before any argument is consumed.
	if (bytes[offset] != 0xe8 &&
	    memcmp(bytes + offset, "\x0f\x1f\x44\x00\x00", 5))
		return -EOPNOTSUPP;
	probe.addr += offset + 5;
	return 0;
}

static void disarm(struct work_struct *work)
{
	unregister_kprobe(&probe);
	WRITE_ONCE(active, false);
}
static DECLARE_DELAYED_WORK(disarm_work, disarm);

static int __init fault_init(void)
{
	struct fd fd;
	int err;

	if (!target_wait || target_fd < 0 || delay_ms > 60000 ||
	    !window_ms || window_ms > 60000 || !max_drops || max_drops > 1000000)
		return -EINVAL;
	fd = fdget(target_fd);
	if (!fd.file)
		return -EBADF;
	if (!S_ISCHR(file_inode(fd.file)->i_mode) || !fd.file->f_op->owner ||
	    strcmp(module_name(fd.file->f_op->owner), "vhost_net")) {
		fdput(fd);
		return -EINVAL;
	}
	// Retain the vhost allocation even if the controller is killed. This
	// prevents its selected waiter address from being reused during injection.
	pinned_file = get_file(fd.file);
	fdput(fd);
	err = resolve_gate_address();
	if (err) {
		fput(pinned_file);
		return err;
	}
	starts_ns = ktime_get_ns() + (u64)delay_ms * NSEC_PER_MSEC;
	ends_ns = starts_ns + (u64)window_ms * NSEC_PER_MSEC;
	err = register_kprobe(&probe);
	if (err) {
		fput(pinned_file);
		return err;
	}
	WRITE_ONCE(active, true);
	schedule_delayed_work(&disarm_work, msecs_to_jiffies(delay_ms + window_ms));
	return 0;
}

static void __exit fault_exit(void)
{
	cancel_delayed_work_sync(&disarm_work);
	if (READ_ONCE(active))
		unregister_kprobe(&probe);
	fput(pinned_file);
}
module_init(fault_init);
module_exit(fault_exit);
MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Bounded single-queue vhost-net lost-wakeup fault injection");
