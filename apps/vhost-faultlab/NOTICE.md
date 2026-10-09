# Provenance and license notices

`internal/module/assets/vhost_fault.c` was carried forward from
`fault-injection/host/vhost_fault.c` at commit
`3146e10245e2b92bc022c134648c6fc43d858fa5` in
`yckao/virtio-net-recovery`. Its `SPDX-License-Identifier: GPL-2.0-only` notice and
`MODULE_LICENSE("GPL")` declaration are retained without changing the C source.

The accompanying Makefile derives from that commit's
`fault-injection/host/Makefile`, with literal path quoting added around the kernel
build and module directories. No compiled kernel object is distributed here.

The new Go controller follows the repository's licensing policy. Independent
publication must include all required repository and GPL notices and record the
backend/kernel build compatibility; this notice does not relicense any source.
