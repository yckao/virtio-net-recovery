# Provenance and licensing

The private BPF snapshot/trace algorithms derive from the reviewed vhost-watch
source at repository revision 3146e10245e2b92bc022c134648c6fc43d858fa5. The new module
changes resource ownership, API boundaries, wire schema and registration lifetimes.
The BPF source retains its GPL-2.0 SPDX notice. No relicensing is asserted here.

The repository's publication owner must establish an appropriate license for the
Go module and verify compatibility/notices before independent distribution. This
implementation does not invent a license grant for source that lacked one.

Dependencies remain separately licensed: github.com/cilium/ebpf v0.22.0 and
golang.org/x/sys v0.43.0. Consult their module licenses when packaging releases.
