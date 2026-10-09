# Independent release policy

The six modules are candidates for separate releases. Workspace composition does not prove release independence. The isolated release gate copies each module outside the workspace, publishes candidate dependency zips into a temporary file proxy, and builds with `GOWORK=off` and no `replace`. External consumers compile against public packages. Candidate `v0.1.0` versions used by this gate are not claims that these versions exist upstream.

Before publication:

1. Run all module contract tests, boundary gates and Linux-specific validation appropriate to the module.
2. Review each module's public API, NOTICE and source assets. The original repository does not declare a project-wide license; a license decision is required before calling the whole distribution open source. Retained BPF/kernel GPL notices are not a license grant for all Go code.
3. Publish library module tags first (`modules/recovery-core/vX.Y.Z`, `modules/vhost-linux/vX.Y.Z`, `modules/qemu-discovery/vX.Y.Z`, `modules/recovery-evidence/vX.Y.Z`). Resolve exact dependency versions and checksums before publishing application tags.
4. Package matching backend BPF objects with their wire schema. Do not substitute an old object's filename. Faultlab packages its own experiment kernel asset and records the kernel build used.
5. Record kernel/architecture support, CLI/output schema changes and actual qualification evidence. Separate accepted eventfd writes from queue progress and service behavior.

A future repository split changes canonical module paths unless a separately designed stable import domain is adopted. No path-transparent split or compatibility promise is made here. Release candidates may make deliberate breaking changes; record them before assigning a stable version.
