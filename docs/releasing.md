# Distribution

Build both executables from the single root Go module. Public packages are reusable through their own APIs; they are not separately versioned modules today. No unpublished dependency versions or candidate proxy are required.

Before publishing a release:

1. Run root tests, vet, boundary tests and target-platform validation. Record what executed on Linux versus what was only cross-compiled.
2. Select a project-wide distribution license and review retained BPF/kernel GPL notices and provenance. The notices do not grant a license for all Go source.
3. Build backend Go code and matching BPF objects together. Record the supported kernel/architecture and wire schema. Record the build kernel for faultlab's separate kernel asset.
4. Document CLI/output changes and operational qualification. Accepted writes, queue progress and service recovery are distinct evidence.

If a public package gains a real independent release lifecycle, extract it with its tests, examples and assets and assign a canonical module path. Review imports and licensing at that point. This repository does not promise a path-transparent split.
