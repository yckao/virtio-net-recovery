#!/usr/bin/env python3
"""Build every release unit outside the workspace through an isolated file proxy.

Default mode is offline: existing third-party download-cache archives are the only
fallback. --online adds the official Go proxy for third-party dependencies. Local
candidate modules always resolve from generated archives, never unpublished GitHub.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PREFIX = "github.com/yckao/virtio-net-recovery"
VERSION = "v0.1.0"
LIBRARIES = ["recovery-core", "recovery-evidence", "qemu-discovery", "vhost-linux"]
UNITS = ["."] + [f"modules/{name}" for name in LIBRARIES] + ["apps/vhost-agent", "apps/vhost-faultlab"]


def run(args, cwd, env):
    print(f"[{Path(cwd).name}] {' '.join(args)}", flush=True)
    subprocess.run(args, cwd=cwd, env=env, check=True)


def download_requirements(directory, env):
    manifest = json.loads(subprocess.check_output(["go", "mod", "edit", "-json"], cwd=directory, env=env, text=True))
    requirements = [f"{item['Path']}@{item['Version']}" for item in (manifest.get("Require") or [])]
    if requirements:
        run(["go", "mod", "download", *requirements], directory, env)


def ignored(directory, names):
    return {name for name in names if name in {".git", ".worktrees", "build", "__pycache__"} or name.endswith(".test")}


def proxy_archive(proxy: Path, name: str):
    source = ROOT / "modules" / name
    module = f"{PREFIX}/modules/{name}"
    target = proxy / module / "@v"
    target.mkdir(parents=True)
    (target / f"{VERSION}.mod").write_bytes((source / "go.mod").read_bytes())
    (target / f"{VERSION}.info").write_text(json.dumps({"Version": VERSION, "Time": "2026-10-09T00:00:00Z"}) + "\n")
    (target / "list").write_text(VERSION + "\n")
    archive = target / f"{VERSION}.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as output:
        for file in sorted(source.rglob("*")):
            relative = file.relative_to(source)
            if any(part in {".git", "build", "__pycache__"} for part in relative.parts):
                continue
            if file.is_symlink():
                raise RuntimeError(f"release archive contains symlink: {file}")
            if not file.is_file() or file.name.endswith(".test"):
                continue
            entry = zipfile.ZipInfo(f"{module}@{VERSION}/{relative.as_posix()}", (2026, 10, 9, 0, 0, 0))
            entry.external_attr = 0o644 << 16
            output.writestr(entry, file.read_bytes())
    return {"module": module, "version": VERSION, "sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}


def environment(work: Path, online: bool):
    original_cache = Path(subprocess.check_output(["go", "env", "GOMODCACHE"], cwd=ROOT, text=True).strip())
    env = os.environ.copy()
    env.update({"GOWORK": "off", "GOMODCACHE": str(work / "module-cache"),
                "GOCACHE": env.get("GOCACHE", str(work / "build-cache")),
                "GONOSUMDB": PREFIX + "/*", "GOPRIVATE": "", "GONOPROXY": ""})
    fallbacks = [(work / "proxy").as_uri(), (original_cache / "cache" / "download").as_uri()]
    fallbacks.append("https://proxy.golang.org" if online else "off")
    env["GOPROXY"] = ",".join(fallbacks)
    if not online:
        # Existing go.sum values and the local downloaded zip hashes remain checked;
        # offline operation cannot consult the public checksum service.
        env["GOSUMDB"] = "off"
    return env


def assert_no_replace(source: Path):
    manifest = json.loads(subprocess.check_output(["go", "mod", "edit", "-json"], cwd=source,
                          env={**os.environ, "GOWORK": "off"}, text=True))
    if manifest.get("Replace"):
        raise RuntimeError(f"release module must not have replacements: {source}")


def copy_unit(unit: str, work: Path):
    destination = work / "units" / ("tooling" if unit == "." else unit.replace("/", "-"))
    source = ROOT / unit
    assert_no_replace(source)
    if unit == ".":
        destination.mkdir(parents=True)
        shutil.copy2(ROOT / "go.mod", destination / "go.mod")
        shutil.copytree(ROOT / "tools", destination / "tools", ignore=ignored)
    else:
        shutil.copytree(source, destination, ignore=ignored)
    return destination


def external_consumers(work: Path, env):
    for name in LIBRARIES:
        consumer = work / "consumers" / name
        consumer.mkdir(parents=True)
        (consumer / "go.mod").write_text(f"module consumer.invalid/{name}\n\ngo 1.25.0\n\nrequire {PREFIX}/modules/{name} {VERSION}\n" +
          ("\nrequire (\n github.com/cilium/ebpf v0.22.0 // indirect\n golang.org/x/sys v0.43.0 // indirect\n)\n" if name == "vhost-linux" else ""))
        if name in {"recovery-core", "recovery-evidence"}:
            shutil.copy2(ROOT / "modules" / name / "examples" / "replay" / "main.go", consumer / "main.go")
        elif name == "qemu-discovery":
            (consumer / "main.go").write_text(f'''package main
import ("fmt"; discovery "{PREFIX}/modules/qemu-discovery")
func main() {{ _,err:=discovery.New(discovery.Options{{PIDs:[]int{{1}},MaxTargets:1}});if err!=nil{{panic(err)}};fmt.Println("standalone discovery contract constructed") }}
''')
        else:
            (consumer / "main.go").write_text(f'''package main
import ("fmt"; vhost "{PREFIX}/modules/vhost-linux")
func main() {{ o:=vhost.Observation{{Source:vhost.SourceLive,Live:true,Num:256,Avail:2,Used:1,Consumed:1,Outstanding:1,Pending:1}};if vhost.Classify(o,1)!=vhost.Eligible{{panic("classifier")}};fmt.Println("standalone backend contract exercised without privileges") }}
''')
        download_requirements(consumer, env)
        run(["go", "run", "."], consumer, env)
        run(["go", "build", "."], consumer, {**env, "GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--online", action="store_true", help="Allow the official Go proxy for uncached third-party modules")
    parser.add_argument("--keep", action="store_true", help="Retain isolated sources, proxy, caches and report")
    parser.add_argument("--prepare-only", action="store_true", help="Create candidate proxy/environment and retain it, without running builds")
    parser.add_argument("--boundaries", action="store_true", help="Also run the workspace boundary checker with the isolated dependency environment")
    args = parser.parse_args()
    work = Path(tempfile.mkdtemp(prefix="vhost-independent-")).resolve()
    success = False
    try:
        versions = [proxy_archive(work / "proxy", name) for name in LIBRARIES]
        env = environment(work, args.online)
        saved = {name: env[name] for name in ["GOWORK", "GOMODCACHE", "GOCACHE", "GONOSUMDB", "GOPRIVATE", "GONOPROXY", "GOPROXY"]}
        if "GOSUMDB" in env:
            saved["GOSUMDB"] = env["GOSUMDB"]
        (work / "environment.json").write_text(json.dumps(saved, indent=2) + "\n")
        print(f"isolated release workspace: {work}", flush=True)
        if args.prepare_only:
            success = True
            return
        for unit in UNITS:
            destination = copy_unit(unit, work)
            # Download only declared dependencies. `tidy` also fetches tests of
            # upstream libraries, which are outside this component's test scope.
            download_requirements(destination, env)
            run(["go", "test", "-mod=readonly", "-race", "./..."], destination, env)
            run(["go", "vet", "-mod=readonly", "./..."], destination, env)
            run(["go", "build", "-mod=readonly", "./..."], destination, env)
            linux = {**env, "GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0"}
            run(["go", "test", "-mod=readonly", "-exec=true", "./..."], destination, linux)
            run(["go", "vet", "-mod=readonly", "./..."], destination, linux)
        external_consumers(work, env)
        if args.boundaries:
            workspace_env = {**env, "GOWORK": str(ROOT / "go.work")}
            run(["go", "run", "./tools/check-boundaries"], ROOT, workspace_env)
        (work / "report.json").write_text(json.dumps({"status": "passed", "candidates": versions,
          "native": "tests with race detector, vet and build", "linux_amd64": "compile and vet only; no execution", "live_kernel_qualification": False}, indent=2) + "\n")
        print("independent release checks passed; Linux runtime/BPF qualification is separate", flush=True)
        success = True
    finally:
        if success and not (args.keep or args.prepare_only):
            shutil.rmtree(work)
        else:
            print(f"retained artifacts: {work}", flush=True)

if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"independent release check failed: {exc}", file=sys.stderr)
        sys.exit(1)
