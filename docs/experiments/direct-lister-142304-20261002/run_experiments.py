#!/usr/bin/env python3
"""Run the preregistered component experiment and retain every command receipt."""
import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
ROOT = Path.cwd()
OUT = ROOT / "evidence"
OUT.mkdir(exist_ok=True)
PREREG = HERE / "PREREG.json"
PLAN = json.loads(PREREG.read_text())
START = time.monotonic()
manifest = {
    "source_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "source_base": PLAN["source_base"],
    "prereg_sha256": hashlib.sha256(PREREG.read_bytes()).hexdigest(),
    "go_version": subprocess.check_output(["go", "version"], text=True).strip(),
    "platform": {"platform": platform.platform(), "processor": platform.processor(), "cpu_count": os.cpu_count(), "cpuinfo": Path("/proc/cpuinfo").read_text().split("\n\n")[0]},
    "run_id": os.environ.get("GITHUB_RUN_ID"),
    "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
    "commands": [],
    "source_hashes": {},
    "status": "running"
}

def save():
    manifest["elapsed_seconds"] = time.monotonic() - START
    manifest["named_command_seconds"] = sum(c["elapsed_seconds"] for c in manifest["commands"])
    manifest["residual_seconds"] = manifest["elapsed_seconds"] - manifest["named_command_seconds"]
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

def run(label, argv, filename, timeout=120):
    print("RUN", label, flush=True)
    before = time.monotonic()
    receipt = {"label": label, "argv": [str(x) for x in argv], "start_offset_seconds": before - START, "stdout_file": filename}
    manifest["commands"].append(receipt)
    code = -1
    try:
        with (OUT / filename).open("w") as handle:
            result = subprocess.run(receipt["argv"], stdout=handle, stderr=subprocess.STDOUT, timeout=timeout, check=False)
        code = result.returncode
    finally:
        receipt.update(returncode=code, elapsed_seconds=time.monotonic() - before)
        save()
    if code:
        print((OUT / filename).read_text(), flush=True)
        raise RuntimeError(f"{label} failed with status {code}")

try:
    source_files = sorted((ROOT / "pkg/kubeapiserver/direct").glob("*.go")) + sorted(HERE.glob("*.py")) + [PREREG]
    manifest["source_hashes"] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in source_files}
    run("verifier calibration", [sys.executable, HERE / "verify_artifacts.py", "--self-test"], "calibration.json")
    run("format characterization", ["gofmt", "-d", *sorted((ROOT / "pkg/kubeapiserver/direct").glob("*contract_test.go")), ROOT / "pkg/kubeapiserver/direct/context_route_test.go"], "format.diff")
    run("build pinned test binary", ["go", "test", "-p=2", "-c", "-o", OUT / "direct.test", "./pkg/kubeapiserver/direct"], "build.log", timeout=900)
    manifest["binary_sha256"] = hashlib.sha256((OUT / "direct.test").read_bytes()).hexdigest()
    save()
    common = ["go", "tool", "test2json", "-t", "-p", "k8s.io/kubernetes/pkg/kubeapiserver/direct", OUT / "direct.test", "-test.v=test2json", "-test.count=1", "-test.timeout=60s"]
    names = PLAN["execution"]["required_tests"]
    run("correctness with controls", common + ["-test.run=^(" + "|".join(names) + ")$"], "correctness.jsonl")
    run("fresh-process replication", common + ["-test.run=^TestExperiment"], "replication.jsonl")
    for pair in range(PLAN["execution"]["benchmark_pairs"]):
        arms = ["cached", "direct_synthetic"] if pair % 2 == 0 else ["direct_synthetic", "cached"]
        for arm in arms:
            regex = "^BenchmarkDirectListerReadCost$/op=.*/n=.*/managed_fields_bytes=.*/path=" + arm + "$"
            run(f"pair {pair:02d} {arm}", [OUT / "direct.test", "-test.run=^$", "-test.bench=" + regex, "-test.benchmem", "-test.benchtime=100x", "-test.cpu=1", "-test.count=1", "-test.timeout=60s"], f"bench-{pair:02d}-{arm}.txt")
    manifest["status"] = "measurements_complete"
    save()
    # Verifier sees a complete pre-verification receipt manifest. Its own span
    # is added afterwards, then the manifest is rechecked by a separate reader.
    run("independent evidence verification", [sys.executable, HERE / "verify_artifacts.py", OUT, PREREG], "verification.log")
    manifest["status"] = "verified"
except BaseException as exc:
    manifest["status"] = "blocked"
    manifest["error"] = repr(exc)
    raise
finally:
    save()
    # Text transport permits independent retrieval through the GitHub connector;
    # the uploaded artifact also retains the binary and unabridged raw files.
    for path in sorted(OUT.iterdir()):
        if path.is_file() and path.name != "direct.test":
            print("EVIDENCE_FILE", path.name, base64.b64encode(path.read_bytes()).decode(), flush=True)
