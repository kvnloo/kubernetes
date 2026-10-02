#!/usr/bin/env python3
"""Independently validate the bounded direct-lister experiment evidence.

Uses only Python's standard library. This reads receipts and outputs; it does
not run Go tests, trust a precomputed summary, or infer production performance.
"""

import argparse
from collections import Counter
import hashlib
import itertools
import json
import math
from pathlib import Path
import random
import re
import statistics
import sys


class VerificationError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise VerificationError(message)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise VerificationError(f"cannot read JSON {path}: {error}") from error


def read_jsonl(path):
    try:
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    except (OSError, ValueError) as error:
        raise VerificationError(f"cannot read JSONL {path}: {error}") from error


def validate_test_events(events, required_tests, label):
    require(events, f"{label}: empty test log")
    runs, passes = Counter(), Counter()
    packages = set()
    package_started = False
    package_passed = False
    for event in events:
        require(isinstance(event, dict), f"{label}: malformed event")
        action, test = event.get("Action"), event.get("Test")
        require(action in {"start", "run", "pause", "cont", "pass", "fail", "skip", "output"},
                f"{label}: unknown or missing action {action!r}")
        require(action not in {"fail", "skip"}, f"{label}: {action} event for {test or 'package'}")
        require(isinstance(event.get("Package"), str) and event["Package"],
                f"{label}: missing package attribution")
        packages.add(event["Package"])
        if action == "start" and not test:
            package_started = True
        if action == "run" and test:
            require(package_started, f"{label}: test ran before package start")
            runs[test] += 1
        if action == "pass" and test:
            require(runs[test] > passes[test], f"{label}: pass without preceding run: {test}")
            passes[test] += 1
        if action == "pass" and not test:
            require(package_started, f"{label}: package pass without start")
            package_passed = True
    require(len(packages) == 1, f"{label}: expected exactly one package")
    require(package_started and package_passed, f"{label}: missing package start/pass")
    require(runs == passes, f"{label}: unfinished test runs")
    for test in required_tests:
        require(runs[test] == 1 and passes[test] == 1,
                f"{label}: required test must run and pass exactly once: {test}")
    return {"package": next(iter(packages)), "required_tests": list(required_tests),
            "required_passed": len(required_tests), "all_test_runs": sum(runs.values())}


BENCHMARK_RE = re.compile(
    r"^BenchmarkDirectListerReadCost/op=(get|list)/n=(\d+)/managed_fields_bytes=(\d+)"
    r"/path=(cached|direct_synthetic)(?:-(\d+))?\s+(\d+)\s+(.+)$"
)
PARITY_RE = re.compile(
    r"READ_COST_PARITY op=(get|list) n=(\d+) managed_fields_bytes=(\d+) "
    r"raw_equal=(true|false) normalized_equal=(true|false) "
    r"direct_managed_fields_bytes=(\d+) cached_managed_fields_bytes=(\d+)"
)


def validate_parity_events(events, cells):
    text = "".join(event.get("Output", "") for event in events if event.get("Action") == "output")
    records = {}
    for operation, count, payload, raw_equal, normalized_equal, direct_bytes, cached_bytes in PARITY_RE.findall(text):
        key = (operation, int(count), int(payload))
        require(key in cells, f"unexpected parity cell {key}")
        require(key not in records, f"duplicate parity cell {key}")
        require((raw_equal == "true") == (key[2] == 0), f"unexpected raw parity for {key}")
        require(normalized_equal == "true", f"normalization did not restore parity for {key}")
        returned_count = key[1] if operation == "list" else 1
        require(int(direct_bytes) == returned_count * key[2] and int(cached_bytes) == 0,
                f"incorrect managedFields byte accounting for {key}")
        records[key] = {"operation": operation, "pod_count": key[1], "managed_fields_bytes": key[2],
                        "raw_equal": raw_equal == "true", "normalized_equal": True,
                        "direct_managed_fields_bytes": int(direct_bytes), "cached_managed_fields_bytes": 0}
    require(set(records) == set(cells), "missing managedFields parity receipts")
    return [records[key] for key in sorted(records)]


def parse_benchmark(text, arm, cells, iterations, cpu, label):
    require(re.search(r"(?m)^PASS\s*$", text), f"{label}: missing final PASS")
    require(not re.search(r"(?m)^(?:FAIL\b|--- FAIL:|panic:|fatal error:|WARNING: DATA RACE)", text),
            f"{label}: failure text in benchmark output")
    records = {}
    for line in text.splitlines():
        if not line.startswith("BenchmarkDirectListerReadCost/"):
            continue
        match = BENCHMARK_RE.fullmatch(line)
        require(match is not None, f"{label}: malformed benchmark record: {line}")
        operation, count, payload, observed_arm, suffix_cpu, observed_iterations, metric_text = match.groups()
        key = (operation, int(count), int(payload))
        require(observed_arm == arm, f"{label}: wrong arm {observed_arm}")
        require(suffix_cpu is None or int(suffix_cpu) == cpu, f"{label}: wrong CPU suffix")
        require(int(observed_iterations) == iterations, f"{label}: wrong iteration count")
        require(key in cells and key not in records, f"{label}: unexpected/duplicate benchmark cell {key}")
        fields = metric_text.split()
        require(len(fields) % 2 == 0, f"{label}: unpaired benchmark metrics")
        metrics = {}
        for offset in range(0, len(fields), 2):
            try:
                value = float(fields[offset])
            except ValueError as error:
                raise VerificationError(f"{label}: invalid metric value") from error
            unit = fields[offset + 1]
            require(unit not in metrics and math.isfinite(value) and value >= 0,
                    f"{label}: invalid or duplicate metric {unit}")
            metrics[unit] = value
        require({"ns/op", "B/op", "allocs/op"}.issubset(metrics), f"{label}: missing benchmark metrics")
        require(metrics["ns/op"] > 0, f"{label}: nonpositive time measurement")
        records[key] = metrics
    require(set(records) == set(cells), f"{label}: incomplete benchmark cells: {len(records)}/{len(cells)}")
    return records


def quantile(sorted_values, fraction):
    position = (len(sorted_values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * (position - lower)


def bootstrap_median_interval(samples):
    rng = random.Random(0)
    estimates = sorted(statistics.median(rng.choices(samples, k=len(samples))) for _ in range(10000))
    return [quantile(estimates, 0.025), quantile(estimates, 0.975)]


def expect_rejection(callback, label):
    try:
        callback()
    except VerificationError:
        return True
    raise VerificationError(f"verifier calibration failed to reject {label}")


def self_calibration():
    tests = ["TestKnownGood", "TestUnsafeConversionAliasesSource", "TestRequired"]

    def synthetic_events(included):
        events = [{"Action": "start", "Package": "calibration"}]
        for name in included:
            events.extend([{"Action": "run", "Package": "calibration", "Test": name},
                           {"Action": "pass", "Package": "calibration", "Test": name}])
        events.append({"Action": "pass", "Package": "calibration"})
        return events

    validate_test_events(synthetic_events(tests), tests, "known-good calibration")
    result = {"known_good_test_log_accepted": True}
    result["zero_test_green_rejected"] = expect_rejection(
        lambda: validate_test_events(synthetic_events([]), tests, "zero tests"), "zero tests")
    result["missing_required_test_rejected"] = expect_rejection(
        lambda: validate_test_events(synthetic_events(tests[:-1]), tests, "missing test"), "missing test")
    result["missing_unsafe_control_rejected"] = expect_rejection(
        lambda: validate_test_events(synthetic_events([tests[0], tests[2]]), tests, "missing control"), "missing control")
    cells = set(itertools.product(["get", "list"], [1, 100], [0, 4082, 32748]))
    lines = [f"BenchmarkDirectListerReadCost/op={op}/n={count}/managed_fields_bytes={size}/path=cached 100 10 ns/op 0 B/op 0 allocs/op"
             for op, count, size in sorted(cells)]
    parse_benchmark("\n".join(lines + ["PASS"]), "cached", cells, 100, 1, "known-good benchmark")
    result["known_good_benchmark_accepted"] = True
    result["incomplete_benchmark_rejected"] = expect_rejection(
        lambda: parse_benchmark("\n".join(lines[:-1] + ["PASS"]), "cached", cells, 100, 1, "incomplete benchmark"),
        "incomplete benchmark")
    result["wrong_iteration_count_rejected"] = expect_rejection(
        lambda: parse_benchmark("\n".join(lines + ["PASS"]).replace(" 100 ", " 99 "), "cached", cells, 100, 1, "wrong iterations"),
        "wrong iterations")
    return result


def validate_manifest(evidence, prereg_path, prereg, manifest, required_outputs):
    require(re.fullmatch(r"[0-9a-f]{40}", manifest.get("source_sha", "")) is not None, "missing/invalid source SHA")
    require(manifest.get("source_base") == prereg["source_base"], "source-base mismatch")
    require(manifest.get("prereg_sha256") == sha256(prereg_path), "preregistration hash mismatch")
    require(manifest.get("binary_sha256") == sha256(evidence / "direct.test"), "test-binary hash mismatch")
    require(isinstance(manifest.get("go_version"), str) and manifest["go_version"], "missing Go toolchain provenance")
    require(isinstance(manifest.get("platform"), (str, dict)) and manifest["platform"], "missing platform provenance")
    commands = manifest.get("commands")
    require(isinstance(commands, list) and commands, "missing command receipts")
    output_receipts = {}
    for command in commands:
        require(isinstance(command, dict), "malformed command receipt")
        require(isinstance(command.get("label"), str) and command["label"], "missing command label")
        require(isinstance(command.get("argv"), list) and command["argv"] and
                all(isinstance(argument, str) for argument in command["argv"]), "invalid command argv")
        require(command.get("returncode") == 0, f"failed command retained: {command['label']}")
        elapsed = command.get("elapsed_seconds")
        require(isinstance(elapsed, (int, float)) and math.isfinite(elapsed) and elapsed >= 0,
                f"missing/invalid elapsed span: {command['label']}")
        output = command.get("stdout_file")
        require(isinstance(output, str) and output, f"missing command output: {command['label']}")
        name = Path(output).name
        require((evidence / name).is_file(), f"missing command output file: {name}")
        require(name not in output_receipts, f"duplicate command output receipt: {name}")
        output_receipts[name] = command
    require(set(required_outputs).issubset(output_receipts), "missing required test/benchmark command receipt")
    return output_receipts


def argument_value(argv, flag):
    for position, argument in enumerate(argv):
        if argument.startswith(flag + "="):
            return argument[len(flag) + 1:]
        if argument == flag and position + 1 < len(argv):
            return argv[position + 1]
    return None


def verify(evidence, prereg_path):
    calibration = self_calibration()
    prereg, manifest = read_json(prereg_path), read_json(evidence / "manifest.json")
    execution = prereg["execution"]
    dimensions = execution["dimensions"]
    payload_sizes = dimensions.get("managed_fields_actual_bytes", [0, 4082, 32748])
    cells = set(itertools.product(dimensions["operation"], dimensions["pod_count"], payload_sizes))
    require(len(cells) == execution["benchmark_cells_per_arm"], "preregistered benchmark grid mismatch")
    pairs = execution["benchmark_pairs"]
    require(isinstance(pairs, int) and pairs > 0, "invalid preregistered pair count")
    arms = ["cached", "direct_synthetic"]
    benchmark_files = {f"bench-{pair:02d}-{arm}.txt" for pair in range(pairs) for arm in arms}
    outputs = {"correctness.jsonl", "replication.jsonl"} | benchmark_files
    receipts = validate_manifest(evidence, prereg_path, prereg, manifest, outputs)
    for name in ["correctness.jsonl", "replication.jsonl"]:
        argv = receipts[name]["argv"]
        selected_tests = argument_value(argv, "-test.run")
        require(isinstance(selected_tests, str) and selected_tests and "/" not in selected_tests,
                f"{name}: missing test selection or subtest-filtered execution")
        require(argument_value(argv, "-test.count") == "1", f"{name}: expected one explicit uncached repetition")
    actual_bench_files = {path.name for path in evidence.glob("bench-*.txt")}
    require(actual_bench_files == benchmark_files, "missing or unexpected benchmark log files")

    required_tests = execution["required_tests"]
    require("TestUnsafeConversionAliasesSource" in required_tests, "preregistration omitted unsafe conversion control")
    require(execution["correctness_repetitions"] == 1, "unsupported correctness repetition plan")
    correctness_events = read_jsonl(evidence / "correctness.jsonl")
    correctness = validate_test_events(correctness_events, required_tests, "correctness")
    replication_tests = [name for name in required_tests if name.startswith("TestExperiment")]
    require(replication_tests, "missing fresh-process replication targets")
    replication = validate_test_events(read_jsonl(evidence / "replication.jsonl"), replication_tests, "replication")
    require(correctness["package"] == replication["package"], "replication package mismatch")
    parity = validate_parity_events(correctness_events, cells)

    observed_order = [Path(command["stdout_file"]).name for command in manifest["commands"]
                      if Path(command["stdout_file"]).name in benchmark_files]
    expected_order = [f"bench-{pair:02d}-{arm}.txt" for pair in range(pairs)
                      for arm in (arms if pair % 2 == 0 else list(reversed(arms)))]
    require(observed_order == expected_order, "benchmark command order does not follow preregistered AB/BA schedule")
    records = {}
    for pair in range(pairs):
        for arm in arms:
            name = f"bench-{pair:02d}-{arm}.txt"
            argv = receipts[name]["argv"]
            require(argument_value(argv, "-test.cpu") == str(execution["benchmark_cpu"]),
                    f"{name}: missing/mismatched CPU command argument")
            require(argument_value(argv, "-test.benchtime") == str(execution["benchmark_iterations"]) + "x",
                    f"{name}: missing/mismatched fixed-iteration command argument")
            require(argument_value(argv, "-test.run") == "^$", f"{name}: benchmark command must disable tests")
            records[(pair, arm)] = parse_benchmark((evidence / name).read_text(), arm, cells,
                                                   execution["benchmark_iterations"], execution["benchmark_cpu"], name)
    cell_summaries = []
    for key in sorted(cells):
        samples = {arm: [records[(pair, arm)][key] for pair in range(pairs)] for arm in arms}
        deltas = {metric: [samples["direct_synthetic"][pair][metric] - samples["cached"][pair][metric]
                           for pair in range(pairs)] for metric in ["ns/op", "B/op", "allocs/op"]}
        cell_summaries.append({
            "operation": key[0], "pod_count": key[1], "managed_fields_bytes": key[2], "pairs": pairs,
            "cached_medians": {metric: statistics.median(sample[metric] for sample in samples["cached"]) for metric in deltas},
            "direct_synthetic_medians": {metric: statistics.median(sample[metric] for sample in samples["direct_synthetic"]) for metric in deltas},
            "paired_direct_minus_cached_medians": {metric: statistics.median(values) for metric, values in deltas.items()},
            "paired_time_delta_bootstrap_95_percent_ns_per_op": bootstrap_median_interval(deltas["ns/op"]),
            "paired_direct_minus_cached_samples": deltas,
        })
    return {
        "status": "VERIFIED_BOUNDED_EVIDENCE",
        "experiment_id": prereg["experiment_id"],
        "source_sha": manifest["source_sha"], "source_base": manifest["source_base"],
        "prereg_sha256": manifest["prereg_sha256"], "binary_sha256": manifest["binary_sha256"],
        "go_version": manifest["go_version"], "platform": manifest["platform"],
        "verifier_calibration": calibration, "correctness": correctness, "replication": replication,
        "managed_fields_parity": parity,
        "benchmark": {"pairs": pairs, "cells_per_arm": len(cells), "records_per_arm": pairs * len(cells),
                      "iterations_per_record": execution["benchmark_iterations"], "cpu": execution["benchmark_cpu"],
                      "bootstrap_seed": 0, "bootstrap_resamples": 10000, "cell_results": cell_summaries},
        "command_elapsed_seconds": {command["label"]: command["elapsed_seconds"] for command in manifest["commands"]},
        "required_output_sha256": {name: sha256(evidence / name) for name in sorted(outputs)},
        "scope": "Component tests and warmed synthetic-storage costs only. No real watch-cache, production safety, or whole-system speedup claim. Source attribution follows the manifest and CI receipt; the verifier checks supplied binary and preregistration hashes.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", nargs="?", type=Path)
    parser.add_argument("prereg", nargs="?", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        if args.self_test:
            print(json.dumps({"status": "CALIBRATION_PASSED", "checks": self_calibration()}, indent=2, sort_keys=True))
            return 0
        require(args.evidence is not None and args.prereg is not None, "provide evidence directory and preregistration path")
        result = verify(args.evidence, args.prereg)
        (args.evidence / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": result["status"], "required_tests_passed": result["correctness"]["required_passed"],
                          "replication_tests_passed": result["replication"]["required_passed"],
                          "benchmark_records_per_arm": result["benchmark"]["records_per_arm"],
                          "summary": str(args.evidence / "summary.json")}, sort_keys=True))
        return 0
    except (VerificationError, OSError, KeyError, TypeError, ValueError) as error:
        failure = {"status": "BLOCKED", "reason": str(error)}
        if args.evidence is not None and args.evidence.is_dir():
            (args.evidence / "summary.json").write_text(json.dumps(failure, indent=2, sort_keys=True) + "\n")
        print(json.dumps(failure, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
