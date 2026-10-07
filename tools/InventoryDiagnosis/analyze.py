"""Read-only, hash-bound analysis of retained physical-inventory TRX evidence."""

import argparse
from collections import Counter
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET


TEST_NAME = "FiftyThousandChunkScanKeepsConcurrentStagingMutationsResponsive"
PREFIXES = ("inventory_control_writes", "inventory_concurrent_writes", "inventory_mutation_tail")
PHASE_FIELDS = ("create_ms", "write_ms", "flush_ms", "close_ms", "delete_ms")
ROUNDING_TOLERANCE_MS = Decimal("0.006")


def timing_rows(text):
    """Ignore unrelated output, but reject malformed/duplicate timing fields."""
    rows = []
    for line in text.splitlines():
        match = re.search(r"\b(" + "|".join(PREFIXES) + r"),", line)
        if not match:
            continue
        fields = line[match.start():].strip().split(",")
        row = {"kind": fields[0]}
        for field in fields[1:]:
            key, value = field.split("=", 1)
            if key in row or not value:
                raise ValueError("Duplicate or empty timing field")
            row[key] = value
        rows.append(row)
    return rows


def analyze_timings(text):
    summaries = {}
    tails = {}
    for row in timing_rows(text):
        if row["kind"] == "inventory_mutation_tail":
            phase = row["phase"]
            total = Decimal(row["total_ms"])
            intervals = [Decimal(row[field]) for field in PHASE_FIELDS]
            if not total.is_finite() or total <= 0 or any(not value.is_finite() or value < 0 for value in intervals):
                raise ValueError("Invalid correlated timing interval")
            if abs(sum(intervals) - total) > ROUNDING_TOLERANCE_MS:
                raise ValueError("Correlated phase sum does not match whole-operation interval")
            tails.setdefault(phase, []).append(row)
        else:
            phase = row.get("phase", "concurrent")
            if phase in summaries:
                raise ValueError("Duplicate phase summary")
            p99 = Decimal(row["p99_mutation_ms"])
            if not p99.is_finite() or p99 < 0 or int(row["mutations"]) <= 0:
                raise ValueError("Invalid phase summary")
            summaries[phase] = row

    analysis = {}
    for phase, rows in tails.items():
        if phase not in summaries:
            raise ValueError("Tail has no corresponding sample summary")
        totals = [Decimal(row["total_ms"]) for row in rows]
        identifiers = [int(row["index"]) for row in rows]
        if totals != sorted(totals) or len(set(identifiers)) != len(rows):
            raise ValueError("Tail must be ordered and contain distinct mutation IDs")
        count = int(summaries[phase]["mutations"])
        if len(rows) > count or any(identifier < 0 or identifier >= count for identifier in identifiers):
            raise ValueError("Tail exceeds sample count or contains an out-of-range mutation ID")
        # These rows are the retained *slowest* mutations, not a complete histogram.
        offset = (99 * count + 99) // 100 - 1 - (count - len(rows))
        if not 0 <= offset < len(rows):
            raise ValueError("Retained tail does not contain the sample's p99 rank")
        selected = rows[offset]
        if abs(Decimal(selected["total_ms"]) - Decimal(summaries[phase]["p99_mutation_ms"])) > Decimal("0.002"):
            raise ValueError("Correlated p99 row differs from sample summary")
        analysis[phase] = {
            "tailRows": len(rows),
            "p99MutationIndex": int(selected["index"]),
            "p99CorrelatedRow": selected,
            "p99FlushFraction": float(Decimal(selected["flush_ms"]) / Decimal(selected["total_ms"])),
            "maximumRetainedMutationMs": float(totals[-1]),
        }
    return {"summaries": summaries, "correlatedTails": analysis,
            "missingCorrelatedTailPhases": sorted(set(summaries) - set(tails))}


def inspect_trx(data):
    root = ET.fromstring(data)
    results = root.findall(".//{*}UnitTestResult")
    counter_nodes = root.findall(".//{*}Counters")
    if len(counter_nodes) != 1:
        raise ValueError("Expected exactly one TRX counter inventory")
    counters = {key: int(value) for key, value in counter_nodes[0].attrib.items()}
    outcomes = Counter(result.attrib["outcome"] for result in results)
    checks = {"total": len(results), "executed": len(results) - outcomes["NotExecuted"],
              "passed": outcomes["Passed"], "failed": outcomes["Failed"]}
    if any(counters.get(key) != value for key, value in checks.items()):
        raise ValueError("TRX counters disagree with individual outcomes")
    records = []
    for result in results:
        if result.attrib["testName"].split(".")[-1] != TEST_NAME:
            continue
        output = result.find("{*}Output/{*}StdOut")
        records.append({
            "testName": result.attrib["testName"], "outcome": result.attrib["outcome"],
            "startedUtc": result.attrib.get("startTime"), "endedUtc": result.attrib.get("endTime"),
            "runCounters": counters, "individualOutcomes": dict(outcomes),
            **analyze_timings(output.text if output is not None and output.text else ""),
        })
    return records


def indexed_observations(repo, manifest_paths):
    observations = {}
    manifests = []
    inspected_trx = 0
    for manifest_path in manifest_paths:
        raw = manifest_path.read_bytes()
        manifest = json.loads(raw)
        manifests.append({"path": str(manifest_path), "sha256": hashlib.sha256(raw).hexdigest(),
                          "commit": manifest.get("commit"), "tree": manifest.get("tree")})
        for entry in manifest["files"]:
            relative = Path(entry["path"])
            if relative.suffix.lower() != ".trx":
                continue
            path = (repo / relative).resolve()
            if relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(repo.resolve()):
                raise ValueError("TRX path escapes repository")
            data = path.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            if len(data) != entry["bytes"] or digest != entry["sha256"]:
                raise ValueError("Indexed TRX identity mismatch: " + str(relative))
            inspected_trx += 1
            if digest in observations:
                observations[digest]["aliases"].append(str(relative))
                continue
            records = inspect_trx(data)
            observations[digest] = {"sha256": digest, "bytes": len(data),
                                    "aliases": [str(relative)], "records": records}
    return {"schemaVersion": 1, "indexedTrxEntriesInspected": inspected_trx, "manifests": manifests,
            "uniqueTrxFiles": len(observations),
            "observations": [value for value in observations.values() if value["records"]],
            "qualification": "Historical accepted mixed-assembly/CI identities stay qualified by their original reports. "
            "Duplicate file copies are aliases, not independent runs. Timing rows are correlated tails only; "
            "flush includes buffered write/synchronization/scheduling. No causality or threshold waiver is inferred."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("manifest", type=Path, nargs="+")
    args = parser.parse_args()
    print(json.dumps(indexed_observations(args.repo, args.manifest), indent=2))


if __name__ == "__main__":
    main()
