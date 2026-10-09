"""Read-only, hash-bound analysis of retained physical-inventory TRX evidence."""

import argparse
from collections import Counter
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET


TEST_NAME = "FiftyThousandChunkScanKeepsConcurrentStagingMutationsResponsive"
PREFIXES = ("inventory_control_writes", "inventory_concurrent_writes", "inventory_mutation_tail",
            "inventory_scan_timeline", "inventory_scan_pass")
PHASE_FIELDS = ("create_ms", "write_ms", "flush_ms", "close_ms", "delete_ms")
ENDPOINT_FIELDS = ("started_ticks", "opened_ticks", "written_ticks", "flushed_ticks", "closed_ticks", "finished_ticks")
CORRELATION_FIELDS = ("scan_overlapping_passes", "scan_overlap_ms",
                      "flush_scan_overlapping_passes", "flush_scan_overlap_ms")
ROUNDING_TOLERANCE_MS = Decimal("0.006")
INT64_MAX = (1 << 63) - 1
INT64_MIN = -(1 << 63)
MAXIMUM_SCAN_PASSES = 110


def required_field(row, key):
    if key not in row:
        raise ValueError("Missing timing field: " + key)
    return row[key]


def integer_field(row, key, minimum=INT64_MIN, maximum=INT64_MAX):
    value = required_field(row, key)
    if not re.fullmatch(r"-?[0-9]+", value):
        raise ValueError("Invalid integer timing field: " + key)
    number = int(value)
    if not minimum <= number <= maximum:
        raise ValueError("Out-of-range timing field: " + key)
    return number


def elapsed_ms(ticks, frequency):
    # Avoid float narrowing for the retained Int64 ticks and timer frequency.
    with localcontext() as context:
        context.prec = 64
        return Decimal(ticks) * 1000 / Decimal(frequency)


def checked_interval(started, finished):
    ticks = finished - started
    if not 0 <= ticks <= INT64_MAX:
        raise ValueError("Invalid or overflowing timestamp interval")
    return ticks


def bcl_rounded_ms(ticks, frequency):
    # Model the pinned Stopwatch.GetElapsedTime -> TimeSpan.TotalMilliseconds
    # -> invariant F3 projection ONLY for checking the producer's printed field.
    # Retained integer ticks and the independent Decimal output are not replaced.
    span_ticks = int(ticks * (10000000.0 / frequency))
    if not 0 <= span_ticks <= INT64_MAX:
        raise ValueError("Timestamp duration cannot be represented by the producer TimeSpan")
    milliseconds = min(span_ticks / 10000.0, INT64_MAX // 10000)
    return Decimal(format(milliseconds, ".3f"))


def verify_rounded_ms(row, key, ticks, frequency):
    try:
        recorded = Decimal(required_field(row, key))
    except InvalidOperation as error:
        raise ValueError("Invalid millisecond timing field: " + key) from error
    if not recorded.is_finite() or recorded < 0 or recorded != bcl_rounded_ms(ticks, frequency):
        raise ValueError("Tick-derived duration disagrees with " + key)


def correlate_intervals(passes, started, finished, frequency):
    checked_interval(started, finished)
    count = 0
    ticks = 0
    for scan in passes:
        if scan["finishedTicks"] <= started:
            continue
        if scan["startedTicks"] >= finished:
            break
        intersection = min(finished, scan["finishedTicks"]) - max(started, scan["startedTicks"])
        if intersection > 0:
            count += 1
            ticks += intersection
            if ticks > INT64_MAX:
                raise ValueError("Overflowing scan intersection")
    return {"passCount": count, "timestampTicks": ticks, "elapsedMs": str(elapsed_ms(ticks, frequency))}


def analyze_scan_timelines(rows, summaries, tails):
    declarations = {}
    scan_rows = {}
    for row in rows:
        if row["kind"] not in ("inventory_scan_timeline", "inventory_scan_pass"):
            continue
        phase = required_field(row, "phase")
        if row["kind"] == "inventory_scan_pass":
            scan_rows.setdefault(phase, []).append(row)
        else:
            if phase in declarations:
                raise ValueError("Duplicate scan timeline declaration")
            declarations[phase] = row
    if set(scan_rows) - set(declarations):
        raise ValueError("Scan pass has no timeline declaration")
    if set(declarations) - set(summaries):
        raise ValueError("Scan timeline has no sample summary")
    for phase, mutations in tails.items():
        if phase not in declarations and any(set(row) & set(ENDPOINT_FIELDS + CORRELATION_FIELDS) for row in mutations):
            raise ValueError("Mutation timeline fields have no scan timeline declaration")

    timelines = {}
    for phase, declaration in declarations.items():
        frequency = integer_field(declaration, "timestamp_frequency", minimum=1)
        count = integer_field(declaration, "passes", minimum=0, maximum=MAXIMUM_SCAN_PASSES)
        retained = scan_rows.get(phase, [])
        if len(retained) != count:
            raise ValueError("Scan pass count disagrees with retained intervals")
        summary = summaries[phase]
        if summary["kind"] == "inventory_control_writes":
            if count != 0:
                raise ValueError("Null-scanner control must have an empty actual scan timeline")
        elif count == 0 or count != integer_field(summary, "passes", minimum=1, maximum=MAXIMUM_SCAN_PASSES):
            raise ValueError("Concurrent scan timeline disagrees with summary pass count")
        passes = []
        for index, row in enumerate(retained):
            if integer_field(row, "index", minimum=0, maximum=MAXIMUM_SCAN_PASSES - 1) != index:
                raise ValueError("Scan interval indices must be unique and contiguous in retained order")
            started = integer_field(row, "started_ticks")
            finished = integer_field(row, "finished_ticks")
            ticks = checked_interval(started, finished)
            if passes and started < passes[-1]["finishedTicks"]:
                raise ValueError("Scan intervals must be ordered and non-overlapping")
            passes.append({"index": index, "startedTicks": started, "finishedTicks": finished,
                           "durationTicks": ticks, "elapsedMs": str(elapsed_ms(ticks, frequency))})
        maximum = max(passes, key=lambda scan: scan["durationTicks"], default=None)
        if summary["kind"] == "inventory_concurrent_writes":
            verify_rounded_ms(summary, "max_pass_ms", maximum["durationTicks"], frequency)

        correlated = []
        for mutation in tails.get(phase, []):
            endpoints = [integer_field(mutation, key) for key in ENDPOINT_FIELDS]
            for key, started, finished in zip(PHASE_FIELDS, endpoints, endpoints[1:]):
                verify_rounded_ms(mutation, key, checked_interval(started, finished), frequency)
            verify_rounded_ms(mutation, "total_ms", checked_interval(endpoints[0], endpoints[-1]), frequency)
            total = correlate_intervals(passes, endpoints[0], endpoints[-1], frequency)
            flush = correlate_intervals(passes, endpoints[2], endpoints[3], frequency)
            for count_key, duration_key, intersection in (
                    ("scan_overlapping_passes", "scan_overlap_ms", total),
                    ("flush_scan_overlapping_passes", "flush_scan_overlap_ms", flush)):
                if integer_field(mutation, count_key, minimum=0, maximum=MAXIMUM_SCAN_PASSES) != intersection["passCount"]:
                    raise ValueError("Scan intersection count disagrees with " + count_key)
                verify_rounded_ms(mutation, duration_key, intersection["timestampTicks"], frequency)
            correlated.append({"mutationIndex": int(mutation["index"]), "wholeOperation": total, "flush": flush})
        timelines[phase] = {"timestampFrequency": frequency, "passes": passes, "maximumPass": maximum,
                            "correlatedTailRows": correlated}
    return timelines


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
    diagnostics = timing_rows(text)
    for row in diagnostics:
        if row["kind"] in ("inventory_scan_timeline", "inventory_scan_pass"):
            continue
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
    timelines = analyze_scan_timelines(diagnostics, summaries, tails)
    return {"summaries": summaries, "correlatedTails": analysis, "scanTimelines": timelines,
            "missingCorrelatedTailPhases": sorted(set(summaries) - set(tails)),
            "missingScanTimelinePhases": sorted(set(summaries) - set(timelines))}


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
            "failureMessage": result.findtext("{*}Output/{*}ErrorInfo/{*}Message"),
            "runCounters": counters, "individualOutcomes": dict(outcomes),
            **analyze_timings(output.text if output is not None and output.text else ""),
        })
    return records


def strict_json(data):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key: " + key)
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError("Nonfinite JSON value: " + value)

    return json.loads(data, object_pairs_hook=unique_object, parse_constant=reject_constant)


def indexed_length(entry):
    lengths = [entry[key] for key in ("bytes", "length") if key in entry]
    if not lengths or any(type(value) is not int or value < 0 for value in lengths) or len(set(lengths)) != 1:
        raise ValueError("Missing, invalid or conflicting indexed TRX length")
    return lengths[0]


def indexed_entries(manifest):
    if not isinstance(manifest, dict):
        raise ValueError("Evidence index must be a JSON object")
    inventories = [manifest[key] for key in ("files", "entries") if key in manifest]
    if (not inventories or any(not isinstance(value, list) for value in inventories)
            or any(value != inventories[0] for value in inventories[1:])
            or any(not isinstance(entry, dict) for entry in inventories[0])):
        raise ValueError("Missing, invalid or conflicting evidence index inventory")
    return inventories[0]


def indexed_observations(repo, manifest_paths):
    observations = {}
    manifests = []
    inspected_trx = 0
    for manifest_path in manifest_paths:
        raw = manifest_path.read_bytes()
        manifest = strict_json(raw)
        manifests.append({"path": str(manifest_path), "sha256": hashlib.sha256(raw).hexdigest(),
                          "commit": manifest.get("commit"), "tree": manifest.get("tree")})
        for entry in indexed_entries(manifest):
            relative = Path(entry["path"])
            if relative.suffix.lower() != ".trx":
                continue
            path = (repo / relative).resolve()
            if relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(repo.resolve()):
                raise ValueError("TRX path escapes repository")
            data = path.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            if len(data) != indexed_length(entry) or digest != entry["sha256"]:
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
            "flush includes buffered write/synchronization/scheduling. Validated scan intersections are call-interval "
            "chronology, not native execution or causality; missing timelines are not empty controls. "
            "Zero overlap excludes neither earlier/background effects nor indirect regression. "
            "No causality, reached-assertion verdict or threshold waiver is inferred."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("manifest", type=Path, nargs="+")
    args = parser.parse_args()
    print(json.dumps(indexed_observations(args.repo, args.manifest), indent=2))


if __name__ == "__main__":
    main()
