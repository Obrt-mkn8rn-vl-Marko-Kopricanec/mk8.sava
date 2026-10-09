"""Semantic/invalid-evidence controls for the read-only research parser."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

import analyze


def evidence(outcome="Failed", counter_failed=1):
    return f'''<TestRun xmlns="http://microsoft.com/schemas/VisualStudio/TeamTest/2010">
      <Results><UnitTestResult testName="Mk8.Sava.Tests.{analyze.TEST_NAME}" outcome="{outcome}">
        <Output><StdOut>inventory_concurrent_writes,passes=98,overlapping_passes=40,mutations=2,max_pass_ms=2.000,p99_mutation_ms=300.000
inventory_mutation_tail,phase=concurrent,index=0,total_ms=10.000,create_ms=1.000,write_ms=1.000,flush_ms=6.000,close_ms=1.000,delete_ms=1.000
inventory_mutation_tail,phase=concurrent,index=1,total_ms=300.000,create_ms=1.000,write_ms=1.000,flush_ms=296.000,close_ms=1.000,delete_ms=1.000</StdOut></Output>
      </UnitTestResult></Results>
      <ResultSummary><Counters total="1" executed="1" passed="{1 - counter_failed}" failed="{counter_failed}"/></ResultSummary>
    </TestRun>'''.encode()


def timeline_output():
    return "\n".join((
        "inventory_concurrent_writes,passes=3,overlapping_passes=1,mutations=2,max_pass_ms=4.000,p99_mutation_ms=300.000",
        "inventory_scan_timeline,phase=concurrent,timestamp_frequency=1000,passes=3",
        "inventory_scan_pass,phase=concurrent,index=0,started_ticks=0,finished_ticks=2",
        "inventory_scan_pass,phase=concurrent,index=1,started_ticks=8,finished_ticks=12",
        "inventory_scan_pass,phase=concurrent,index=2,started_ticks=299,finished_ticks=300",
        "inventory_mutation_tail,phase=concurrent,index=0,total_ms=10.000,create_ms=1.000,write_ms=1.000,"
        "flush_ms=6.000,close_ms=1.000,delete_ms=1.000,started_ticks=0,opened_ticks=1,written_ticks=2,"
        "flushed_ticks=8,closed_ticks=9,finished_ticks=10,scan_overlapping_passes=2,scan_overlap_ms=4.000,"
        "flush_scan_overlapping_passes=0,flush_scan_overlap_ms=0.000",
        "inventory_mutation_tail,phase=concurrent,index=1,total_ms=300.000,create_ms=1.000,write_ms=1.000,"
        "flush_ms=296.000,close_ms=1.000,delete_ms=1.000,started_ticks=0,opened_ticks=1,written_ticks=2,"
        "flushed_ticks=298,closed_ticks=299,finished_ticks=300,scan_overlapping_passes=3,scan_overlap_ms=7.000,"
        "flush_scan_overlapping_passes=1,flush_scan_overlap_ms=4.000",
    ))


def evidence_with_output(text):
    root = ET.fromstring(evidence())
    root.find(".//{*}StdOut").text = text
    return ET.tostring(root)


def empty_control_output():
    lines = [line for line in timeline_output().splitlines() if not line.startswith("inventory_scan_pass,")]
    lines[0] = "inventory_control_writes,phase=before,passes=64,mutations=2,p99_mutation_ms=300.000"
    lines[1] = "inventory_scan_timeline,phase=before,timestamp_frequency=1000,passes=0"
    text = "\n".join(lines).replace("phase=concurrent", "phase=before")
    for old, new in (("scan_overlapping_passes=2", "scan_overlapping_passes=0"),
                     ("scan_overlapping_passes=3", "scan_overlapping_passes=0"),
                     ("flush_scan_overlapping_passes=1", "flush_scan_overlapping_passes=0"),
                     ("scan_overlap_ms=4.000", "scan_overlap_ms=0.000"),
                     ("scan_overlap_ms=7.000", "scan_overlap_ms=0.000")):
        text = text.replace(old, new)
    return text


class AnalysisTests(unittest.TestCase):
    def test_preserves_failure_and_correlated_p99_not_independent_phase_quantiles(self):
        record, = analyze.inspect_trx(evidence())
        self.assertEqual("Failed", record["outcome"])
        tail = record["correlatedTails"]["concurrent"]
        self.assertEqual(1, tail["p99MutationIndex"])
        self.assertEqual("296.000", tail["p99CorrelatedRow"]["flush_ms"])
        self.assertAlmostEqual(296 / 300, tail["p99FlushFraction"])

    def test_rejects_counter_inventory_disagreement(self):
        with self.assertRaisesRegex(ValueError, "counters disagree"):
            analyze.inspect_trx(evidence(outcome="Passed"))

    def test_passed_target_does_not_turn_a_failed_full_inventory_into_success(self):
        data = evidence(outcome="Passed", counter_failed=0)
        data = data.replace(b"</Results>", b'<UnitTestResult testName="OtherTest" outcome="Failed"/></Results>')
        data = data.replace(b'total="1" executed="1" passed="1" failed="0"',
                            b'total="2" executed="2" passed="1" failed="1"')
        record, = analyze.inspect_trx(data)
        self.assertEqual("Passed", record["outcome"])
        self.assertEqual(1, record["runCounters"]["failed"])
        self.assertEqual({"Passed": 1, "Failed": 1}, record["individualOutcomes"])

    def test_missing_diagnostic_rows_are_missing_not_zero_or_success(self):
        record, = analyze.inspect_trx(evidence().replace(b"inventory_", b"older_"))
        self.assertEqual({}, record["summaries"])
        self.assertEqual("Failed", record["outcome"])
        self.assertEqual({}, record["scanTimelines"])

    def test_rejects_non_correlated_or_negative_phases(self):
        for replacement in (b"flush_ms=200.000", b"flush_ms=-296.000", b"flush_ms=NaN"):
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                analyze.inspect_trx(evidence().replace(b"flush_ms=296.000", replacement))

    def test_rejects_duplicate_mutation_or_incorrect_nearest_rank(self):
        for before, after in ((b"index=1", b"index=0"),
                              (b"index=1", b"index=-1"),
                              (b"index=1", b"index=2"),
                              (b"p99_mutation_ms=300.000", b"p99_mutation_ms=10.000")):
            with self.subTest(after=after), self.assertRaises(ValueError):
                analyze.inspect_trx(evidence().replace(before, after))

    def test_accepts_timestamped_logging_and_rounding_without_subtracting_time(self):
        data = evidence().replace(b"flush_ms=296.000", b"flush_ms=295.998")
        data = data.replace(b"inventory_concurrent_writes", b"2026-10-07T01:00:00Z inventory_concurrent_writes")
        record, = analyze.inspect_trx(data)
        self.assertEqual("300.000", record["correlatedTails"]["concurrent"]["p99CorrelatedRow"]["total_ms"])

    def test_identical_indexed_copies_are_aliases_not_independent_runs(self):
        with tempfile.TemporaryDirectory(prefix="mk8-sava-analysis-") as directory:
            repo = Path(directory)
            data = evidence()
            entries = []
            for name in ("original.trx", "copy.trx"):
                (repo / name).write_bytes(data)
                entries.append({"path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
            manifest = repo / "index.json"
            manifest.write_text(json.dumps({"files": entries}), encoding="utf-8")
            result = analyze.indexed_observations(repo, [manifest])
            self.assertEqual(2, result["indexedTrxEntriesInspected"])
            self.assertEqual(1, result["uniqueTrxFiles"])
            self.assertEqual(2, len(result["observations"][0]["aliases"]))
            (repo / "copy.trx").write_bytes(data + b" ")
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                analyze.indexed_observations(repo, [manifest])


class TimelineAnalysisTests(unittest.TestCase):
    def inspect(self, text):
        record, = analyze.inspect_trx(evidence_with_output(text))
        return record

    def test_validates_every_tail_and_binds_maximum_without_changing_failed_outcome(self):
        record = self.inspect(timeline_output())
        self.assertEqual("Failed", record["outcome"])
        timeline = record["scanTimelines"]["concurrent"]
        self.assertEqual(1000, timeline["timestampFrequency"])
        self.assertEqual(1, timeline["maximumPass"]["index"])
        self.assertEqual(4, timeline["maximumPass"]["durationTicks"])
        first, second = timeline["correlatedTailRows"]
        self.assertEqual({"passCount": 2, "timestampTicks": 4, "elapsedMs": "4"}, first["wholeOperation"])
        self.assertEqual({"passCount": 0, "timestampTicks": 0, "elapsedMs": "0"}, first["flush"])
        self.assertEqual({"passCount": 3, "timestampTicks": 7, "elapsedMs": "7"}, second["wholeOperation"])
        self.assertEqual({"passCount": 1, "timestampTicks": 4, "elapsedMs": "4"}, second["flush"])
        self.assertEqual(1, record["correlatedTails"]["concurrent"]["p99MutationIndex"])
        self.assertEqual([], record["missingScanTimelinePhases"])

    def test_legacy_missing_timeline_is_not_an_explicit_empty_control(self):
        record, = analyze.inspect_trx(evidence())
        self.assertEqual({}, record["scanTimelines"])
        self.assertEqual(["concurrent"], record["missingScanTimelinePhases"])

    def test_actual_empty_control_does_not_turn_writer_batches_into_scan_passes(self):
        record = self.inspect(empty_control_output())
        self.assertEqual("64", record["summaries"]["before"]["passes"])
        timeline = record["scanTimelines"]["before"]
        self.assertEqual([], timeline["passes"])
        self.assertIsNone(timeline["maximumPass"])
        for row in timeline["correlatedTailRows"]:
            self.assertEqual(0, row["wholeOperation"]["timestampTicks"])
            self.assertEqual(0, row["flush"]["passCount"])

    def test_zero_duration_and_touching_scan_endpoints_are_not_intersections(self):
        text = timeline_output().replace("index=2,started_ticks=299,finished_ticks=300",
                                         "index=2,started_ticks=300,finished_ticks=300")
        text = text.replace("scan_overlapping_passes=3,scan_overlap_ms=7.000",
                            "scan_overlapping_passes=2,scan_overlap_ms=6.000")
        timeline = self.inspect(text)["scanTimelines"]["concurrent"]
        self.assertEqual(0, timeline["passes"][-1]["durationTicks"])
        self.assertEqual(6, timeline["correlatedTailRows"][-1]["wholeOperation"]["timestampTicks"])

    def test_timeline_without_tail_remains_explicit_missing_tail(self):
        text = "\n".join(line for line in timeline_output().splitlines() if not line.startswith("inventory_mutation_tail,"))
        record = self.inspect(text)
        self.assertEqual(["concurrent"], record["missingCorrelatedTailPhases"])
        self.assertEqual([], record["scanTimelines"]["concurrent"]["correlatedTailRows"])

    def test_rejects_duplicate_missing_or_orphan_timeline_declarations(self):
        lines = timeline_output().splitlines()
        for text in ("\n".join(lines + [lines[1]]), "\n".join([lines[0]] + lines[2:]),
                     "\n".join(lines[1:]), "\n".join(lines[:1] + lines[5:])):
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.inspect(text)

    def test_rejects_invalid_frequencies_and_declared_counts(self):
        for original, replacement in (
                ("timestamp_frequency=1000", "timestamp_frequency=0"),
                ("timestamp_frequency=1000", "timestamp_frequency=-1"),
                ("timestamp_frequency=1000", "timestamp_frequency=1.0"),
                ("timestamp_frequency=1000", "timestamp_frequency=NaN"),
                ("timestamp_frequency=1000", "timestamp_frequency=9223372036854775808"),
                ("timestamp_frequency=1000", "missing_frequency=1000"),
                ("timestamp_frequency=1000,passes=3", "timestamp_frequency=1000,passes=111"),
                ("timestamp_frequency=1000,passes=3", "timestamp_frequency=1000,passes=-1"),
                ("timestamp_frequency=1000,passes=3", "timestamp_frequency=1000,passes=2"),
                ("passes=3,overlapping_passes=1", "passes=4,overlapping_passes=1")):
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                self.inspect(timeline_output().replace(original, replacement))

    def test_rejects_reversed_overlapping_unordered_out_of_range_or_missing_passes(self):
        for original, replacement in (
                ("index=1,started_ticks=8,finished_ticks=12", "index=1,started_ticks=13,finished_ticks=12"),
                ("index=1,started_ticks=8,finished_ticks=12", "index=1,started_ticks=1,finished_ticks=12"),
                ("index=1,started_ticks=8,finished_ticks=12", "index=1,started_ticks=-2,finished_ticks=0"),
                ("index=1,started_ticks=8,finished_ticks=12", "index=0,started_ticks=8,finished_ticks=12"),
                ("index=1,started_ticks=8,finished_ticks=12", "index=3,started_ticks=8,finished_ticks=12"),
                ("index=1,started_ticks=8,finished_ticks=12", "index=-1,started_ticks=8,finished_ticks=12"),
                ("index=1,started_ticks=8,finished_ticks=12", "index=1,started_ticks=8,finished_ticks=9223372036854775808"),
                ("index=0,started_ticks=0,finished_ticks=2", "index=0,started_ticks=-9223372036854775808,finished_ticks=2"),
                ("index=1,started_ticks=8,finished_ticks=12", "index=1,missing_start=8,finished_ticks=12")):
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                self.inspect(timeline_output().replace(original, replacement))

    def test_rejects_summary_maximum_that_disagrees_with_actual_pass_intervals(self):
        for value in ("3.999", "NaN", "-4.000", "word"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.inspect(timeline_output().replace("max_pass_ms=4.000", "max_pass_ms=" + value))

    def test_rejects_nonempty_null_scanner_control(self):
        text = timeline_output().replace("inventory_concurrent_writes,", "inventory_control_writes,phase=concurrent,")
        with self.assertRaisesRegex(ValueError, "empty actual scan timeline"):
            self.inspect(text)

    def test_rejects_falsified_overlap_even_on_a_non_p99_tail_row(self):
        for original, replacement in (
                ("scan_overlapping_passes=2,scan_overlap_ms=4.000", "scan_overlapping_passes=1,scan_overlap_ms=4.000"),
                ("scan_overlapping_passes=2,scan_overlap_ms=4.000", "scan_overlapping_passes=2,scan_overlap_ms=3.999"),
                ("flush_scan_overlapping_passes=0,flush_scan_overlap_ms=0.000", "flush_scan_overlapping_passes=1,flush_scan_overlap_ms=0.000"),
                ("flush_scan_overlapping_passes=0,flush_scan_overlap_ms=0.000", "flush_scan_overlapping_passes=0,flush_scan_overlap_ms=0.001"),
                ("scan_overlapping_passes=3", "scan_overlapping_passes=-1"),
                ("scan_overlap_ms=7.000", "scan_overlap_ms=Infinity")):
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                self.inspect(timeline_output().replace(original, replacement))

    def test_rejects_phase_falsification_even_if_old_phase_sum_still_matches(self):
        text = timeline_output().replace("write_ms=1.000,flush_ms=296.000", "write_ms=2.000,flush_ms=295.000")
        with self.assertRaisesRegex(ValueError, "Tick-derived duration"):
            self.inspect(text)

    def test_rejects_partial_or_reversed_mutation_endpoint_and_intersection_fields(self):
        for original, replacement in (("opened_ticks=1", "opened_ticks=3"),
                                      ("opened_ticks=1", "missing_opened=1"),
                                      ("flush_scan_overlap_ms=4.000", "missing_overlap=4.000"),
                                      ("finished_ticks=300,scan_overlapping", "finished_ticks=9223372036854775808,scan_overlapping")):
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                self.inspect(timeline_output().replace(original, replacement))

    def test_preserves_tick_precision_and_accepts_rounding_without_subtracting_time(self):
        text = timeline_output().replace("timestamp_frequency=1000", "timestamp_frequency=1000000000")
        rows = analyze.timing_rows(text)
        for row in rows:
            for key in ENDPOINT_KEYS:
                if key in row:
                    row[key] = str(390000000000000 + int(row[key]) * 1000000)
            if row["kind"] == "inventory_scan_pass" and row["index"] == "1":
                row["finished_ticks"] = str(int(row["finished_ticks"]) + 400)
        text = "\n".join(row["kind"] + "," + ",".join(key + "=" + value for key, value in row.items() if key != "kind") for row in rows)
        timeline = self.inspect(text)["scanTimelines"]["concurrent"]
        self.assertEqual(390000008000000, timeline["maximumPass"]["startedTicks"])
        self.assertEqual(7000400, timeline["correlatedTailRows"][-1]["wholeOperation"]["timestampTicks"])
        self.assertEqual("7.0004", timeline["correlatedTailRows"][-1]["wholeOperation"]["elapsedMs"])

    def test_checks_actual_timespan_quantization_not_a_broader_raw_tick_tolerance(self):
        # Golden values from retained producer rows: F3 follows 100ns TimeSpan
        # truncation, not direct rounding of the precise timestamp duration.
        for ticks, printed in ((30502, "0.030"), (54540, "0.054"), (61543, "0.061"),
                               (15598, "0.015"), (9373585, "9.373"), (68121512, "68.121")):
            with self.subTest(ticks=ticks):
                analyze.verify_rounded_ms({"duration_ms": printed}, "duration_ms", ticks, 1000000000)
                self.assertNotEqual(analyze.elapsed_ms(ticks, 1000000000), analyze.bcl_rounded_ms(ticks, 1000000000))
        with self.assertRaises(ValueError):
            analyze.verify_rounded_ms({"duration_ms": "0.055"}, "duration_ms", 54540, 1000000000)

    def test_rejects_a_duration_outside_the_producer_timespan_representation(self):
        with self.assertRaisesRegex(ValueError, "producer TimeSpan"):
            analyze.bcl_rounded_ms(analyze.INT64_MAX, 1)

    def test_accepts_all_110_ordered_passes_and_counts_only_clipped_intersections(self):
        text = timeline_output()
        lines = [line for line in text.splitlines() if not line.startswith("inventory_scan_pass,")]
        text = "\n".join(lines).replace("inventory_concurrent_writes,passes=3", "inventory_concurrent_writes,passes=110")
        text = text.replace("timestamp_frequency=1000,passes=3", "timestamp_frequency=1000,passes=110")
        text = text.replace("max_pass_ms=4.000", "max_pass_ms=1.000")
        for old, new in (("scan_overlapping_passes=2,scan_overlap_ms=4.000", "scan_overlapping_passes=4,scan_overlap_ms=4.000"),
                         ("flush_scan_overlapping_passes=0,flush_scan_overlap_ms=0.000", "flush_scan_overlapping_passes=2,flush_scan_overlap_ms=2.000"),
                         ("scan_overlapping_passes=3,scan_overlap_ms=7.000", "scan_overlapping_passes=100,scan_overlap_ms=100.000"),
                         ("flush_scan_overlapping_passes=1,flush_scan_overlap_ms=4.000", "flush_scan_overlapping_passes=99,flush_scan_overlap_ms=99.000")):
            text = text.replace(old, new)
        text += "\n" + "\n".join(f"inventory_scan_pass,phase=concurrent,index={index},started_ticks={3 * index},finished_ticks={3 * index + 1}" for index in range(110))
        timeline = self.inspect(text)["scanTimelines"]["concurrent"]
        self.assertEqual(110, len(timeline["passes"]))
        self.assertEqual(100, timeline["correlatedTailRows"][-1]["wholeOperation"]["passCount"])
        self.assertEqual(99, timeline["correlatedTailRows"][-1]["flush"]["timestampTicks"])

    def test_keeps_original_failure_message_without_a_reached_assertion_verdict(self):
        data = evidence_with_output(timeline_output()).replace(b"</ns0:StdOut>",
            b"</ns0:StdOut><ns0:ErrorInfo><ns0:Message>A bounded inventory pass exceeded 500 ms.</ns0:Message></ns0:ErrorInfo>")
        record, = analyze.inspect_trx(data)
        self.assertEqual("A bounded inventory pass exceeded 500 ms.", record["failureMessage"])
        self.assertEqual("Failed", record["outcome"])


ENDPOINT_KEYS = ("started_ticks", "opened_ticks", "written_ticks", "flushed_ticks", "closed_ticks", "finished_ticks")


class ManifestFormatTests(unittest.TestCase):
    def test_supports_both_historical_length_keys_without_rewriting_input(self):
        with tempfile.TemporaryDirectory(prefix="mk8-sava-manifest-") as directory:
            repo = Path(directory)
            data = evidence_with_output(timeline_output())
            trx = repo / "retained.trx"
            trx.write_bytes(data)
            for inventory_key in ("files", "entries"):
                for keys in ({"bytes": len(data)}, {"length": len(data)}, {"bytes": len(data), "length": len(data)}):
                    with self.subTest(inventory_key=inventory_key, keys=keys):
                        manifest = repo / "index.json"
                        raw = json.dumps({inventory_key: [{"path": trx.name, "sha256": hashlib.sha256(data).hexdigest(), **keys}]}).encode()
                        manifest.write_bytes(raw)
                        result = analyze.indexed_observations(repo, [manifest])
                        self.assertEqual("Failed", result["observations"][0]["records"][0]["outcome"])
                        self.assertEqual(raw, manifest.read_bytes())
                        self.assertEqual(data, trx.read_bytes())

    def test_rejects_missing_invalid_or_conflicting_length_declarations(self):
        for entry in ({}, {"bytes": True}, {"bytes": 1.0}, {"length": -1},
                      {"bytes": 3, "length": 4}, {"length": "3"}):
            with self.subTest(entry=entry), self.assertRaises(ValueError):
                analyze.indexed_length(entry)

    def test_rejects_duplicate_and_nonfinite_json_before_interpreting_an_index(self):
        for raw in ('{"files":[],"files":[]}', '{"files":[],"extra":NaN}',
                    '{"files":[],"extra":Infinity}', '{"files":[{"length":1,"length":2}]}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                analyze.strict_json(raw)

    def test_rejects_missing_invalid_or_conflicting_manifest_inventories(self):
        for manifest in ({}, [], {"files": {}}, {"entries": [1]}, {"files": [], "entries": [{}]}):
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                analyze.indexed_entries(manifest)
        self.assertEqual([], analyze.indexed_entries({"files": [], "entries": []}))


if __name__ == "__main__":
    unittest.main()
