"""Semantic/invalid-evidence controls for the read-only research parser."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
