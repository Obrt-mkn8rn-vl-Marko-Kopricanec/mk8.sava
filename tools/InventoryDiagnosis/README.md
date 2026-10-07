# Retained inventory timing diagnosis

Research-only, read-only tooling. It neither runs a storage workload nor changes
any service, timing threshold, test result, production source or archived input.

From the repository root, using the existing Python standard library:

```sh
python3 -m unittest discover -s tools/InventoryDiagnosis -v
python3 tools/InventoryDiagnosis/analyze.py artifacts/quality-observability-20261007/evidence-manifest.json artifacts/quality-page-comparison-20261007/evidence-manifest.json artifacts/quality-page-comparison-20261007/ci-evidence-manifest.json
```

Pass any other retained evidence indexes explicitly to broaden the historical
inventory. The tool verifies each indexed TRX length and SHA-256, parses all
individual outcomes and counters, then extracts this particular responsiveness
test. Identical TRX copies are aliases, not independent executions. Missing
diagnostics remain missing; malformed/inconsistent evidence fails analysis.

Tail rows are the slowest eight whole operations, not full latency histograms.
The selected p99 row is checked against the unchanged nearest-rank definition.
Phase fractions refer to that **same** mutation; do not sum unrelated quantiles,
subtract instrumentation, waive failures, or infer causal attribution.

The fixture buffers a one-byte write before `Flush(true)` on a WriteThrough
stream. Its `flush_ms` therefore includes the buffered write, disk synchronization
and scheduling. It does not isolate `fsync`/`FlushFileBuffers` latency. Separate
before/after controls, scanner pass bounds and observed I/O pressure narrow an
investigation but do not establish an environmental cause or exclude regression.

Keep exact historical source/assembly/CI qualifications in the original reports.
This output does not accept a package or certify production performance. A
development-only syscall/phase probe and fuller interpretation are recorded in
the internal 2026-10-07 inventory-diagnosis report; no target/DevOps exercise is
part of this research tool.
