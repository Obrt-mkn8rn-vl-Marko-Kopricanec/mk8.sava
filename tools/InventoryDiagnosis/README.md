# Retained inventory timing diagnosis

Research-only, read-only tooling. It neither runs a storage workload nor changes
any service, timing threshold, test result, production source or archived input.

From the repository root, using the existing Python standard library:

```sh
python3 -m unittest discover -s tools/InventoryDiagnosis -v
python3 tools/InventoryDiagnosis/analyze.py artifacts/quality-observability-20261007/evidence-manifest.json artifacts/quality-page-comparison-20261007/evidence-manifest.json artifacts/quality-page-comparison-20261007/ci-evidence-manifest.json
python3 tools/InventoryDiagnosis/analyze.py artifacts/quality-inventory-timeline-20261009/evidence/evidence-manifest.json
```

Pass any other retained evidence indexes explicitly to broaden the historical
inventory. The tool verifies each indexed TRX length and SHA-256, parses all
individual outcomes and counters, then extracts this particular responsiveness
test. Identical TRX copies are aliases, not independent executions. Missing
diagnostics remain missing; malformed/inconsistent evidence fails analysis.
Both `files`/`entries` inventory containers and historical `bytes`/later `length`
keys are accepted. If both forms are present they must agree; duplicate JSON keys
and nonfinite constants are rejected.
No source evidence or index is rewritten by this tool.

Tail rows are the slowest eight whole operations, not full latency histograms.
The selected p99 row is checked against the unchanged nearest-rank definition.
Phase fractions refer to that **same** mutation; do not sum unrelated quantiles,
subtract instrumentation, waive failures, or infer causal attribution.

When a sample declares `inventory_scan_timeline`, the tool verifies every ordered,
non-overlapping pass and its contiguous index, the positive timestamp frequency,
the bounded 110-pass inventory and the rounded maximum-pass summary. It checks
all six retained mutation endpoints, every tick-derived phase duration and the
positive half-open whole/flush intersections for **every** retained tail row,
not just the selected p99 row. Checked Int64 bounds and Decimal output preserve
the retained ticks. For printed-field verification **only**, the parser models
the pinned runtime's double-based Stopwatch conversion, truncation to 100ns
TimeSpan ticks, TotalMilliseconds and invariant F3 formatting. It requires that
numeric printed value, rather than widening a raw-tick error tolerance. Durations
outside the producer TimeSpan representation are rejected. Exact fractional
timestamp-derived milliseconds remain separately available in the output.
The comparison model follows the pinned [Stopwatch](https://raw.githubusercontent.com/dotnet/runtime/v10.0.12/src/libraries/System.Private.CoreLib/src/System/Diagnostics/Stopwatch.cs)
and [TimeSpan](https://raw.githubusercontent.com/dotnet/runtime/v10.0.12/src/libraries/System.Private.CoreLib/src/System/TimeSpan.cs)
sources and the [numeric formatting contract](https://learn.microsoft.com/en-us/dotnet/standard/base-types/standard-numeric-format-strings).
This is source/data correspondence, not personally executed .NET or native proof.
Gaps, touching endpoints and zero-duration
passes contribute no positive intersection. Explicit null-scanner controls have
zero actual passes, even though their writer-batch summary counts remain positive.

`scanTimelines` exposes the raw endpoints, exact tick counts, decimal millisecond
conversion, maximum-pass binding and per-tail whole/flush correlation.
The maximum binds the longest retained raw call interval (first on an exact tie),
not a unique native operation or a causal culprit.
`missingScanTimelinePhases` distinguishes legacy missing diagnostics from an
explicitly empty control. Partial or contradictory new-format fields fail
analysis rather than being treated as zero; declared timelines without tails
still retain the existing explicit missing-tail classification. Original target
failure messages and whole-run counters remain observations, not a verdict that
later assertions ran or passed. No threshold is waived or workload rerun by
analysis. Existing raw stdout remains authoritative.

These are observed **call** intervals, including possible suspension, not native
I/O execution, kernel entry/return, scheduling or causal proof. Zero intersection
does not exclude earlier/background effects or indirect whole-process regression.
New Tests-only recording overhead is not subtracted or ruled out as influence.

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
