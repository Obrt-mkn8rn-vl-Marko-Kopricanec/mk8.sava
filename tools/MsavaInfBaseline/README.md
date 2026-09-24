# MSAVA-INF allocated-byte baseline

This optional tool compiles seven unmodified MSAVA-INF source files from a
clean checkout at `17ccf1cd1a43d21360fd21b434a82ea2011b4f8f`. It invokes
the actual `FileManager.SaveTempFileAsync` and LiteDB `MetadataStore`, using
the same byte-generating seeds and five cumulative workloads as
`StorageAllocationBenchmarkTests`. No MSAVA source is copied into or linked
into the mk8.sava service or test assembly. This project is intentionally
outside `Mk8.Sava.slnx`.

The tool disables mk8.sava's code analyzers for its own build because those
rules would reject the unmodified linked MSAVA source. Compiler diagnostics
still fail the build. The main solution retains its strict analyzer gate.

Run on Linux with GNU `du`, choosing a fresh output directory on the **same
filesystem** used by mk8.sava's allocation benchmark:

```bash
baseline_output=$(mktemp -d /path/on/benchmark/filesystem/mk8-msava-inf-XXXXXX)
MSAVA_SOURCE_ROOT=/absolute/path/to/MSAVA \
  dotnet run --project tools/MsavaInfBaseline/MsavaInfBaseline.csproj \
  -c Release -p:BaseOutputPath="$baseline_output/" --no-launch-profile
```

The tool refuses a dirty or different MSAVA revision and refuses to reuse an
existing `Data` directory. Its CSV reports allocated bytes in the legacy
`Data` root, its SHA-256-named content files, and its LiteDB file. The total
also includes directory allocation and any journal files. Each source upload
uses the legacy `txt` extension because its INF content validator accepts
arbitrary bytes for that extension; mk8.sava's counterpart uses arbitrary
binary Blob content. This preserves the byte fixtures but is **not** an
end-to-end claim that MSAVA's public API accepted `.bin` uploads.
For the `eight_versions` checkpoint, the legacy tool saves eight distinct
metadata references and content hashes; it does not emulate Azure Blob
versioning in MSAVA.

The comparison excludes MSAVA's PostgreSQL business database, HTTP API,
business validation, authentication, backups, and deployment replication. Its
stage/register and direct-read times and process memory exclude those layers
and must not be compared directly with mk8.sava's SDK-over-HTTP timings. The
legacy temporary upload file is moved or deleted before each measurement;
peak temporary allocation is not captured in this baseline. The report is a
measured MSAVA-INF storage-floor comparator, not a production performance or
whole-system space claim. Keep the output directory for inspection or remove
that exact benchmark-generated path when no longer needed.
