# mk8.sava operations

The service keeps its authoritative metadata in `metadata.db` and encrypted,
content-addressed extents under `chunks/` or append-only small-object packs under
`packs/`. Chunk identities are immutable, while their verified physical encoding
may be atomically replaced by background recompression or pack compaction. The
configured `Sava:DataPath` is a single storage root; do not copy a live root with
a generic filesystem command and assume the result is consistent.

## Health and metrics

- `GET /health/live` reports that the process is running.
- `GET /health/ready` checks SQLite availability and the last incremental
  reachable-chunk integrity cycle. It returns HTTP 503 after a reachable chunk
  is found missing or fails authenticated decoding.
- `GET /metrics` emits Prometheus text for request count and duration, 5xx
  responses, logical and physical storage, staging use, integrity findings, and
  lifecycle maintenance. These operator endpoints contain no credentials or
  blob names, but deployments should still restrict them to the monitoring
  network.

Integrity scanning is controlled by
`Sava:IntegrityScanChunksPerMaintenancePass`. Customer-provided-key chunks are
structurally checked and counted separately because mk8.sava deliberately does
not retain those keys. An authorized read supplies the key and performs the
full AES-GCM, decompression, digest, and length verification.

Lifecycle work uses independent keyset cursors. Each pass examines at most
`Sava:BlobRecordsPerMaintenancePass` blob/version/snapshot records and
`Sava:ContainerRecordsPerMaintenancePass` container records for due copy,
rehydration, expiry, and retention transitions. It also removes at most
`Sava:UncommittedBlocksPerMaintenancePass` expired uncommitted blocks, oldest
first. A full cursor cycle repeats, so records created or moved behind a cursor
are deferred rather than lost.

Crash-abandoned `.tmp` files are removed after
`Sava:AbandonedStagingRetention`, up to
`Sava:MaximumStagingFilesPerMaintenancePass` deletions per pass. A file still
held by an active request cannot be reclaimed.

Unreachable immutable chunks are considered in filesystem key order, up to
`Sava:GarbageCollectionChunksPerMaintenancePass` per pass. Reachability is
checked through the transactional metadata index both before and after an
exclusive chunk reservation, so reclamation remains bounded without racing a
concurrent publication. A complete sweep resumes from the beginning; chunks
created behind an active cursor are therefore considered during the next
sweep.

Reachable chunks older than `Sava:BackgroundCompressionMinimumAge` are revisited
in cursor order, up to `Sava:BackgroundCompressionChunksPerMaintenancePass`.
The service authenticates and reconstructs the plaintext, writes a candidate at
`Sava:BackgroundCompressionQuality`, verifies it byte-for-byte, and atomically
publishes it only when it saves at least
`Sava:BackgroundCompressionMinimumSavingsBytes`. Active readers, backups, and
writes pin the old representation and cause that attempt to be deferred. This
physical rewrite does not update blob ETags or last-modified values.
Customer-provided-key chunks are excluded because the service does not retain
their keys. Recompression totals and serialized chunk-file bytes saved are
exported through `/metrics`.

Chunks no larger than `Sava:SmallChunkPackingThresholdBytes` are placed in
authenticated append-only packs when `Sava:EnableSmallChunkPacking` is enabled.
`Sava:ChunkPackTargetBytes` and `Sava:ChunkPackMaximumRecords` bound each pack.
Metadata publishes a chunk locator only after its complete framed record has
been flushed durably, and reads verify the frame identity and digest before the
normal AES-GCM, decompression, plaintext digest, and length checks. Packs older
than `Sava:ChunkPackSealAge` are sealed; maintenance considers at most
`Sava:ChunkPacksPerMaintenancePass` sealed packs and rewrites one only when its
unreachable bytes meet both `Sava:ChunkPackCompactionMinimumSavingsBytes` and
`Sava:ChunkPackCompactionMinimumDeadRatio`. Locator replacement is atomic, and
the old pack is deleted only after that transaction commits.

## Create and validate a backup

The backup command runs without starting the HTTP listener. It takes a
transactionally consistent SQLite snapshot while all reachable immutable
chunks are pinned against garbage collection, verifies their storage integrity,
copies exactly that root set, and publishes the backup directory only after a
versioned manifest and all file hashes are durable.

```bash
dotnet Mk8.Sava.API.dll --backup-create /srv/backups/mk8-sava-2026-09-21
dotnet Mk8.Sava.API.dll --backup-validate /srv/backups/mk8-sava-2026-09-21
```

The destination must not already exist and must be outside `Sava:DataPath`.
Store the completed directory on independent durable media. Validation checks:

- the backup and metadata schema versions;
- SQLite's own integrity check and the logical inventory recorded in the
  manifest;
- exact agreement between metadata reachability and the chunk manifest;
- SHA-256 and length for the metadata database and every encrypted chunk file;
- absence of undeclared chunk files and symbolic-link traversal; and
- fingerprints of the configured account or cross-account encryption keys
  needed to read the restored extents.

Key material is never written to the backup manifest. Preserve the deployment's
account keys separately; a fingerprint match cannot reconstruct a lost key.
Customer-provided keys remain the responsibility of their callers.

## Restore

Restore is deliberately offline and non-destructive. Stop the service, choose a
new nonexistent data path, configure the same required account keys, then run:

```bash
Sava__DataPath=/srv/mk8-sava-restored \
  dotnet Mk8.Sava.API.dll --restore-from /srv/backups/mk8-sava-2026-09-21
```

The command fully validates the source, copies into a private sibling staging
directory with hash verification, checks the copied SQLite database, and then
atomically renames that directory into the configured target. It refuses to
overwrite even an empty target. Start the service only after the command
succeeds, exercise representative full and ranged reads, and retain the old
root until the restored deployment is accepted.

If validation reports a missing or corrupt file, do not edit the manifest or
substitute an extent with a similar name. Recover a complete backup or a known
good durability copy. Deduplicated extents can affect multiple logical blobs.

## Format upgrades and rollback

Metadata uses an explicit SQLite `user_version`; the current metadata schema is
version 4 and the backup container format is version 1. Schema 2 adds
transactionally maintained chunk-reference indexes and logical-length counters;
schema 3 adds a transactional blob-tag search index; schema 4 adds authoritative
pack and packed-chunk locator tables. The JSON blob/block manifests remain
authoritative and startup and backup validation check the derived indexes against
them. The service migrates schema 1, 2, or 3 on startup and can validate or
restore backups using an older supported schema. Backups materialize packed
records as canonical standalone chunk files and remove pack locators from the
copied database, so the existing backup format remains self-contained. The
service refuses metadata newer than it understands, and restore refuses
unsupported backup or schema versions.

Before deploying a build that changes either format:

1. create and validate a fresh backup with the currently running build;
2. stop all writers;
3. retain the old binary, configuration, data root, and validated backup;
4. deploy the new build and allow its documented migration to finish;
5. run integrity/read checks before returning traffic; and
6. if rollback is required, restore the pre-upgrade backup into a new root with
   the old binary rather than attempting to downgrade mutated metadata in
   place.

Never delete the old root or the independent backup merely because an upgrade
or restore command completed.
