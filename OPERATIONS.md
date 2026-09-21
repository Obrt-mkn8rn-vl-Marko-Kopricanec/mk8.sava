# mk8.sava operations

The service keeps its authoritative metadata in `metadata.db` and immutable,
encrypted content extents under `chunks/`. The configured `Sava:DataPath` is a
single storage root; do not copy a live root with a generic filesystem command
and assume the result is consistent.

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

Crash-abandoned `.tmp` files are removed after
`Sava:AbandonedStagingRetention`, up to
`Sava:MaximumStagingFilesPerMaintenancePass` deletions per pass. A file still
held by an active request cannot be reclaimed.

## Create and validate a backup

The backup command runs without starting the HTTP listener. It takes a
transactionally consistent SQLite snapshot while all reachable immutable
chunks are pinned against garbage collection, verifies their storage integrity,
copies exactly that root set, and publishes the backup directory only after a
versioned manifest and all file hashes are durable.

```bash
dotnet Mk8.Sava.dll --backup-create /srv/backups/mk8-sava-2026-09-21
dotnet Mk8.Sava.dll --backup-validate /srv/backups/mk8-sava-2026-09-21
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
  dotnet Mk8.Sava.dll --restore-from /srv/backups/mk8-sava-2026-09-21
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

Metadata uses an explicit SQLite `user_version`; the current schema and backup
format are both version 1. A service refuses metadata newer than it understands
and a restore refuses unsupported backup or schema versions.

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
