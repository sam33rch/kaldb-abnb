# RocksDB Core Synchronization Protocol

## Overview
Defines the canonical writer/reader contract for shuttling RocksDB checkpoints through S3. The goal is to stream immutable SST files and minimal metadata from a single writer fleet to a read-only reader fleet without staging directories or ad-hoc listings.

## Writer Responsibilities
1. **Produce a consistent snapshot**
   - Every interval (e.g., 30 s) flush + compact, then use RocksDB `Checkpoint` to materialize a frozen view under a temp directory.
2. **Upload only essential files**
   - Copy `*.sst`, `MANIFEST-*`, and `CURRENT` into `s3://<bucket>/<db-prefix>/manifest_<id>/…`.
   - Use `If-None-Match "*"` when uploading SSTs so existing immutable files are skipped; treat HTTP 200/201 (created) or 412 (already exists) as the only acceptable acknowledgements from S3, retrying on any other status.
3. **Emit per-snapshot manifest**
   - Generate `kaldb_db_manifest.json` within the snapshot prefix containing an entry per file:
     ```json
     {
       "manifestId": "manifest_0042",
       "generatedAt": "2024-11-08T02:15:57Z",
       "files": [
        {"name": "000123.sst", "s3Uri": "s3://bucket/db-prefix/manifest_0042/000123.sst", "type": "SST"},
         {"name": "MANIFEST-000456", "s3Uri": "s3://bucket/db-prefix/manifest_0042/MANIFEST-000456", "type": "MANIFEST"},
         {"name": "CURRENT", "s3Uri": "s3://bucket/db-prefix/manifest_0042/CURRENT", "type": "CURRENT"}
       ]
     }
     ```
   - After every upload succeeds, rewrite `s3://<bucket>/<db-prefix>/kaldb_snapshot_manifest` with:
     ```json
     {
       "version": 7,
       "latestManifestId": "manifest_0042",
       "snapshotManifestUri": "s3://bucket/db-prefix/manifest_0042/kaldb_db_manifest.json",
       "previous": [...]
     }
     ```
   - Use a conditional PUT (`If-Match` on the ETag retrieved during the last pointer read) so concurrent writers cannot clobber the pointer; this is a read-once/write-once operation leveraging the already fetched ETag—no additional read-modify cycle is needed.

## Reader Responsibilities
1. **Detect new snapshots**
   - Poll `kaldb_snapshot_manifest` using `If-None-Match` so S3 returns `304 Not Modified` when the pointer hasn’t changed, cutting needless downloads while still guaranteeing we see the new manifest as soon as its ETag flips.
2. **Download via manifest**
   - When a newer manifest appears, fetch the referenced `kaldb_db_manifest.json`.
   - For each entry:
     - Download SSTs only if that filename is absent locally (immutable guarantee).
     - Always overwrite MANIFEST/CURRENT (and any other metadata types).
3. **Apply snapshot**
   - After all files succeed, download the snapshot’s `CURRENT`, close the read-only RocksDB instance, then reopen (still read-only) and always compact so it consumes the new manifest.
4. **Serve traffic**
   - Readers never mutate S3; they only hydrate missing SSTs and switch manifests locally.

## Failure Handling / Guarantees
- **Strong consistency**: S3 PUT returning 200/201 guarantees visibility; the conditional pointer write ensures readers never see a partially uploaded snapshot.
- **Idempotency**: SST uploads use `If-None-Match "*"`, so rerunning the writer loop after a transient failure is safe.
- **No listings**: Readers rely on the per-snapshot manifest to know exactly which files exist, avoiding `ListObjects` scalability limits.
- **Single directory**: Readers operate on one RocksDB directory opened in read-only mode; SST copies happen in place, and a reopen is required after `CURRENT` is refreshed.

## Open Items
- Add checksums to `kaldb_db_manifest.json` for integrity verification.
- Decide on retention / lifecycle management for old `manifest_<id>` prefixes.
- Track applied manifest IDs per reader to coordinate garbage collection and rollbacks.
