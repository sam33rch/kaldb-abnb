# RocksDB Core Synchronization Protocol

## Overview
Defines the canonical writer/reader contract for shuttling RocksDB checkpoints through S3. The goal is to stream immutable SST files and minimal metadata from a single writer fleet to a read-only reader fleet without staging directories or ad-hoc listings.

Related ADRs:
- docs/adrs/0001-rocksdb-s3-sync-protocol.md
- docs/adrs/0002-reader-ingest-behind.md

## Writer Responsibilities
1. **Produce a consistent snapshot**
   - Every interval (e.g., 30 s) flush + compact (configure compaction to emit ~6 MiB SSTs so uploads stay small and latency stays flat), then use RocksDB `Checkpoint` to materialize a frozen view under a temp directory.
   - Snapshot fence with WAL alignment:
     - Pause Kafka/WAL consumption and record the current offset `X`.
     - Flush/compact and create a checkpoint that reflects all mutations through `X`.
     - Upload SSTs and MANIFEST/CURRENT for that checkpoint; confirm each PUT (200/201 or 412) before proceeding.
     - Publish `kaldb_manifest.json` with `walOffset = X` via conditional PUT (`If-Match`).
     - Resume WAL consumption.
2. **Upload only essential files**
   - Keep the S3 layout limited to two flat prefixes (plus the root-level manifest pointer):
     - `/kaldb_manifest.json`: the pointer file that describes the latest snapshot.
     - `metadata/`: holds every `MANIFEST-*` file plus the single `CURRENT`.
     - `ssts/`: holds every immutable SST.
   - During every checkpoint cycle, iterate the local snapshot and issue PUTs only for SSTs whose filenames are absent from the `durableStoredSsts` set. Still send each PUT with `If-None-Match "*"` as a safety net—S3 replies with `412 Precondition Failed` if the local state lags reality.
   - Rationale: concurrent uploads often finish out of order; tracking a simple set of uploaded filenames ensures we only retry files that genuinely need uploading while allowing parallel workers to proceed independently.
   - Upload the new `MANIFEST-*` into `s3://<bucket>/<db-prefix>/metadata/MANIFEST-...` and rewrite `metadata/CURRENT` so metadata stays centralized while SSTs remain flat.
   - Use `If-None-Match "*"` when uploading SSTs so existing immutable files are skipped; treat HTTP 200/201 (created) or 412 (already exists) as the only acceptable acknowledgements from S3, retrying on any other status.
   - **Invariant**: writers must never create RocksDB artifacts outside `s3://<bucket>/<db-prefix>/kaldb_manifest.json`, `…/metadata/`, or `…/ssts/`; readers assume no other prefixes are touched.
3. **Publish `kaldb_manifest.json`**
   - After uploading SSTs/MANIFEST files (each guarded with `If-None-Match "*"`, so existing objects yield `412 Precondition Failed`), rewrite `s3://<bucket>/<db-prefix>/kaldb_manifest.json` with the smallest payload that unblocks readers and includes integrity fields:
     ```json
     {
       "version": 8,
       "manifestId": "MANIFEST-000456",
       "generatedAt": "2024-11-08T02:15:57Z",
       "walOffset": 12873442,
      "sstFiles": [
        {"name": "000123.sst", "size": 6291456, "crc32c": "<base64>"},
        {"name": "000124.sst", "size": 6291456, "crc32c": "<base64>"}
      ]
     }
     ```
   - Required fields:
     - `version`: schema version so readers can gate new formats.
     - `manifestId`: RocksDB MANIFEST filename (always uppercase, e.g., `MANIFEST-000456`) that the reader concatenates with the shared `metadata/` prefix.
     - `generatedAt`: ISO-8601 timestamp for observability and staleness detection (informational only; not used for ordering/GC).
     - `walOffset`: Kafka (or external WAL) offset up to which the snapshot is durable; readers can ignore this field, but new writers use it to resume WAL replay after a crash.
     - `sstFiles`: array of objects describing every active SST with:
       - `name`: SST filename; readers map to `s3://<bucket>/<db-prefix>/ssts/<name>`.
       - `size`: object size in bytes (integer).
       - `crc32c`: base64-encoded CRC32C of the object (readers verify locally before ingesting). If using S3-managed checksums, copy the `x-amz-checksum-crc32c` value here.
   - All path conventions (two prefixes: `metadata/` + `ssts/`) live in code, so the manifest stays tiny regardless of DB size.
   - Guard the pointer update with a conditional PUT: when the writer last read `kaldb_manifest.json`, S3 returned an ETag (the object’s content hash). Include that ETag in the next PUT’s `If-Match` header so S3 only accepts the write if nobody else has changed the pointer in the meantime. If the ETag has changed, S3 returns `412 Precondition Failed`, signaling the writer to re-read and start over—no extra read-modify dance is required beyond the single read that produced the ETag. Any `409 Conflict` or other status must be treated as an actual failure (e.g., bucket/versioning issue) and retried only after remediation; only `200/201` or `412` count as acceptable outcomes for these conditional PUTs.
   - **Invariant**: `kaldb_manifest.json` is only rewritten after every newly listed SST/MANIFEST upload has completed (200/201 or 412) *and* the writer has persisted the WAL offset it is advertising; readers should never see a manifest that references missing objects or a WAL offset that wasn’t durably acknowledged.
   - **Invariant**: `walOffset` only advances after the snapshot (all SSTs plus MANIFEST/CURRENT) is durably stored in S3 and acknowledged at that Kafka offset, ensuring a replacement writer can resume exactly from that point without reapplying already-checkpointed mutations.

### Writer-side bookkeeping
- Persist lightweight metadata alongside the DB inside `kaldb_writer_state`:
  - `durableStoredSsts`: a set (or key-only column family) of SST filenames that have successfully been written to S3. Once a filename appears in the set, it is never removed.
  - `durableKaldbManifestEtag`: ETag observed when the pointer was last read (used solely to avoid an extra GET before the next conditional PUT).
- Before issuing PUTs, consult the set to skip SSTs we know are already in S3; this minimizes 412 responses (which S3 still bills like normal PUTs). Always keep conditional PUTs enabled to catch drifts.
- After each successful upload, insert the filename into `durableStoredSsts`, fsync the column family, and (optionally) compact old entries. *Never* add an entry unless S3 returned a success/412 for that exact PUT—otherwise we’d mark files durable that don’t actually exist. After publishing a manifest, update `durableKaldbManifestEtag` atomically so restarts don’t regress; again, only record the ETag returned by a successful pointer read.
- There’s no need to persist the manifest ID or WAL offset locally—the manifest itself remains the source of truth for replacement writers.
- **Note**: both `durableStoredSsts` and `durableKaldbManifestEtag` are writer-side optimizations to cut redundant PUTs/GETs; durability correctness still comes from S3’s conditional writes, so the system remains safe even if these caches are rebuilt after a crash—as long as entries are only added when S3 has already acknowledged the corresponding object.

- **Constraint**: the database uses a single column family (default). If multi-CF is needed in the future, the manifest must include a `cf` field per SST and readers must ingest into the matching CF.

## Reader Responsibilities (preferred ingest-behind flow)
1. **Detect new snapshots**
   - Cache the ETag returned by the most recent `GET kaldb_manifest.json`, then poll by issuing another GET with `If-None-Match: "<last-etag>"`. If S3 responds `304 Not Modified`, the reader keeps serving the existing snapshot without downloading anything; once the ETag changes, the GET returns `200 OK` with the new manifest payload and the reader proceeds to hydrate it.
2. **Ingest via `kaldb_manifest.json`**
   - Keep the RocksDB instance open in write mode with `allow_ingest_behind = true` (no snapshots).
   - For every new manifest, identify SST files in `sstFiles` that have not yet been ingested. Process them sequentially to minimize disk usage:
     - Download the SST into a temporary staging path (e.g., `/tmp/kaldb_sst_stage/<filename>`).
     - Verify the download locally (size and, once checksums are published, the checksum) before ingesting.
    - Call `IngestExternalFile` with `ingest_behind = true` and `write_global_seqno = true` so RocksDB atomically links the file, assigns local sequence numbers, and safely accepts overlapping ranges.
    - Reasoning: using `write_global_seqno = true` avoids any requirement that writer-produced SSTs have per-key seqno==0. Sequence numbers are logical, not time-based; assigning them locally does not affect durability or reader correctness, and a follow-up compaction collapses duplicates/overlaps.
     - Delete the staged file immediately after a successful ingestion so at most a handful of SSTs occupy local disk outside RocksDB’s own directory.
   - **Invariant**: ingestion happens exactly once per SST filename; RocksDB’s `IngestExternalFile` either links the entire SST set or leaves the DB untouched, so we only update local metadata after a successful return code and simply retry if the call fails.
   - After a successful ingestion, insert the filename into the `ingestedSsts` reader-state set so retries don’t re-download or re-ingest already applied files.
3. **Compact after ingestion**
   - Immediately run a compaction pass (e.g., `CompactRange`) after each ingestion batch so overlaps collapse and superseded SSTs are cleaned up automatically.
   - **Invariant**: compaction must complete after every ingestion batch before the corresponding manifest is considered fully applied; otherwise stale SSTs may linger indefinitely.
4. **Serve traffic**
   - Because ingest-behind readers never close/reopen the DB, traffic continues uninterrupted while new SSTs stream in.
   - **Invariant**: a manifest is considered applied (and `lastAppliedManifestId` may be advanced) only after all SSTs referenced by that manifest are ingested successfully and the post-ingest compaction completes; partial ingestion must not be exposed to queries.

### Reader-side bookkeeping (applies to both options)
1. Persist a tiny column family inside the reader RocksDB itself (e.g., `kaldb_reader_state`) containing:
   - `lastAppliedManifestId`: the most recent manifest ID successfully processed.
   - `ingestedSsts`: key-only entries for every SST filename successfully ingested (once an entry exists, it is never removed).
2. Before downloading an SST, check whether its filename already exists in `ingestedSsts`; if so, skip the transfer even if the physical file was removed locally by compaction.
3. After ingesting an SST (or, in the legacy flow, after copying it into place), insert the filename into `ingestedSsts` (regular RocksDB write + optional WAL flush) so crashes don’t cause double work.
4. On startup, read `ingestedSsts` to determine where to resume; if the writer’s manifest references a file that isn’t recorded yet, the reader knows it must fetch it—even if a previous compaction deleted the file from disk.
5. **Note**: `ingestedSsts` is an optimization to avoid redundant downloads; correctness still comes from `IngestExternalFile` being idempotent, but tracking filenames prevents unnecessary re-fetches, especially when ingestion happens concurrently.

## Failure Handling / Guarantees
1. **Strong consistency**: S3 PUT returning 200/201 guarantees visibility; the conditional pointer write ensures readers never see a partially uploaded snapshot.
   - Requirement: the object store must provide read-after-write consistency for new/overwritten objects referenced by `kaldb_manifest.json` (AWS S3 satisfies this). If targeting other stores, equivalent semantics are required for correctness.
2. **Idempotency**: SST uploads use `If-None-Match "*"`, so rerunning the writer loop after a transient failure is safe.
3. **No listings**: Readers rely on the embedded `sstFiles` list inside `kaldb_manifest.json`, avoiding `ListObjects` scalability limits.
4. **Flat namespace**: Writers only touch `metadata/` (MANIFEST + CURRENT) and `ssts/` (data), so only net-new SSTs ever cross the wire while metadata stays centralized.
5. **Single directory**: Readers operate on one RocksDB directory—ingest-behind keeps it writable but still mutates only the local directory.
6. **Ingest-behind safety**: Readers must enable `allow_ingest_behind`, persist which SSTs were applied, and trigger compaction after every ingestion batch; if ingestion fails due to overlaps, compact and retry so the next attempt succeeds.
7. **Crash recovery**: Replacement writers read `kaldb_manifest.json`, trust its `walOffset`, and resume Kafka/WAL consumption from that precise offset, replaying only the tail that hasn’t been checkpointed.
8. **GC safety**: Garbage collection must never delete SSTs referenced by recent manifests. Prefer a sweep that keeps the union of SSTs from the last N manifests (plus a grace period) rather than time-based deletion.

## Checksums
- Algorithm: use CRC32C for all objects written to S3 (SSTs, MANIFEST, CURRENT, and `kaldb_manifest.json`). For SSTs, include `size` (bytes) and `crc32c` (base64) per entry in `sstFiles`, and verify locally on the reader before ingestion.
- Writer behavior:
  - For every PUT, compute CRC32C while streaming and send `x-amz-checksum-crc32c` so S3 validates server-side and rejects bad uploads.
  - On success, copy S3’s returned checksum into `kaldb_manifest.json` for SST entries; only then consider the file durable.
  - Also send checksum headers for MANIFEST, CURRENT, and `kaldb_manifest.json` so S3 never persists corrupt metadata or pointer objects.
- Reader behavior:
  - For SSTs, recompute CRC32C locally (streaming) and compare with the manifest; on mismatch, retry the download and do not ingest.
  - For metadata objects used by the legacy (appendix) flow, optionally verify CRC32C on download before switching `CURRENT`.
  - Never rely on ETag for integrity; use CRC32C.
- Backward compatibility:
  - If older manifests lack checksums, either reject them (strict mode) or accept with size-only verification (permissive mode) until migration is complete.

## Appendix: Legacy snapshot-reopen flow (alternative considered)
1. Download missing SSTs for the new manifest (as described above) and let compaction, not manual deletes, reclaim superseded files.
   1. **Invariant**: `CURRENT` is never overwritten until every SST referenced by the manifest is present locally.
2. Overwrite `metadata/MANIFEST-…` and, only after every SST transfer succeeds, overwrite `metadata/CURRENT`.
3. Close the RocksDB instance, reopen it in read-only mode, and run a compaction so the new manifest is fully applied.
   1. **Invariant**: traffic may resume only after the reopened DB completes this compaction, ensuring the manifest on disk matches the writer’s pointer.

## Open Items
1. Add metrics/alerts for checksum validation failures and define the reader error policy on mismatch (retry/backoff/escalation).
2. Decide on retention / lifecycle management for old `metadata/MANIFEST-*` objects; GC can be a simple manifest diff that reconstructs cleanup history from the existing files.
3. Define how ingest-behind readers persist their `ingestedSsts` state so restarts don’t re-ingest already compacted data.
4. Specify the exact format/durability requirements for the writer’s `durableStoredSsts` set so redundant uploads (and paid 412s) stay bounded after crashes.
