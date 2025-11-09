# ADR 0002: Reader Ingest-Behind Replication (Preferred)

- Status: Accepted
- Date: 2025-11-09

## Context
We want readers to apply the writer’s snapshots without DB restarts and with minimal disk overhead while preserving correctness.

## Decision
- Readers operate in write mode, download SSTs listed in `kaldb_manifest.json`, verify CRC32C, and call `IngestExternalFile` with `allow_ingest_behind=true` and `write_global_seqno=true`.
- After each batch, run compaction to collapse overlaps and reclaim space.
- Track `ingestedSsts` locally to avoid re-downloading after restarts; only update after successful ingestion.

## Rationale / Evolution
- Supersedes the legacy “snapshot + reopen” approach (ADR 0001) to avoid reader restarts, reduce apply latency, and minimize disk usage.
- Preserves a single-writer, single-pointer model while letting readers apply deltas continuously.
- Leverages RocksDB atomic ingest + compaction for correctness; local sequence numbers may differ across readers but do not affect durability.

## Durability Model
- Writer fencing: pause WAL at offset X → checkpoint → upload SSTs/metadata with conditional PUTs and CRC32C → publish `kaldb_manifest.json` with `walOffset=X` via `If-Match` → resume WAL.
- S3 consistency: relies on read-after-write semantics for objects referenced by a new manifest.
- Integrity: every object PUT includes `x-amz-checksum-crc32c` and is validated by S3; readers verify CRC32C locally before ingest.
- Pointer safety: the manifest pointer moves only after every referenced object returns 200/201 or 412; readers never see torn snapshots.

## Correctness Properties
- Atomic ingest: `IngestExternalFile` either links the whole file or leaves the DB untouched; readers update `ingestedSsts` only on success.
- Apply gating: a manifest is considered applied (and may be served) only after all SSTs are ingested and compaction completes.
- Single-writer constraint: only one writer may publish to a db-prefix; otherwise, orphan objects and undefined ordering can occur.

## Failure Scenarios
- Writer crash mid-upload: pointer not advanced; new writer reads latest manifest and resumes from its `walOffset`.
- Reader crash mid-ingest: upon restart, consult `ingestedSsts` and skip already-ingested filenames; re-download/ingest only missing ones.
- S3 412 on pointer: refresh ETag via GET and retry conditional PUT.
- Checksum mismatch on reader: retry download; do not ingest.

## Consequences
- Continuous apply without reopen, higher ingestion throughput with concurrency, and correctness backed by RocksDB atomic ingest + compaction.
- Local sequence numbers differ across readers but do not impact durability.

## Constraints & Assumptions
- Single column family (default). If multi-CF is needed, the manifest must include a CF tag for each SST and readers must target that CF.
- Object store provides read-after-write consistency on PUT and overwrite for referenced objects.

## Non-Goals
- Cross-reader sequence number determinism.
- Multi-writer merge at the same db-prefix.

## References
- See docs/rocksdb-kv/ROCKSDB_CORE_SYNCHRONIZATION_PROTOCOL.md (Reader Responsibilities)
