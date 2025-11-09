# ADR 0001: RocksDB → S3 Synchronization Protocol

- Status: Superseded
- Superseded by: ADR 0002 (Reader Ingest-Behind Replication)
- Date: 2025-11-09

## Context
We need a durable, simple mechanism to ship a RocksDB writer’s state to many readers using S3 as the distribution channel, without listing or rebuilding DBs on readers.

## Decision (superseded by ADR 0002)
Adopt a flat S3 layout with two prefixes and a single pointer file:
- `metadata/`: MANIFEST-* and a single `CURRENT` (legacy path only)
- `ssts/`: immutable SST files
- `/kaldb_manifest.json`: pointer and snapshot description

The writer:
- Produces checkpoints aligned to a Kafka/WAL fence, uploads SSTs and MANIFEST/CURRENT with conditional PUTs and server-side CRC32C validation, then publishes `kaldb_manifest.json` via conditional PUT.
- Publishes `kaldb_manifest.json` with fields: `version`, `manifestId`, `generatedAt`, `walOffset`, and `sstFiles[]` with `{name, size, crc32c}`.

The reader (preferred) uses ingest-behind + compaction:
- Polls `kaldb_manifest.json` by ETag (`If-None-Match`), downloads only missing SSTs, verifies CRC32C, ingests with `allow_ingest_behind=true` and `write_global_seqno=true`, then compacts.

## Consequences
- Simple, scalable layout, S3 is the single source of truth.
- Readers never list; they hydrate strictly from the manifest.
- Integrity is enforced end-to-end via CRC32C on writer PUTs (S3 validation) and reader verification.

## Why Superseded
ADR 0001 captured the overall layout and pointer semantics but assumed a legacy reader that copied files and reopened RocksDB. We evolved to ADR 0002 to:
- Remove reader restarts and reduce apply latency by ingesting SSTs in-place (ingest-behind).
- Minimize disk pressure by staging/ingesting files one-by-one instead of copying the entire snapshot first.
- Support safe reader concurrency without coordination by tracking per-file ingestion state (`ingestedSsts`) and relying on RocksDB’s atomic ingest.
- Keep the writer contract unchanged (single pointer + conditional PUT) but clarify WAL fencing and integrity with CRC32C.
 - Eliminate redundant uploads: avoid re-writing the same SSTs to S3 on every snapshot. ADR 0002 formalizes a delta-only writer loop (conditional PUTs plus per-file durability tracking) so only net-new SSTs are uploaded, reducing costs and churn.

Key changes vs ADR 0001:
- Preferred reader path is ingest-behind (`allow_ingest_behind=true`, `write_global_seqno=true`) followed by compaction, rather than snapshot reopen.
- Manifest entries for SSTs now carry `size` and `crc32c` for reader-side verification and writer-side server validation.
- Writer/reader caches (`durableStoredSsts`, `durableKaldbManifestEtag`, `ingestedSsts`) are explicitly documented as optimizations, not sources of truth.

## References
- See docs/rocksdb-kv/ROCKSDB_CORE_SYNCHRONIZATION_PROTOCOL.md
- Superseding ADR: docs/adrs/0002-reader-ingest-behind.md
