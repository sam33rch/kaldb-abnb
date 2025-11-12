## RocksDB-Cloud Atomic Checkpointing Strategy

### Background

We are operating a topology with:

- **Single writer** service that ingests key/value updates from Kafka and writes
  into RocksDB-Cloud (WAL disabled, writes persisted through SST uploads).
- **Multiple reader-only cache nodes** that should only surface data that has
  been fully processed and checkpointed in Kafka.

The writer periodically issues a `Flush(FlushOptions{ .wait = true })` to force
its memtables into SSTs. Our primary requirement is that readers must never see
data newer than the last published Kafka offset, even if the writer crashes and
replays from its Kafka checkpoint.

### Selected Approach — Checkpoint Column Family + Atomic Flush

We introduced a dedicated column family (CF) that stores the latest Kafka
offset/sequence number. Every `WriteBatch` contains:

1. User-facing key/value updates (default CF or other CFs).
2. A single row in the checkpoint CF recording the highest Kafka offset covered
   by the batch.

Key RocksDB options and behaviors:

- `atomic_flush = true` in `DBOptions` guarantees that when multiple CFs have
  data ready to flush, RocksDB writes their SSTs and commits the corresponding
  MANIFEST edits atomically. Either all CFs in the batch appear in S3, or none
  do (`HISTORY.md:1905`).
- WAL remains disabled (standard RocksDB-Cloud pattern). Durability is tied to
  SST creation + MANIFEST sync **and** to the external KalDB WAL stream in
  Kafka. The upstream WAL provides redo information until a flush succeeds,
  while the SST/MANIFEST upload (done synchronously in
  `cloud/cloud_storage_provider.cc:158-235`) makes the batch visible to
  readers.
- We can still trigger manual `Flush` with `wait=true` to control publish
  cadence, but even auto flushes preserve the invariant because the checkpoint
  CF participates in the same atomic flush as the data CFs.

### Why This Provides Crash Consistency

| Scenario | Outcome |
| --- | --- |
| **Batch write succeeds, flush succeeds** | SSTs for data CFs and checkpoint CF upload together. MANIFEST references them atomically, so any reader that reloads sees both logical data and the matching Kafka offset. |
| **Batch write succeeds, writer crashes before flush** | Data + checkpoint only exist in memtables. RocksDB-Cloud loses the batch entirely. The writer restarts, reads the last durable checkpoint CF row, and resumes consuming Kafka from the next offset. Readers never saw the transient data. |
| **Flush fails (local or upload error)** | RocksDB reports the failure and keeps memtables intact. Nothing new appears in MANIFEST or S3, so readers cannot observe a partial batch. The writer retries the flush or rewrites the batch. |
| **Automatic flush triggers** | RocksDB promotes any dirty CF into the same flush job. Because every user batch also updates the checkpoint CF, the checkpoint SST is included in the flush. Readers may observe data earlier than we intended, but it always carries the matching checkpoint, so they can enforce their own “<= allowed offset” policy at read time. |

In all cases, the Kafka offset recorded in RocksDB never advances past the data
that was durably uploaded, which is the exact safety requirement for crash
recovery.

### Alternatives Considered

1. **Holding back CLOUDMANIFEST updates**
    - Initially appealing because `CLOUDMANIFEST` maps file numbers to epochs.
    - In practice every instance sharing a cookie reads the same `MANIFEST` file.
      Auto flush/compaction rewrites the manifest immediately, so readers would
      still see “in-flight” SSTs even if we never rewrote `CLOUDMANIFEST`.

2. **Cookie-based manifest rollover (per-reader cookie)**
    - Creates isolated `CLOUDMANIFEST-<cookie>` + `MANIFEST-<epoch>` pairs.
    - Readers must reopen with a new cookie to advance, which implies either
      downtime or a blue/green reader rollout. We need a solution that works
      without restarting cache nodes.

3. **Embedding Kafka checkpoints in MANIFEST/WAL metadata**
    - MANIFEST is the ground truth for RocksDB state; once a file’s version edit
      is recorded, every reader must honor it. There is no supported way to ask
      RocksDB to ignore entries past a custom watermark.
    - WAL metadata (`PutLogData`) only affects WAL recovery, not the visibility
      of flushed SSTs, so it cannot be used to gate readers.

These options either failed to hide transient SSTs or required operational
processes (cookie rollover) we cannot enforce on always-on readers.

### Reader Responsibilities

Readers continue running against the shared manifest/cookie. To avoid showing
data beyond their permitted Kafka position, they should:

1. Read the checkpoint CF key after opening the DB (and periodically).
2. Clamp any served data to the advertised checkpoint (e.g., by rejecting
   responses tagged with a higher offset).

Because the checkpoint CF row is updated in every batch, the value a reader sees
is always the highest Kafka offset that has fully flushed to S3.

### Operational Notes

- Ensure `WriteBatch`s always include both the data mutations and the checkpoint
  mutation. Any batch that omits the checkpoint could desynchronize the two.
- Size `write_buffer_size` and related limits so that you understand when auto
  flushes happen. Even though they remain crash-safe, you may want to avoid
  publishing extremely small batches.
- Readers that require strong publish control can still add cookie-based gating
  on top of this approach, but cookies are no longer required for crash safety.

### Cloud Deletion Configuration

- For KalDB’s RDBC sync protocol with a single cookie, configure the writer
  with `delete_cloud_invisible_files_on_open=false`.
  - Rationale: avoid deleting old MANIFEST/SST objects automatically on writer
    open while any Cacher may still be serving or refreshing. Cleanup can be
    orchestrated separately (e.g., via a maintenance window or S3 lifecycle).
  - With this setting, opening the writer will not remove "invisible" cloud
    files; readers remain safe to refresh at their own cadence.

With the checkpoint CF + `atomic_flush` configuration, the writer can crash,
replay from Kafka, and resume without risking “time travel” on readers: every
durable SST in S3 always has a matching Kafka checkpoint stored in RocksDB.
