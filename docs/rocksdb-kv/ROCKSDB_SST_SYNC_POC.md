# RocksDB SST Sync Prototype – Working Plan

## Goals
- Align with the canonical flow documented in `docs/rocksdb-kv/ROCKSDB_CORE_SYNCHRONIZATION_PROTOCOL.md` while keeping the prototype self-contained.
- Demonstrate end-to-end “writer → S3 backup → reader restore” flow using actual HTTP servers.
- Keep the prototype self-contained (no reuse of existing kaldb services); everything lives in a new package/module.
- Use RocksDB’s built-in `BackupEngine` for incremental backups/restores instead of the ad-hoc manifest logic from the tests.
- Provide a simple test harness that issues writes through the writer HTTP API and reads them back via the reader HTTP API.

## Components

### Writer Service
- Java HTTP server (Simple `HttpServer`) exposing:
  - `POST /kv` with JSON body `{ "key": "...", "value": "..." }`.
  - `GET /healthz` for smoke checks.
- Maintains a local RocksDB instance (e.g., `~/temp/rocksdb-writer`).
- Every `POST` writes the key/value to RocksDB and responds 202.
- Every 30 seconds (scheduler):
  1. Flush + compact the DB, then create a RocksDB checkpoint in a temp directory (produces a self-consistent view).
  2. Assign a new manifest id (e.g., `manifest_0042`) and upload only the RocksDB-critical files from the checkpoint—`*.sst`, `MANIFEST-*`, and `CURRENT`—to `s3://<bucket>/<db-prefix>/<manifest_id>/...` using `If-None-Match "*"` so immutable SSTs never re-upload.
  3. Emit `kaldb_db_manifest.json` inside the snapshot prefix—a JSON document enumerating **every** required object (`s3Uri`, filename, type, optional size/checksum)—so readers can diff exactly which SSTs are already present locally without issuing S3 list calls.
  4. Update the canonical pointer `s3://<bucket>/<db-prefix>/kaldb_snapshot_manifest` via a conditional PUT (`If-Match` on the previous ETag). This is the gate: if the conditional write succeeds, readers know the entire snapshot (including `CURRENT`) is durable.

### Reader Service
- Java HTTP server exposing `GET /kv?key=...` and `GET /healthz`.
- Uses a single RocksDB directory (e.g., `~/temp/rocksdb-reader`) opened in read-only mode so immutable SSTs can be copied in-place while requests are served.
- Every 30 seconds:
  1. Fetch `kaldb_snapshot_manifest` (with `If-None-Match` to avoid re-downloading identical content), inspect its `latestManifestId`/timestamp, and compare it to the manifest id currently mounted locally.
  2. If the pointer references a newer manifest, download that snapshot’s `kaldb_db_manifest.json` (the file list) and:
     - For every `.sst` entry, download it *only if* the same filename is missing locally (SSTs are immutable, so existing copies stay valid).
     - For all other entries (`MANIFEST-*`, etc.) always re-download so the reader mirrors the writer’s metadata exactly.
     - `kaldb_db_manifest.json` itself is overwritten locally so future restarts know which files belong to the mounted manifest.
  3. After all listed files succeed, fetch `CURRENT`, then acquire the write lock, close the read-only RocksDB instance, optionally run a compaction (requires a short read-write reopen), and finally reopen in read-only mode so it observes the new manifest.
- `GET /kv` reads from the reopened RocksDB instance.

### S3 Layout (Updated Decision)
- Each snapshot lives under its own prefix: `s3://<bucket>/<db-prefix>/manifest_<NNNN>/...` and contains the full RocksDB checkpoint (all `.sst`, `MANIFEST-*`, `CURRENT`, etc.).
- A canonical pointer `s3://<bucket>/<db-prefix>/kaldb_snapshot_manifest` (JSON) carries the latest manifest id, history, and the `s3Uri` of that snapshot’s `kaldb_db_manifest.json`. The per-snapshot manifest lists every file name explicitly so readers can compare against their local directory to find missing SSTs quickly. Writers rewrite this object only after the snapshot upload succeeds (conditional PUT with the previous ETag).
- Upload order: (1) generate checkpoint, (2) PUT every file inside the manifest prefix using `If-None-Match "*"`, (3) write `kaldb_db_manifest.json` describing the snapshot contents, and (4) update `kaldb_snapshot_manifest`.
- Readers poll `kaldb_snapshot_manifest`, detect a new manifest id, and only download the delta relative to what they already have (mostly SSTs). Metadata and CURRENT follow the manifest instructions, and compaction + RocksDB reopen happen after the copy finishes.

Pointer example (`kaldb_snapshot_manifest`):
```json
{
  "version": 7,
  "latestManifestId": "manifest_0042",
  "snapshotManifestUri": "s3://bucket/db-prefix/manifest_0042/kaldb_db_manifest.json",
  "previous": [
    {"manifestId": "manifest_0041", "publishedAt": "2024-11-08T02:11:33Z"},
    {"manifestId": "manifest_0040", "publishedAt": "2024-11-08T01:41:05Z"}
  ]
}
```

Snapshot-level manifest example (`kaldb_db_manifest.json` stored inside `manifest_0042/`):
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

### Scheduling & Process Separation
- Run writer and reader as separate JVM processes (new module/package to keep isolation).
- Use a scheduled executor or simple `TimerTask` for the 30-second backup/restore loops.
- Provide configuration via env vars or a small YAML:
  - RocksDB paths.
  - S3 bucket/prefix.
  - Backup interval.

### Test Harness
- JUnit test that:
  1. Starts the writer service (embedded server) with temp directories.
  2. Starts the reader service pointing at the same bucket (but separate local dirs).
  3. Issues `POST /kv` to the writer with a random key/value.
  4. Waits long enough for at least one backup/restore cycle (~35–40 s or uses hooks to trigger immediately).
  5. Calls `GET /kv` on the reader until the key appears; asserts the value matches.
- The test can stub S3 by pointing both services at the same local filesystem to keep the prototype hermetic (e.g., use the S3Mock/MinIO already in CI) or rely on a running MinIO instance in dev.

## Open Questions / Future Work
- **Retention**: when/how to delete old backups (S3 lifecycle vs. `purgeOldBackups`).
- **Delta ingestion**: this prototype copies immutable SSTs directly into the live reader directory; finer-grained reconciliation/GC still open.
- **Auth/config**: right now we assume static credentials (`minioadmin`). Add configurable AWS creds/regions later.
- **Error handling**: barebones for now; productionizing would require retries, metrics, etc.

## Next Steps
1. Scaffold a new module/package (e.g., `proto/rocksdb-sync`) containing writer/reader servers and shared utilities (S3 client, backup helpers).
2. Implement the writer service with RocksDB checkpoint creation + manifest-prefix uploads + `kaldb_snapshot_manifest` pointer writes.
3. Implement the reader service with incremental snapshot syncing: poll pointer, download missing files under the target manifest prefix, apply `CURRENT` last, then reopen/compact in-place.
4. Add the integration test described above (can reuse MinIO from the existing test infrastructure).
5. Document how to run both services locally (MinIO command, Maven goals, HTTP examples).

## Running the Prototype
1. Start MinIO (or point to an S3 bucket) and ensure the bucket referenced by `S3_BUCKET` exists.
2. Writer:
   ```bash
   export WRITER_DB_PATH=~/temp/rocksdb-writer
   export WRITER_SNAPSHOT_PATH=~/temp/snapshot-writer
   export S3_BUCKET=test-bucket
   export S3_PREFIX=rocksdb-sync-poc
   export S3_ENDPOINT=http://localhost:9000
   export AWS_ACCESS_KEY_ID=minioadmin
   export AWS_SECRET_ACCESS_KEY=minioadmin
   mvn -pl rocksdb-sync-poc exec:java -Dexec.mainClass=com.slack.rocksdbsync.writer.WriterMain
   ```
3. Reader (in a second terminal):
   ```bash
   export READER_DB_PATH=~/temp/rocksdb-reader
   export S3_BUCKET=test-bucket
   export S3_PREFIX=rocksdb-sync-poc
   export S3_ENDPOINT=http://localhost:9000
   export AWS_ACCESS_KEY_ID=minioadmin
   export AWS_SECRET_ACCESS_KEY=minioadmin
   mvn -pl rocksdb-sync-poc exec:java -Dexec.mainClass=com.slack.rocksdbsync.reader.ReaderMain
   ```
4. Use `curl` to exercise the API:
   ```bash
   curl -XPOST localhost:8081/kv -d '{"key":"foo","value":"bar"}'
   curl 'localhost:8082/kv?key=foo'
   ```

## Load Generator
For large-scale validation there is a dedicated load generator entrypoint:

```bash
# or simply run `make loadgen-full`
LOADGEN_WRITER_URL=${LOADGEN_WRITER_URL:-http://localhost:8081/kv} \
LOADGEN_READER_URL=${LOADGEN_READER_URL:-http://localhost:8082} \
LOADGEN_TOTAL_BATCHES=10 \
LOADGEN_BATCH_BYTES=$((1024*1024*1024)) \
mvn -pl rocksdb-sync-poc exec:java -Dexec.mainClass=com.slack.rocksdbsync.loadgen.LoadGeneratorMain
```

Environment knobs:

| Variable | Default | Description |
| --- | --- | --- |
| `LOADGEN_WRITER_URL` | `http://localhost:8081/kv` | Writer HTTP endpoint. |
| `LOADGEN_READER_URL` | `http://localhost:8082` | Base reader URL (no `/kv`). |
| `LOADGEN_BATCH_BYTES` | `1_073_741_824` | Bytes per batch (1 GiB). |
| `LOADGEN_TOTAL_BATCHES` | `10` | Number of batches (10 GiB total). |
| `LOADGEN_KEY_SIZE_BYTES` | `32` | Key size padding. |
| `LOADGEN_VALUE_SIZE_BYTES` | `1024` | Value size padding. |
| `LOADGEN_KEYS_PER_BATCH` | unset | Force a fixed number of keys per batch (overrides `LOADGEN_BATCH_BYTES`). |
| `LOADGEN_WRITER_CONCURRENCY` | `8` | Concurrent writer POSTs. |
| `LOADGEN_READER_CONCURRENCY` | `8` | Concurrent reader GETs during validation. |

The tool writes deterministic keys for each batch, waits for the reader to ingest, and then validates that every key/value pair can be read back through the reader HTTP API.

**Smoke Test (100 keys total)**

```bash
# or run `make loadgen-smoke`
LOADGEN_TOTAL_BATCHES=1 \
LOADGEN_KEYS_PER_BATCH=100 \
LOADGEN_KEY_SIZE_BYTES=32 \
LOADGEN_VALUE_SIZE_BYTES=1024 \
mvn -pl rocksdb-sync-poc exec:java -Dexec.mainClass=com.slack.rocksdbsync.loadgen.LoadGeneratorMain
```
