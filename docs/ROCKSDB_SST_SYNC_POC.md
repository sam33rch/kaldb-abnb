# RocksDB SST Sync Prototype – Working Plan

## Goals
- Demonstrate end-to-end “writer → S3 backup → reader restore” flow using actual HTTP servers.
- Keep the prototype self-contained (no reuse of existing kaldb services); everything lives in a new package/module.
- Use RocksDB’s built-in `BackupEngine` for incremental backups/restores instead of the ad-hoc manifest logic from the tests.
- Provide a simple test harness that issues writes through the writer HTTP API and reads them back via the reader HTTP API.

## Components

### Writer Service
- Java HTTP server (Simple `HttpServer`) exposing:
  - `POST /kv` with JSON body `{ "key": "...", "value": "..." }`.
  - `GET /healthz` for smoke checks.
- Maintains a local RocksDB instance (e.g., `/var/tmp/writer-db`).
- Every `POST` writes the key/value to RocksDB and responds 202.
- Every 30 seconds (scheduler):
  1. Flush + compact the DB, then create a RocksDB checkpoint in a temp directory (produces a self-consistent view).
  2. Assign a new manifest id (e.g., `manifest_0042`) and upload every file from the checkpoint to `s3://<bucket>/<db-prefix>/<manifest_id>/...` using `If-None-Match "*"` so immutable SSTs never re-upload.
  3. After all non-pointer files succeed (HTTP 200 or 412), upload the snapshot’s `CURRENT`.
  4. Update the canonical `db_manifest` object once the prefix is fully present; readers treat that as the commit point.

### Reader Service
- Java HTTP server exposing `GET /kv?key=...` and `GET /healthz`.
- Keeps two directories:
  - `active-db/` serving traffic.
  - `staging/` used to download the S3 snapshot safely.
- Every 30 seconds:
  1. Fetch `db_manifest`, compare `latestManifestId` to the one currently serving traffic.
  2. If a newer manifest exists, enumerate `s3://<bucket>/<db-prefix>/<manifest_id>/` and download only the files missing locally (mostly new SSTs). Leave `CURRENT` for last.
  3. After the download succeeds, acquire the write lock, close RocksDB, swap `staging/` → `active-db/`, run a compaction, and reopen for reads.
- `GET /kv` reads from the reopened RocksDB instance.

### S3 Layout (Updated Decision)
- Each snapshot lives under its own prefix: `s3://<bucket>/<db-prefix>/manifest_<NNNN>/...` and contains the full RocksDB checkpoint (all `.sst`, `MANIFEST-*`, `IDENTITY`, etc.).
- A canonical pointer `s3://<bucket>/<db-prefix>/db_manifest` (JSON) carries the latest manifest id plus small history. Writers rewrite this object only after the snapshot upload succeeds.
- Upload order: (1) generate checkpoint, (2) PUT every file inside the manifest prefix using `If-None-Match "*"`, (3) once every upload returns 200/412, PUT the snapshot-local `CURRENT`, and finally (4) update `db_manifest`.
- Readers poll `db_manifest`, detect a new manifest id, and only download the delta relative to what they already have (mostly SSTs). Metadata and CURRENT come last, and compaction runs after the swap.

Pointer example:
```json
{
  "version": 7,
  "latestManifestId": "manifest_0042",
  "previous": [
    {"manifestId": "manifest_0041", "publishedAt": "2024-11-08T02:11:33Z"},
    {"manifestId": "manifest_0040", "publishedAt": "2024-11-08T01:41:05Z"}
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
- **Delta ingestion**: this prototype swaps entire DB directories. Incremental SST ingestion could follow later.
- **Auth/config**: right now we assume static credentials (`minioadmin`). Add configurable AWS creds/regions later.
- **Error handling**: barebones for now; productionizing would require retries, metrics, etc.

## Next Steps
1. Scaffold a new module/package (e.g., `proto/rocksdb-sync`) containing writer/reader servers and shared utilities (S3 client, backup helpers).
2. Implement the writer service with RocksDB checkpoint creation + manifest-prefix uploads + `db_manifest` pointer writes.
3. Implement the reader service with incremental snapshot syncing: poll pointer, download missing files under the target manifest prefix, stage CURRENT last, then swap directories and compact.
4. Add the integration test described above (can reuse MinIO from the existing test infrastructure).
5. Document how to run both services locally (MinIO command, Maven goals, HTTP examples).

## Running the Prototype
1. Start MinIO (or point to an S3 bucket) and ensure the bucket referenced by `S3_BUCKET` exists.
2. Writer:
   ```bash
   export WRITER_DB_PATH=/tmp/writer-db
   export S3_BUCKET=test-bucket
   export S3_PREFIX=rocksdb-sync-poc
   export S3_ENDPOINT=http://localhost:9000
   export AWS_ACCESS_KEY_ID=minioadmin
   export AWS_SECRET_ACCESS_KEY=minioadmin
   mvn -pl rocksdb-sync-poc exec:java -Dexec.mainClass=com.slack.rocksdbsync.writer.WriterMain
   ```
3. Reader (in a second terminal):
   ```bash
   export READER_DB_PATH=/tmp/reader-db
   export READER_STAGING_PATH=/tmp/reader-staging
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
