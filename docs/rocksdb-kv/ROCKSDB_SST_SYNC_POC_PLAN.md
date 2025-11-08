# RocksDB SST File Sync - Manual Step-by-Step Test Plan

## Overview
Create a simple test that manually walks through each step of the SST sync workflow between two RocksDB instances via S3.

## Test Structure

**Test file location:**
- `astra/src/test/java/com/slack/astra/logstore/rocksdb/RocksDbSstSyncPrototypeTest.java`

## Test Steps (Sequential, Manual)

### Step 1: Writer - Create DB and SSTs
- Create RocksDB instance in temp directory (writerDbPath)
- Write batch of key-value pairs (e.g., 1000 records)
- Call `db.flush()` to force SST creation
- Close the DB
- Verify SST files and MANIFEST files exist in directory

### Step 2: Upload to S3Mock
- Upload the entire RocksDB directory to S3Mock bucket
- Verify files are in S3 (list objects)

### Step 3: Reader - Download from S3
- Download SST files and MANIFEST from S3 to local staging directory
- Verify files downloaded correctly

### Step 4: Reader - Load SSTs
- Create new RocksDB instance in separate temp directory (readerDbPath)
- Use `ingestExternalFile()` to load the downloaded SSTs
- Optionally run compaction

### Step 5: Verify
- Iterate through reader DB and verify all keys/values match expected data
- Print stats (key count, etc.)

### Step 6: Repeat (Second Batch)
- Reopen writer DB (same directory)
- Write another batch of different key-value pairs
- Flush, upload new SSTs to S3
- Reader ingests the new SSTs
- Verify reader now has both batches

## Implementation Details

### Infrastructure to Use
- MinIO running manually in Docker (localhost:9000)
- `@TempDir` for RocksDB directories
- AWS S3 client configured to point to MinIO endpoint
- Simple puts/gets, no complex logic

**Run MinIO with:**
```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

### Key RocksDB APIs
- `RocksDB.open(options, path)` - open/create database
- `db.put(key, value)` - write data
- `db.flush(new FlushOptions())` - force memtable to SST
- `db.ingestExternalFile(List<String> files, IngestExternalFileOptions options)` - load external SSTs
- `db.compactRange()` - manual compaction
- `db.get(key)` - read data
- `db.newIterator()` - iterate all keys

### Design Principles
- No threading initially
- Manual step-by-step execution
- Lots of logging/assertions to verify each step
- Easy to debug and understand
- Minimal dependencies (use what's already in the project)

## Implementation Directive

**Optimize for prototype success and simplicity.**

Apply clean code principles:
- Clear, descriptive names
- Small, focused methods
- Self-documenting code
- Minimal abstractions
- No premature optimization
- No production-level error handling

## Dependencies

### Already Available
- RocksDB 8.8.1 (rocksdbjni)
- JUnit 5 with modern annotations
- AWS S3 CRT Client for uploads/downloads

### External Requirements
- Docker (to run MinIO manually)

## Manifest Rolling Scheme (Prototype)

To experiment with S3 prefix entropy we will mirror each RocksDB flush/compaction into a hashed `db-xx` prefix (00–ff) and maintain a small pointer file:

```
s3://<bucket>/db_manifest.json               # canonical manifest pointer
s3://<bucket>/db-9c/MANIFEST-000123          # real RocksDB manifest + SSTs
```

`db_manifest.json` is a single object that carries the latest pointer plus a short rollback history:

```json
{
  "version": 58,
  "latest": {
    "prefix": "db-9c",
    "updatedAt": "2024-11-07T06:00:05Z"
  },
  "previous": [
    {
      "prefix": "db-02",
      "updatedAt": "2024-11-07T05:15:44Z"
    },
    {
      "prefix": "db-b7",
      "updatedAt": "2024-11-07T05:00:12Z"
    }
  ]
}
```

- `latest` points to the prefix containing the newest MANIFEST/SST set and carries the timestamp when it was published.
- `previous` keeps a newest→oldest list of fallback prefixes with their timestamps so readers can roll back one or more batches if ingestion fails.
- Writers overwrite `db_manifest.json` after each upload. Readers poll that object, compare ETags, and jump to the indicated `db-xx` prefix.
