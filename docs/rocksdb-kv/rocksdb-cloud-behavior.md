# RocksDB-Cloud Behavior Notes

This document summarizes several behaviors of the `rocksdb-cloud` fork that are relevant when running KalDB components against cloud-backed RocksDB instances.

## Secondary Catch-Up Model

* A secondary (a.k.a. follower) database opened via `DB::OpenAsSecondary` never auto-synchronizes. You must call `DB::TryCatchUpWithPrimary()` whenever you need the secondary to ingest MANIFEST/WAL updates from the primary. (See `rocksdb-cloud/include/rocksdb/db.h:235-298` and the implementation in `rocksdb-cloud/db/db_impl/db_impl_secondary.cc:672-744`.)
  * `TryCatchUpWithPrimary()` replays new MANIFEST edits, scans the WAL directory for unseen logs, installs new `SuperVersion`s, and then runs RocksDB’s normal obsolete-file cleanup. This means stale files are purged after each catch-up call, even on the secondary.
* There are no background threads watching for primary updates. Typical deployments call `TryCatchUpWithPrimary()` either on a timer or just-in-time before serving reads that must observe the latest state (see `rocksdb-cloud/examples/multi_processes_example.cc:305-334`).

## Cloud Storage & Local SST Cache

* Cloud access is mediated by `CloudFileSystemImpl`. When `CloudFileSystemOptions.keep_local_sst_files=true`, SSTs and MANIFESTs are always opened from the local filesystem. On a miss, RocksDB downloads the object from S3/GCS into the same path and retries the local open (`rocksdb-cloud/cloud/cloud_file_system_impl.cc:173-258`).
* With `keep_local_sst_files=false`, SST reads bypass local disk and stream directly from the cloud provider via `NewCloudReadableFile`. There is no automatic pinning to disk in this mode.
* Regardless of the mode, file deletion requests that come out of compaction/catch-up remove both the cloud copy and the local copy, so disk usage tracks the size of the *live* SST set (`rocksdb-cloud/cloud/cloud_file_system_impl.cc:700-739`).
* mmap reads (`Options.allow_mmap_reads`) are allowed only when `keep_local_sst_files=true`; otherwise `CloudFileSystemImpl::CheckOption` rejects the open because there is no local file to map (`rocksdb-cloud/cloud/cloud_file_system_impl.cc:1063-1070`).

## mmap vs. Direct/Buffered Reads

* Setting `Options.allow_mmap_reads=true` causes `Env::NewRandomAccessFile` to map SST files via `PosixMmapReadableFile`, returning slices backed directly by the OS page cache (`rocksdb-cloud/env/fs_posix.cc:239-275`, `rocksdb-cloud/env/io_posix.cc:936-1010`).
* mmap mode disables RocksDB’s `FilePrefetchBuffer` because the kernel already handles paging; sequential scans therefore rely on `madvise` hints rather than RocksDB-managed read-ahead (`rocksdb-cloud/file/file_prefetch_buffer.h:179-205`, `rocksdb-cloud/table/block_based/block_based_table_reader.cc:2598-2607`).
* The OS decides which pages stay resident. Long scans in mmap mode can evict other data from the page cache just like large `pread()` workloads would. To avoid polluting the OS cache you can:
  * Leave `allow_mmap_reads=false` (default) and rely on the block cache with `ReadOptions.fill_cache=false` for bulk scans.
  * Enable `Options.use_direct_reads=true` (mutually exclusive with mmap) to bypass the page cache entirely.

## Caching Without mmap

* The block cache (configured via `BlockBasedTableOptions::block_cache`) stores frequently accessed data/index/filter blocks. Every point lookup checks the cache first and inserts the block after reading from disk when `ReadOptions.fill_cache=true` (`rocksdb-cloud/include/rocksdb/table.h:256-289`, `rocksdb-cloud/table/block_based/block_based_table_reader.cc:1616-1689`).
* For extremely hot individual keys, RocksDB also supports a `RowCache` (`Options.row_cache`). When enabled, fully decoded key/value pairs are cached and future reads bypass SST access entirely (`rocksdb-cloud/include/rocksdb/options.h:1294-1315`).
* Recent writes live in mutable or immutable memtables, so reads that hit data still in memory never touch SSTs. This behavior is orthogonal to mmap and provides an implicit cache for the newest keys.

These behaviors should help when diagnosing secondary staleness, cloud-storage disk usage, or caching-related performance issues in KalDB deployments.
