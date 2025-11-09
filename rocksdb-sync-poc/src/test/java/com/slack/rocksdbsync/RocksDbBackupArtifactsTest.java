package com.slack.rocksdbsync;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupEngineOptions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDbBackupArtifactsTest {
  private static final Logger LOG = LoggerFactory.getLogger(RocksDbBackupArtifactsTest.class);
  private static final long ONE_GIB = 1L << 30;
  private static final int VALUE_SIZE = 1024; // 1 KiB values

  static {
    RocksDB.loadLibrary();
  }

  @Test
  void incrementalBackupsProduceArtifacts(@TempDir Path tempDir) throws Exception {
    Path dbPath = tempDir.resolve("db");
    Path backupPath =
        System.getProperty("backup.dir") != null
            ? Path.of(System.getProperty("backup.dir"))
            : Path.of(System.getProperty("user.home"), "temp", "rocksdb-backups");
    Files.createDirectories(dbPath);
    Files.createDirectories(backupPath);

    try (Options options = new Options().setCreateIfMissing(true);
        RocksDB db = RocksDB.open(options, dbPath.toString());
        BackupEngineOptions backupOptions = new BackupEngineOptions(backupPath.toString());
        BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), backupOptions)) {

      writeGigabyte(db, 0);
      createBackup(backupEngine, db, backupPath, 1);

      writeGigabyte(db, 1);
      createBackup(backupEngine, db, backupPath, 2);

      writeGigabyte(db, 2);
      createBackup(backupEngine, db, backupPath, 3);
    }
  }

  private void createBackup(
      BackupEngine backupEngine, RocksDB db, Path backupPath, int generation) throws Exception {
    long start = System.nanoTime();
    backupEngine.createNewBackup(db, true);
    long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
    LOG.info("Backup {} created in {}s. Listing artifacts:", generation, seconds);
    Files.walk(backupPath)
        .filter(Files::isRegularFile)
        .forEach(
            path -> {
              try {
                LOG.info(
                    "  {} ({} bytes)",
                    backupPath.relativize(path),
                    Files.size(path));
              } catch (Exception e) {
                LOG.warn("Failed to stat {}", path, e);
              }
            });
  }

  private void writeGigabyte(RocksDB db, int batch) throws Exception {
    long records = ONE_GIB / VALUE_SIZE;
    byte[] value = new byte[VALUE_SIZE];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) ('a' + batch);
    }

    for (long i = 0; i < records; i++) {
      byte[] key = ("batch-" + batch + "-k-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
      db.put(key, value);
      if ((i + 1) % 100_000 == 0) {
        LOG.info("Batch {} progress: {}/{}", batch, i + 1, records);
      }
    }
  }
}
