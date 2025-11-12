package com.slack.rocksdbsync.reader;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.rocksdb.CloudEnvOptions;
import org.rocksdb.DBCloud;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReaderService {
  private static final Logger LOG = LoggerFactory.getLogger(ReaderService.class);
  private static final byte[] CHECKPOINT_KEY = "__checkpoint__".getBytes(StandardCharsets.UTF_8);

  static {
    RocksDB.loadLibrary();
  }

  private final ReaderConfig config;

  public ReaderService(ReaderConfig config) {
    this.config = config;
  }

  public void runOnce() {
    CloudEnvOptions cloudOptions = config.cloud().toCloudEnvOptions();
    Options options = new Options().setCreateIfMissing(true);
    try {
      Files.createDirectories(config.cloud().localDbPath());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create reader directory", e);
    }
    LOG.info("Opening reader database at {}", config.cloud().localDbPath());
    try (DBCloud db =
        DBCloud.open(
            options,
            config.cloud().localDbPath().toString(),
            cloudOptions,
            "",
            0,
            true)) {
      byte[] checkpointBytes = db.get(CHECKPOINT_KEY);
      if (checkpointBytes == null) {
        throw new IllegalStateException("Checkpoint key not found in cloud database");
      }
      long checkpoint = Long.parseLong(new String(checkpointBytes, StandardCharsets.UTF_8));
      LOG.info("Checkpoint recorded at {}", checkpoint);

      List<String> missing = new ArrayList<>();
      for (long i = 0; i < checkpoint; i++) {
        byte[] key = ("key-%03d".formatted(i)).getBytes(StandardCharsets.UTF_8);
        byte[] value = db.get(key);
        if (value == null) {
          missing.add(new String(key, StandardCharsets.UTF_8));
        }
      }

      if (missing.isEmpty()) {
        LOG.info("Reader verified {} keys from MinIO via RocksDB-Cloud ✔", checkpoint);
      } else {
        LOG.error("Reader is missing {} keys: {}", missing.size(), missing);
        throw new IllegalStateException("Reader verification failed");
      }
    } catch (RocksDBException e) {
      throw new IllegalStateException("Reader failed", e);
    }
  }
}
