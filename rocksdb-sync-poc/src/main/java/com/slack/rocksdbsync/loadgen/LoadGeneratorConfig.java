package com.slack.rocksdbsync.loadgen;

import java.net.URI;

public record LoadGeneratorConfig(
    URI writerUri,
    URI readerUri,
    long batchBytes,
    long keysPerBatch,
    int totalBatches,
    int keySizeBytes,
    int valueSizeBytes,
    int writerConcurrency,
    int readerConcurrency) {

  static final long ONE_GIB = 1_073_741_824L;

  public static LoadGeneratorConfig fromEnv() {
    String writerUrl = getenvOrDefault("LOADGEN_WRITER_URL", "http://localhost:8081/kv");
    String readerUrl = getenvOrDefault("LOADGEN_READER_URL", "http://localhost:8082");

    long batchSize = getLong("LOADGEN_BATCH_BYTES", ONE_GIB);
    int batches = (int) getLong("LOADGEN_TOTAL_BATCHES", 10);
    int keySize = (int) getLong("LOADGEN_KEY_SIZE_BYTES", 32);
    int valueSize = (int) getLong("LOADGEN_VALUE_SIZE_BYTES", 1024);
    long keysPerBatch = getLong("LOADGEN_KEYS_PER_BATCH", -1);
    int writerConc = (int) getLong("LOADGEN_WRITER_CONCURRENCY", 8);
    int readerConc = (int) getLong("LOADGEN_READER_CONCURRENCY", 8);
    return new LoadGeneratorConfig(
        URI.create(writerUrl),
        URI.create(readerUrl),
        batchSize,
        keysPerBatch,
        batches,
        keySize,
        valueSize,
        writerConc,
        readerConc);
  }

  private static String getenvOrDefault(String key, String def) {
    String value = System.getenv(key);
    return (value == null || value.isBlank()) ? def : value;
  }

  private static long getLong(String key, long def) {
    String value = System.getenv(key);
    if (value == null || value.isBlank()) {
      return def;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid number for " + key + ": " + value, e);
    }
  }
}
