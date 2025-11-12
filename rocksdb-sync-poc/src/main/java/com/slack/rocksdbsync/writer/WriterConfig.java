package com.slack.rocksdbsync.writer;

import com.slack.rocksdbsync.CloudConfig;

public record WriterConfig(
    CloudConfig cloud,
    int httpPort,
    boolean flushOnWrite,
    boolean enablePeriodicFlush,
    long flushIntervalSeconds) {
  public static WriterConfig fromEnv() {
    CloudConfig cloud = CloudConfig.fromEnv("/tmp/rocksdb-cloud-writer", true); // isWriter=true
    int port = parsePort(System.getenv("WRITER_HTTP_PORT"), 8081);
    boolean flushOnWrite =
        Boolean.parseBoolean(System.getenv().getOrDefault("WRITER_FLUSH_ON_WRITE", "false"));
    boolean enablePeriodicFlush =
        Boolean.parseBoolean(System.getenv().getOrDefault("WRITER_ENABLE_PERIODIC_FLUSH", "true"));
    long flushIntervalSeconds = parseLong(System.getenv("WRITER_FLUSH_INTERVAL_SECONDS"), 10);
    return new WriterConfig(cloud, port, flushOnWrite, enablePeriodicFlush, flushIntervalSeconds);
  }

  private static int parsePort(String raw, int defaultPort) {
    if (raw == null || raw.isBlank()) {
      return defaultPort;
    }
    try {
      int port = Integer.parseInt(raw);
      if (port <= 0 || port > 65535) {
        return defaultPort;
      }
      return port;
    } catch (NumberFormatException e) {
      return defaultPort;
    }
  }

  private static long parseLong(String raw, long defaultValue) {
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(raw);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
