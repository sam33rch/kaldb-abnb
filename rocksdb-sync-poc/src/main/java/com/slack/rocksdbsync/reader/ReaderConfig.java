package com.slack.rocksdbsync.reader;

import com.slack.rocksdbsync.CloudConfig;
import java.nio.file.Path;

public record ReaderConfig(
    CloudConfig cloud,
    int httpPort,
    Path primaryDbPath,
    boolean enablePeriodicResync,
    long resyncIntervalSeconds) {
  public static ReaderConfig fromEnv() {
    CloudConfig cloud = CloudConfig.fromEnv(
        System.getProperty("user.home") + "/temp/rocksdb-cloud-reader", false); // isWriter=false
    int port = parsePort(System.getenv("READER_HTTP_PORT"), 8082);
    // In production, reader has its own local path and syncs from cloud only
    // primaryDbPath = reader's local cache of files downloaded from MinIO
    Path primaryPath = cloud.localDbPath();
    boolean enableResync =
        Boolean.parseBoolean(System.getenv().getOrDefault("READER_ENABLE_PERIODIC_RESYNC", "false"));
    long resyncInterval = parseLong(System.getenv("READER_RESYNC_INTERVAL_SECONDS"), 5);
    return new ReaderConfig(cloud, port, primaryPath, enableResync, resyncInterval);
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
