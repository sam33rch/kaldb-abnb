package com.slack.rocksdbsync.reader;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

public record ReaderConfig(
    Path dbPath,
    String bucket,
    String prefix,
    String endpoint,
    String region,
    String accessKey,
    String secretKey,
    int httpPort,
    Duration restoreInterval) {

  public static ReaderConfig fromEnv() {
    return new Builder().fromEnv().build();
  }

  public static final class Builder {
    private Path dbPath;
    private String bucket;
    private String prefix = "";
    private String endpoint;
    private String region = "us-east-1";
    private String accessKey;
    private String secretKey;
    private int httpPort = 8082;
    private Duration restoreInterval = Duration.ofSeconds(30);

    public Builder fromEnv() {
      Optional.ofNullable(System.getenv("READER_DB_PATH")).ifPresent(value -> dbPath = Path.of(value));
      Optional.ofNullable(System.getenv("S3_BUCKET")).ifPresent(value -> bucket = value);
      Optional.ofNullable(System.getenv("S3_PREFIX")).ifPresent(value -> prefix = value);
      Optional.ofNullable(System.getenv("S3_ENDPOINT")).ifPresent(value -> endpoint = value);
      Optional.ofNullable(System.getenv("S3_REGION")).ifPresent(value -> region = value);
      Optional.ofNullable(System.getenv("AWS_ACCESS_KEY_ID")).ifPresent(value -> accessKey = value);
      Optional.ofNullable(System.getenv("AWS_SECRET_ACCESS_KEY"))
          .ifPresent(value -> secretKey = value);
      Optional.ofNullable(System.getenv("READER_HTTP_PORT"))
          .ifPresent(value -> httpPort = Integer.parseInt(value));
      Optional.ofNullable(System.getenv("RESTORE_INTERVAL_SECONDS"))
          .ifPresent(value -> restoreInterval = Duration.ofSeconds(Long.parseLong(value)));
      return this;
    }

    public Builder dbPath(Path dbPath) {
      this.dbPath = dbPath;
      return this;
    }

    public Builder bucket(String bucket) {
      this.bucket = bucket;
      return this;
    }

    public Builder prefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder endpoint(String endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    public Builder region(String region) {
      this.region = region;
      return this;
    }

    public Builder credentials(String accessKey, String secretKey) {
      this.accessKey = accessKey;
      this.secretKey = secretKey;
      return this;
    }

    public Builder httpPort(int httpPort) {
      this.httpPort = httpPort;
      return this;
    }

    public Builder restoreInterval(Duration restoreInterval) {
      this.restoreInterval = restoreInterval;
      return this;
    }

    public ReaderConfig build() {
      if (dbPath == null) {
        dbPath = Path.of(System.getProperty("user.home"), "temp", "rocksdb-reader");
      }
      if (bucket == null || bucket.isBlank()) {
        throw new IllegalStateException("S3_BUCKET must be set");
      }
      return new ReaderConfig(
          dbPath,
          bucket,
          prefix == null ? "" : prefix,
          endpoint,
          region,
          accessKey,
          secretKey,
          httpPort,
          restoreInterval);
    }
  }
}
