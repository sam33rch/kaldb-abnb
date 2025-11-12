package com.slack.rocksdbsync;

import java.nio.file.Path;
import java.util.Objects;
import org.rocksdb.CloudEnvOptions;

public record CloudConfig(
    String bucketName,
    String objectPath,
    String region,
    String accessKey,
    String secretKey,
    String cookie,
    boolean keepLocalSstFiles,
    boolean keepLocalLogFiles,
    boolean resyncOnOpen,
    Path localDbPath,
    boolean isWriter) {

  private static String requireEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing environment variable: " + name);
    }
    return value;
  }

  public static CloudConfig fromEnv(String defaultDbPath, boolean isWriter) {
    Objects.requireNonNull(defaultDbPath, "defaultDbPath");
    String bucket = requireEnv("CLOUD_BUCKET");
    String objectPath = System.getenv().getOrDefault("CLOUD_OBJECT_PATH", "rocksdb-cloud-poc");
    String region = System.getenv().getOrDefault("CLOUD_REGION", "us-east-1");
    String accessKey = requireEnv("AWS_ACCESS_KEY_ID");
    String secretKey = requireEnv("AWS_SECRET_ACCESS_KEY");
    String cookie = System.getenv().getOrDefault("CLOUD_COOKIE", "cloud-poc");
    boolean keepSst =
        Boolean.parseBoolean(System.getenv().getOrDefault("KEEP_LOCAL_SST_FILES", "true"));
    boolean keepLog =
        Boolean.parseBoolean(System.getenv().getOrDefault("KEEP_LOCAL_LOG_FILES", "true"));
    boolean resyncOnOpen =
        Boolean.parseBoolean(System.getenv().getOrDefault("RESYNC_ON_OPEN", "false"));
    Path dbPath = Path.of(System.getenv().getOrDefault("LOCAL_DB_PATH", defaultDbPath));
    return new CloudConfig(bucket, objectPath, region, accessKey, secretKey, cookie, keepSst,
        keepLog, resyncOnOpen, dbPath, isWriter);
  }

  public CloudEnvOptions toCloudEnvOptions() {
    CloudEnvOptions.Builder builder = CloudEnvOptions.newBuilder()
        .setSrcBucketName(bucketName)
        .setSrcObjectPath(objectPath)
        .setSrcRegion(region)
        .setDestBucketName(bucketName)
        .setDestObjectPath(objectPath)
        .setDestRegion(region)
        .setAccessKeyId(accessKey)
        .setSecretAccessKey(secretKey)
        .setKeepLocalSstFiles(keepLocalSstFiles)
        .setKeepLocalLogFiles(keepLocalLogFiles)
        .setResyncOnOpen(resyncOnOpen)
        .setCookieOnOpen(cookie);

    // Writers roll manifest on open (needed to upload CLOUDMANIFEST to S3)
    // Readers don't roll to avoid creating new epochs
    if (isWriter) {
      builder.setRollManifestOnOpen(true);
      builder.setNewCookieOnOpen(cookie);
    } else {
      builder.setRollManifestOnOpen(false);
    }

    return builder.build();
  }
}
