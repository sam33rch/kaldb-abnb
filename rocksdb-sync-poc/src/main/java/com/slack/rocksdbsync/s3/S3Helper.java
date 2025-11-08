package com.slack.rocksdbsync.s3;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3Helper implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(S3Helper.class);

  private final S3Client client;
  private final String bucket;
  private final String prefix;

  public static Builder builder() {
    return new Builder();
  }

  private S3Helper(S3Client client, String bucket, String prefix) {
    this.client = client;
    this.bucket = bucket;
    String normalized = prefix == null ? "" : prefix.trim();
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    this.prefix = normalized;
  }

  public void uploadDirectory(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      LOG.warn("Directory {} does not exist, skipping upload.", directory);
      return;
    }
    try (Stream<Path> stream = Files.walk(directory)) {
      stream
          .filter(Files::isRegularFile)
          .forEach(
              path -> {
                String relative = directory.relativize(path).toString().replace("\\", "/");
                String key = resolveKey(relative);
                try {
                  LOG.info("Uploading {} -> s3://{}/{}", path, bucket, key);
                  client.putObject(
                      PutObjectRequest.builder().bucket(bucket).key(key).build(),
                      RequestBody.fromFile(path));
                } catch (Exception e) {
                  throw new RuntimeException("Failed to upload " + path + " to S3", e);
                }
              });
    }
  }

  public enum UploadResult {
    CREATED,
    ALREADY_EXISTS
  }

  public record ObjectContent(byte[] data, String eTag) {}

  public record PutResult(String eTag) {}

  public UploadResult uploadFile(Path source, String relativeKey, boolean onlyIfAbsent)
      throws IOException {
    String key = resolveKey(relativeKey);
    PutObjectRequest.Builder builder =
        PutObjectRequest.builder().bucket(bucket).key(key);
    if (onlyIfAbsent) {
      builder.ifNoneMatch("*");
    }
    try {
      client.putObject(builder.build(), RequestBody.fromFile(source));
      return UploadResult.CREATED;
    } catch (S3Exception e) {
      if (onlyIfAbsent && e.statusCode() == 412) {
        LOG.debug("Skipping existing S3 object {}", key);
        return UploadResult.ALREADY_EXISTS;
      } else {
        throw new IOException("Failed to upload " + source + " to S3", e);
      }
    }
  }

  public PutResult putObjectBytes(
      String relativeKey, byte[] data, String ifMatch, boolean ifNoneMatch) throws IOException {
    String key = resolveKey(relativeKey);
    PutObjectRequest.Builder builder =
        PutObjectRequest.builder().bucket(bucket).key(key);
    AwsRequestOverrideConfiguration.Builder override = null;
    if ((ifMatch != null && !ifMatch.isBlank()) || ifNoneMatch) {
      override = AwsRequestOverrideConfiguration.builder();
      if (ifMatch != null && !ifMatch.isBlank()) {
        override.putHeader("If-Match", ifMatch);
      }
      if (ifNoneMatch) {
        override.putHeader("If-None-Match", "*");
      }
      builder.overrideConfiguration(override.build());
    }
    try {
      PutObjectResponse response = client.putObject(builder.build(), RequestBody.fromBytes(data));
      return new PutResult(response.eTag());
    } catch (S3Exception e) {
      throw new IOException("Failed to upload bytes to " + key, e);
    }
  }

  public Optional<ObjectContent> getObject(String relativeKey) throws IOException {
    return getObject(relativeKey, null);
  }

  public Optional<ObjectContent> getObject(String relativeKey, String ifNoneMatchEtag)
      throws IOException {
    String key = resolveKey(relativeKey);
    GetObjectRequest.Builder builder =
        GetObjectRequest.builder().bucket(bucket).key(key);
    if (ifNoneMatchEtag != null && !ifNoneMatchEtag.isBlank()) {
      builder.ifNoneMatch(ifNoneMatchEtag);
    }
    try (ResponseInputStream<GetObjectResponse> stream =
        client.getObject(builder.build())) {
      byte[] data = stream.readAllBytes();
      return Optional.of(new ObjectContent(data, stream.response().eTag()));
    } catch (S3Exception e) {
      if (e.statusCode() == 304) {
        return Optional.empty();
      }
      if (e.statusCode() == 404) {
        return Optional.empty();
      }
      throw new IOException("Failed to fetch s3://" + bucket + "/" + key, e);
    }
  }

  public void downloadObject(String relativeKey, Path destination) throws IOException {
    String key = resolveKey(relativeKey);
    Files.createDirectories(destination.getParent());
    GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
    try (ResponseInputStream<GetObjectResponse> stream = client.getObject(request)) {
      Files.copy(stream, destination, StandardCopyOption.REPLACE_EXISTING);
    } catch (S3Exception e) {
      throw new IOException("Failed to download s3://" + bucket + "/" + key, e);
    }
  }

  public String buildS3Uri(String relativeKey) {
    StringBuilder builder = new StringBuilder("s3://").append(bucket).append("/");
    if (!prefix.isEmpty()) {
      builder.append(prefix).append("/");
    }
    builder.append(relativeKey);
    return builder.toString();
  }

  public String relativeKeyFromUri(String s3Uri) {
    URI uri = URI.create(s3Uri);
    if (!"s3".equalsIgnoreCase(uri.getScheme())) {
      throw new IllegalArgumentException("Unsupported URI scheme for " + s3Uri);
    }
    String uriBucket = uri.getHost();
    if (uriBucket == null || uriBucket.isBlank()) {
      throw new IllegalArgumentException("Missing bucket in " + s3Uri);
    }
    if (!uriBucket.equals(bucket)) {
      throw new IllegalArgumentException(
          "Snapshot references bucket "
              + uriBucket
              + " but helper configured for "
              + bucket);
    }
    String key = uri.getPath();
    if (key.startsWith("/")) {
      key = key.substring(1);
    }
    if (prefix.isEmpty()) {
      return key;
    }
    if (key.startsWith(prefix + "/")) {
      return key.substring(prefix.length() + 1);
    }
    throw new IllegalArgumentException(
        "Snapshot key "
            + key
            + " does not reside under expected prefix "
            + prefix);
  }

  public void downloadToDirectory(Path destination) throws IOException {
    if (Files.exists(destination)) {
      deleteDirectory(destination);
    }
    Files.createDirectories(destination);

    List<S3Object> objects = listAllObjects();
    for (S3Object object : objects) {
      String key = object.key();
      if (key.endsWith("/")) {
        continue;
      }
      String relative = prefix.isEmpty() ? key : key.substring(prefix.length() + 1);
      Path targetPath = destination.resolve(relative);
      Files.createDirectories(targetPath.getParent());
      LOG.info("Downloading s3://{}/{} -> {}", bucket, key, targetPath);
      GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
      try (ResponseInputStream<?> stream = client.getObject(request)) {
        Files.copy(stream, targetPath);
      } catch (Exception e) {
        throw new IOException("Failed to download " + key, e);
      }
    }
  }

  public void deletePrefix() {
    List<S3Object> objects = listAllObjects();
    for (S3Object object : objects) {
      LOG.info("Deleting s3://{}/{}", bucket, object.key());
      client.deleteObject(
          DeleteObjectRequest.builder().bucket(bucket).key(object.key()).build());
    }
  }

  private List<S3Object> listAllObjects() {
    List<S3Object> results = new ArrayList<>();
    String continuation = null;
    AtomicBoolean done = new AtomicBoolean(false);
    while (!done.get()) {
      String listPrefix = prefix.isEmpty() ? "" : prefix + "/";
      ListObjectsV2Request.Builder builder =
          ListObjectsV2Request.builder().bucket(bucket).prefix(listPrefix).maxKeys(1000);
      if (continuation != null) {
        builder.continuationToken(continuation);
      }
      ListObjectsV2Response response = client.listObjectsV2(builder.build());
      results.addAll(response.contents());
      if (response.isTruncated()) {
        continuation = response.nextContinuationToken();
      } else {
        done.set(true);
      }
    }
    return results;
  }

  private static void deleteDirectory(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }
    try (Stream<Path> stream = Files.walk(directory)) {
      stream
          .sorted((a, b) -> b.compareTo(a))
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  throw new RuntimeException("Failed to delete " + path, e);
                }
              });
    }
  }

  @Override
  public void close() {
    client.close();
  }

  private String resolveKey(String relative) {
    return prefix.isEmpty() ? relative : prefix + "/" + relative;
  }

  public static final class Builder {
    private String bucket;
    private String prefix = "";
    private String endpoint;
    private Region region = Region.US_EAST_1;
    private String accessKey;
    private String secretKey;

    private Builder() {}

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
      this.region = Region.of(region);
      return this;
    }

    public Builder credentials(String accessKey, String secretKey) {
      this.accessKey = accessKey;
      this.secretKey = secretKey;
      return this;
    }

    public S3Helper build() {
      Objects.requireNonNull(bucket, "bucket must be set");
      S3ClientBuilder builder = S3Client.builder().region(region);
      if (endpoint != null && !endpoint.isEmpty()) {
        builder.endpointOverride(URI.create(endpoint)).forcePathStyle(true);
      }
      if (accessKey != null && secretKey != null) {
        builder.credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
      } else {
        builder.credentialsProvider(DefaultCredentialsProvider.create());
      }
      return new S3Helper(builder.build(), bucket, prefix);
    }
  }
}
