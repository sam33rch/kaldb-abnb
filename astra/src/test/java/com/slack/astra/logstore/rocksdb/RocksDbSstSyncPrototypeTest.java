package com.slack.astra.logstore.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.blobfs.S3TestUtils;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class RocksDbSstSyncPrototypeTest {
  private static final Logger LOG = LoggerFactory.getLogger(RocksDbSstSyncPrototypeTest.class);
  private static final String BUCKET_NAME = "test-bucket";
  private static final String MINIO_ENDPOINT = "http://localhost:9000";
  private static final String MANIFEST_KEY = "db_manifest.json";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final int VALUE_SIZE_BYTES = 1024;

  private S3AsyncClient s3Client = S3TestUtils.createS3CrtClient(MINIO_ENDPOINT);

  @Test
  void testStep1_writeDataAndCreateSsts(@TempDir Path writerPath) throws Exception {
    LOG.info("Step 1: Writing data and creating SSTs");

    try (Options options = new Options().setCreateIfMissing(true);
        RocksDB writerDb = RocksDB.open(options, writerPath.toString())) {

      writeRecords(writerDb, "key-", 1000);
      flushAndCompact(writerDb);
      LOG.info("Flushed data to SST files");

      List<File> sstFiles = listSstFiles(writerPath);
      assertThat(sstFiles).isNotEmpty();
      LOG.info("Created {} SST files", sstFiles.size());

      for (File sstFile : sstFiles) {
        LOG.info("  - {}", sstFile.getName());
      }
    }

    LOG.info("Step 1 completed successfully");
  }

  @Test
  void testStep2_uploadToMinio(@TempDir Path writerPath) throws Exception {
    LOG.info("Step 2: Upload RocksDB directory to MinIO");

    ensureBucketExists();

    try (Options options = new Options().setCreateIfMissing(true);
        RocksDB writerDb = RocksDB.open(options, writerPath.toString())) {

      writeRecords(writerDb, "key-", 10_000_000);
      flushAndCompact(writerDb);
    }

    String partitionPrefix = uploadDirectory(writerPath);

    LatestManifest manifest =
        readLatestManifest().orElseThrow(() -> new IllegalStateException("Manifest missing"));
    assertThat(manifest.latest.prefix).isEqualTo(partitionPrefix);
    assertThat(manifest.latest.updatedAt).isNotBlank();
    assertThat(manifest.previous).isNotNull();

    LOG.info("Step 2 completed successfully");
  }

  private void writeRecords(RocksDB db, String keyPrefix, int count) throws Exception {
    for (int i = 0; i < count; i++) {
      String key = keyPrefix + i;
      byte[] value = makeValueBytes(i);
      db.put(key.getBytes(StandardCharsets.UTF_8), value);
    }
    LOG.info("Wrote {} records with prefix '{}'", count, keyPrefix);
  }

  private String uploadDirectory(Path localPath) throws Exception {
    Instant batchTimestamp = Instant.now();
    String partitionPrefix = selectPartitionPrefix(batchTimestamp);
    String timestampComponent = buildTimestampComponent(batchTimestamp);
    String fullPrefix = partitionPrefix + "/" + timestampComponent;
    String normalizedPrefix = normalizePrefix(fullPrefix);

    List<File> files;
    try (Stream<Path> pathStream = Files.walk(localPath)) {
      files = pathStream.filter(Files::isRegularFile).map(Path::toFile).toList();
    }

    LOG.info(
        "Uploading {} files to s3://{}/{}",
        files.size(),
        BUCKET_NAME,
        fullPrefix);

    for (File file : files) {
      String relativePath = localPath.relativize(file.toPath()).toString();
      String s3Key = normalizedPrefix + relativePath;

      PutObjectRequest putRequest =
          PutObjectRequest.builder().bucket(BUCKET_NAME).key(s3Key).build();

      s3Client.putObject(putRequest, AsyncRequestBody.fromFile(file)).join();

      LOG.info("  Uploaded: {} -> {}", file.getName(), s3Key);
    }

    LOG.info("Upload completed");
    updateLatestManifest(fullPrefix, batchTimestamp.toString());
    LOG.info("Updated latest manifest to {}", fullPrefix);
    return fullPrefix;
  }

  @Test
  void testStep3_downloadAndIngest(@TempDir Path writerPath, @TempDir Path readerPath)
      throws Exception {
    LOG.info("Step 3: Download from MinIO and ingest into reader DB");

    ensureBucketExists();

    LatestManifest manifest =
        readLatestManifest()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "db_manifest.json not found. Run Step 2 (writer upload) first."));

    String latestPrefix =
        Optional.ofNullable(manifest.latest)
            .map(pointer -> pointer.prefix)
            .orElseThrow(() -> new IllegalStateException("Manifest missing latest entry"));

    // Reader: Download and ingest
    Path downloadPath = readerPath.resolve("staging");
    Files.createDirectories(downloadPath);
    downloadDirectory(latestPrefix, downloadPath);
    Path activePath = activateDownloadedDb(downloadPath, readerPath.resolve("active-db"));

    try (Options options = new Options().setCreateIfMissing(false);
        RocksDB readerDb = RocksDB.open(options, activePath.toString())) {
      verifyData(readerDb, "key-", 10_000_000);

    }

    LOG.info("Step 3 completed successfully");
  }

  private void downloadDirectory(String s3Prefix, Path localPath) throws Exception {
    String normalizedPrefix = normalizePrefix(s3Prefix);
    ListObjectsV2Request listRequest =
        ListObjectsV2Request.builder().bucket(BUCKET_NAME).prefix(normalizedPrefix).build();

    List<S3Object> objects = s3Client.listObjectsV2(listRequest).join().contents();
    if (objects == null) {
      objects = List.of();
    }

    LOG.info("Downloading {} files from s3://{}/{}", objects.size(), BUCKET_NAME, s3Prefix);

    for (S3Object s3Object : objects) {
      String key = s3Object.key();
      String relativePath = key.substring(normalizedPrefix.length());
      Path localFile = localPath.resolve(relativePath);

      Files.createDirectories(localFile.getParent());

      GetObjectRequest getRequest = GetObjectRequest.builder().bucket(BUCKET_NAME).key(key).build();

      s3Client.getObject(getRequest, AsyncResponseTransformer.toFile(localFile)).join();

      LOG.info("  Downloaded: {} -> {}", key, localFile.getFileName());
    }

    LOG.info("Download completed");
  }

  private void verifyData(RocksDB db, String keyPrefix, int count) throws Exception {
    int foundCount = 0;
    for (int i = 0; i < count; i++) {
      String key = keyPrefix + i;
      byte[] expectedValue = makeValueBytes(i);

      byte[] actualValue = db.get(key.getBytes(StandardCharsets.UTF_8));
      assertThat(actualValue).isNotNull();
      assertThat(actualValue).containsExactly(expectedValue);
      foundCount++;
    }

    LOG.info("Verified {} records with prefix '{}'", foundCount, keyPrefix);
  }

  private List<File> listSstFiles(Path dbPath) {
    List<File> sstFiles = new ArrayList<>();
    File dbDir = dbPath.toFile();

    if (dbDir.exists() && dbDir.isDirectory()) {
      for (File file : dbDir.listFiles()) {
        if (file.getName().endsWith(".sst")) {
          sstFiles.add(file);
        }
      }
    }

    return sstFiles;
  }

  private Optional<LatestManifest> readLatestManifest() throws Exception {
    GetObjectRequest request =
        GetObjectRequest.builder().bucket(BUCKET_NAME).key(MANIFEST_KEY).build();
    try {
      String payload =
          s3Client.getObject(request, AsyncResponseTransformer.toBytes()).join().asUtf8String();
      LatestManifest manifest = OBJECT_MAPPER.readValue(payload, LatestManifest.class);
      if (manifest.previous == null) {
        manifest.previous = new ArrayList<>();
      }
      return Optional.of(manifest);
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchKeyException) {
        return Optional.empty();
      }
      throw e;
    }
  }

  private void ensureBucketExists() throws Exception {
    try {
      s3Client.headBucket(HeadBucketRequest.builder().bucket(BUCKET_NAME).build()).join();
    } catch (CompletionException e) {
      throw new IllegalStateException(
          "Bucket '"
              + BUCKET_NAME
              + "' not found. Create it manually before running this test.",
          e.getCause());
    }
  }

  private void updateLatestManifest(String newPrefix, String updatedAt) throws Exception {
    LatestManifest manifest = readLatestManifest().orElseGet(LatestManifest::new);
    if (manifest.previous == null) {
      manifest.previous = new ArrayList<>();
    }
    if (manifest.latest != null) {
      manifest.previous.add(0, manifest.latest);
    }

    ManifestPointer pointer = new ManifestPointer();
    pointer.prefix = newPrefix;
    pointer.updatedAt = updatedAt;
    manifest.latest = pointer;

    manifest.version = manifest.version + 1;
    byte[] payload =
        OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest);

    writeManifestObject(MANIFEST_KEY, payload);
  }

  private String selectPartitionPrefix(Instant timestamp) {
    byte[] md5;
    try {
      md5 =
          MessageDigest.getInstance("MD5")
              .digest(timestamp.toString().getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException("Unable to compute MD5 hash", e);
    }
    int bucket = md5[0] & 0xff;
    return String.format("db-%02x", bucket);
  }

  private String normalizePrefix(String prefix) {
    return prefix.endsWith("/") ? prefix : prefix + "/";
  }

  private byte[] makeValueBytes(int sequence) {
    String prefix = "value-" + sequence + "-";
    byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
    byte[] value = new byte[VALUE_SIZE_BYTES];
    System.arraycopy(prefixBytes, 0, value, 0, prefixBytes.length);
    for (int i = prefixBytes.length; i < VALUE_SIZE_BYTES; i++) {
      value[i] = 'x';
    }
    return value;
  }

  private String buildTimestampComponent(Instant timestamp) {
    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);
    return formatter.format(timestamp);
  }

  private void flushAndCompact(RocksDB db) throws Exception {
    try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      db.flush(flushOptions);
    }
    db.compactRange();
    try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      db.flush(flushOptions);
    }
  }

  private void writeManifestObject(String key, byte[] payload) {
    PutObjectRequest putRequest =
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(key)
            .contentType("application/json")
            .build();

    s3Client.putObject(putRequest, AsyncRequestBody.fromBytes(payload)).join();
  }

  private Path activateDownloadedDb(Path downloadedPath, Path activeRoot) throws Exception {
    Path activePath = activeRoot;
    Path backupPath = activeRoot.resolveSibling(activeRoot.getFileName() + "-bak");

    deleteRecursively(backupPath);
    if (Files.exists(activePath)) {
      moveDirectory(activePath, backupPath);
      LOG.info("Backed up previous active DB to {}", backupPath);
    }

    moveDirectory(downloadedPath, activePath);
    LOG.info("Activated downloaded DB at {}", activePath);
    return activePath;
  }

  private void moveDirectory(Path source, Path target) throws Exception {
    Files.createDirectories(target.getParent());
    try {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    } catch (Exception atomicFailure) {
      LOG.warn("Atomic move failed ({}), falling back to regular move", atomicFailure.getMessage());
      if (Files.exists(target)) {
        deleteRecursively(target);
      }
      Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private void deleteRecursively(Path path) throws Exception {
    if (!Files.exists(path)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(path)) {
      walk
          .sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (Exception e) {
                  throw new RuntimeException("Failed to delete " + p, e);
                }
              });
    }
  }

  private static class ManifestPointer {
    public String prefix;
    public String updatedAt;
  }

  private static class LatestManifest {
    public int version;
    public ManifestPointer latest;
    public List<ManifestPointer> previous = new ArrayList<>();
  }
}
