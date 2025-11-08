package com.slack.rocksdbsync.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.rocksdbsync.common.JsonUtil;
import com.slack.rocksdbsync.common.SnapshotDescriptors.SnapshotFileEntry;
import com.slack.rocksdbsync.common.SnapshotDescriptors.SnapshotFileType;
import com.slack.rocksdbsync.common.SnapshotDescriptors.SnapshotHistoryEntry;
import com.slack.rocksdbsync.common.SnapshotDescriptors.SnapshotManifest;
import com.slack.rocksdbsync.common.SnapshotDescriptors.SnapshotPointer;
import com.slack.rocksdbsync.s3.S3Helper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.rocksdb.Checkpoint;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WriterService implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(WriterService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter MANIFEST_ID_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSX", Locale.US).withZone(ZoneOffset.UTC);
  private static final String SNAPSHOT_POINTER_KEY = "kaldb_snapshot_manifest.json";
  private static final String SNAPSHOT_MANIFEST_FILENAME = "kaldb_db_manifest.json";
  private static final int POINTER_HISTORY_LIMIT = 16;
  private static final int LOCAL_SNAPSHOT_RETENTION = 5;

  static {
    RocksDB.loadLibrary();
  }

  private final WriterConfig config;
  private RocksDB db;
  private Options options;
  private HttpServer httpServer;
  private ScheduledExecutorService scheduler;
  private S3Helper s3Helper;
  private SnapshotPointer currentPointer;
  private String pointerEtag;

  public WriterService(WriterConfig config) {
    this.config = config;
  }

  public void start() {
    try {
      Files.createDirectories(config.dbPath());
      Files.createDirectories(config.snapshotPath());
      LOG.info("Writer DB directory: {}", config.dbPath());
      LOG.info("Writer snapshot directory: {}", config.snapshotPath());
      options = new Options().setCreateIfMissing(true);
      try {
        db = RocksDB.open(options, config.dbPath().toString());
      } catch (RocksDBException e) {
        throw new IllegalStateException("Failed to open writer DB", e);
      }

      s3Helper =
          S3Helper.builder()
              .bucket(config.bucket())
              .prefix(config.prefix())
              .endpoint(config.endpoint())
              .region(config.region())
              .credentials(config.accessKey(), config.secretKey())
              .build();

      loadCurrentPointer();

      httpServer = HttpServer.create(new InetSocketAddress(config.httpPort()), 0);
      httpServer.createContext("/kv", new WriteHandler());
      httpServer.createContext("/healthz", exchange -> sendResponse(exchange, 200, "ok"));
      httpServer.setExecutor(Executors.newCachedThreadPool());
      httpServer.start();

      scheduler = Executors.newSingleThreadScheduledExecutor();
      scheduler.execute(this::runSnapshotLoop);

      LOG.info("Writer service listening on port {}", config.httpPort());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to start writer service", e);
    }
  }

  private void runSnapshotLoop() {
    long start = System.nanoTime();
    try {
      createAndUploadSnapshot();
    } finally {
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      long targetMillis = config.backupInterval().toMillis();
      long delay = Math.max(0, targetMillis - elapsedMillis);
      scheduler.schedule(this::runSnapshotLoop, delay, TimeUnit.MILLISECONDS);
    }
  }

  private void createAndUploadSnapshot() {
    Path checkpointDir = null;
    try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      db.flush(flushOptions);
      try {
        db.compactRange();
      } catch (RocksDBException e) {
        throw new IllegalStateException("Failed to compact writer DB", e);
      }
      db.flush(flushOptions);
      String snapshotName = "snapshot-" + System.nanoTime();
      checkpointDir = config.snapshotPath().resolve(snapshotName);
      Files.createDirectories(config.snapshotPath());
      if (Files.exists(checkpointDir)) {
        deleteDirectorySilently(checkpointDir);
      }
      try (Checkpoint checkpoint = Checkpoint.create(db)) {
        checkpoint.createCheckpoint(checkpointDir.toString());
      }

      String manifestId = generateManifestId();
      List<SnapshotFileEntry> files = uploadSnapshotFiles(manifestId, checkpointDir);
      SnapshotManifest manifest =
          new SnapshotManifest(manifestId, Instant.now().toString(), files);
      uploadSnapshotManifest(manifestId, manifest);
      updateSnapshotPointer(manifestId);
      LOG.info("Published snapshot {} (stored at {})", manifestId, checkpointDir);
      pruneLocalSnapshots();
    } catch (Exception e) {
      LOG.error("Snapshot upload failed", e);
    }
  }

  private String generateManifestId() {
    return "manifest_" + MANIFEST_ID_FORMATTER.format(Instant.now());
  }

  private void pruneLocalSnapshots() {
    try (Stream<Path> stream = Files.list(config.snapshotPath())) {
      List<Path> snapshots =
          stream.filter(Files::isDirectory).sorted(Comparator.comparing(Path::getFileName)).toList();
      int excess = snapshots.size() - LOCAL_SNAPSHOT_RETENTION;
      for (int i = 0; i < excess; i++) {
        Path stale = snapshots.get(i);
        deleteDirectorySilently(stale);
        LOG.debug("Deleted stale local snapshot {}", stale);
      }
    } catch (IOException e) {
      LOG.warn("Failed to prune local snapshots: {}", e.getMessage());
    }
  }

  private List<SnapshotFileEntry> uploadSnapshotFiles(String manifestId, Path checkpointDir)
      throws IOException {
    List<SnapshotFileEntry> entries = new ArrayList<>();
    try (Stream<Path> stream = Files.list(checkpointDir)) {
      stream
          .sorted(Comparator.comparing(Path::getFileName))
          .forEach(
              path -> {
                SnapshotFileType type = classify(path.getFileName().toString());
                if (type == null) {
                  LOG.debug("Skipping non-essential checkpoint file {}", path.getFileName());
                  return;
                }
                String relativeKey = manifestId + "/" + path.getFileName();
                uploadAndValidate(path, relativeKey, type == SnapshotFileType.SST);
                entries.add(
                    new SnapshotFileEntry(
                        path.getFileName().toString(),
                        s3Helper.buildS3Uri(relativeKey),
                        type));
              });
    }
    return entries;
  }

  private void uploadSnapshotManifest(String manifestId, SnapshotManifest manifest)
      throws IOException {
    byte[] payload = JsonUtil.toJsonBytes(manifest);
    s3Helper.putObjectBytes(manifestId + "/" + SNAPSHOT_MANIFEST_FILENAME, payload, null, false);
  }

  private void updateSnapshotPointer(String manifestId) throws IOException {
    SnapshotPointer pointer = currentPointer;
    if (pointer == null) {
      pointer = new SnapshotPointer();
    } else if (pointer.latestManifestId != null) {
      SnapshotHistoryEntry historyEntry =
          new SnapshotHistoryEntry(
              pointer.latestManifestId, pointer.snapshotManifestUri, Instant.now().toString());
      pointer.previous.add(0, historyEntry);
      if (pointer.previous.size() > POINTER_HISTORY_LIMIT) {
        pointer.previous.remove(pointer.previous.size() - 1);
      }
    }

    pointer.version = pointer.version + 1;
    pointer.latestManifestId = manifestId;
    pointer.snapshotManifestUri =
        s3Helper.buildS3Uri(manifestId + "/" + SNAPSHOT_MANIFEST_FILENAME);

    byte[] payload = JsonUtil.toJsonBytes(pointer);
    boolean ifNoneMatch = pointerEtag == null;
    S3Helper.PutResult result =
        s3Helper.putObjectBytes(SNAPSHOT_POINTER_KEY, payload, pointerEtag, ifNoneMatch);
    pointerEtag = result.eTag();
    currentPointer = pointer;
  }

  private SnapshotFileType classify(String name) {
    if (name.endsWith(".sst")) {
      return SnapshotFileType.SST;
    }
    if (name.startsWith("MANIFEST")) {
      return SnapshotFileType.MANIFEST;
    }
    if ("CURRENT".equals(name)) {
      return SnapshotFileType.CURRENT;
    }
    return null;
  }

  private void uploadAndValidate(Path file, String relativeKey, boolean ifAbsent) {
    try {
      S3Helper.UploadResult result = s3Helper.uploadFile(file, relativeKey, ifAbsent);
      switch (result) {
        case CREATED -> LOG.debug("Uploaded {} -> {}", file.getFileName(), relativeKey);
        case ALREADY_EXISTS ->
            LOG.debug("Skipping existing {} -> {}", file.getFileName(), relativeKey);
        default -> throw new IllegalStateException("Unhandled upload result: " + result);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to upload " + file, e);
    }
  }

  private void loadCurrentPointer() {
    try {
      s3Helper
          .getObject(SNAPSHOT_POINTER_KEY)
          .ifPresent(
              content -> {
                currentPointer = JsonUtil.fromJson(content.data(), SnapshotPointer.class);
                pointerEtag = content.eTag();
              });
    } catch (IOException e) {
      LOG.warn("Unable to load existing snapshot pointer: {}", e.getMessage());
    }
  }

  private void deleteDirectorySilently(Path path) {
    if (path == null || !Files.exists(path)) {
      return;
    }
    try (Stream<Path> stream = Files.walk(path)) {
      stream
          .sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException e) {
                  LOG.warn("Failed to delete {}", p, e);
                }
              });
    } catch (IOException e) {
      LOG.warn("Failed to clean checkpoint directory {}", path, e);
    }
  }

  private class WriteHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
        sendResponse(exchange, 405, "Method Not Allowed");
        return;
      }
      byte[] body = exchange.getRequestBody().readAllBytes();
      JsonNode node = MAPPER.readTree(body);
      JsonNode keyNode = node.get("key");
      JsonNode valueNode = node.get("value");
      if (keyNode == null || valueNode == null) {
        sendResponse(exchange, 400, "Missing key or value");
        return;
      }
      String key = keyNode.asText();
      byte[] value = valueNode.asText().getBytes(StandardCharsets.UTF_8);
      try {
        db.put(key.getBytes(StandardCharsets.UTF_8), value);
        sendResponse(exchange, 202, "accepted");
      } catch (Exception e) {
        LOG.error("Failed to write key {}", key, e);
        sendResponse(exchange, 500, "write failed");
      }
    }
  }

  private void sendResponse(HttpExchange exchange, int status, String message) throws IOException {
    byte[] payload = message.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, payload.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(payload);
    }
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    if (httpServer != null) {
      httpServer.stop(0);
    }
    if (s3Helper != null) {
      s3Helper.close();
    }
    if (db != null) {
      db.close();
    }
    if (options != null) {
      options.close();
    }
  }
}
