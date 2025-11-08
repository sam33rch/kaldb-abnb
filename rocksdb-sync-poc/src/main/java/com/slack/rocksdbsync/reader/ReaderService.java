package com.slack.rocksdbsync.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.rocksdbsync.common.JsonUtil;
import com.slack.rocksdbsync.common.SnapshotDescriptors.SnapshotFileEntry;
import com.slack.rocksdbsync.common.SnapshotDescriptors.SnapshotFileType;
import com.slack.rocksdbsync.common.SnapshotDescriptors.SnapshotManifest;
import com.slack.rocksdbsync.common.SnapshotDescriptors.SnapshotPointer;
import com.slack.rocksdbsync.s3.S3Helper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReaderService implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ReaderService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String SNAPSHOT_POINTER_KEY = "kaldb_snapshot_manifest.json";
  private static final String LOCAL_MANIFEST_FILENAME = "kaldb_db_manifest.json";

  static {
    RocksDB.loadLibrary();
  }

  private final ReaderConfig config;
  private HttpServer httpServer;
  private ScheduledExecutorService scheduler;
  private S3Helper s3Helper;
  private Options readOnlyOptions;
  private RocksDB db;
  private final ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();
  private String pointerEtag;
  private String currentManifestId;

  public ReaderService(ReaderConfig config) {
    this.config = config;
  }

  public void start() {
    try {
      Files.createDirectories(config.dbPath());
      LOG.info("Reader DB directory: {}", config.dbPath());
      readOnlyOptions = new Options().setCreateIfMissing(true);
      try {
        db = RocksDB.openReadOnly(readOnlyOptions, config.dbPath().toString());
        loadLocalManifestId();
      } catch (RocksDBException e) {
        db = null;
        LOG.info(
            "Reader DB not initialized yet ({}). Waiting for first snapshot.", e.getMessage());
      }

      s3Helper =
          S3Helper.builder()
              .bucket(config.bucket())
              .prefix(config.prefix())
              .endpoint(config.endpoint())
              .region(config.region())
              .credentials(config.accessKey(), config.secretKey())
              .build();

      httpServer = HttpServer.create(new InetSocketAddress(config.httpPort()), 0);
      httpServer.createContext("/kv", new ReadHandler());
      httpServer.createContext("/healthz", exchange -> sendResponse(exchange, 200, "ok"));
      httpServer.setExecutor(Executors.newCachedThreadPool());
      httpServer.start();

      scheduler = Executors.newSingleThreadScheduledExecutor();
      scheduler.scheduleAtFixedRate(
          this::syncFromS3,
          config.restoreInterval().toSeconds(),
          config.restoreInterval().toSeconds(),
          TimeUnit.SECONDS);

      LOG.info("Reader service listening on port {}", config.httpPort());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to start reader service", e);
    }
  }

  private void loadLocalManifestId() {
    Path manifestPath = config.dbPath().resolve(LOCAL_MANIFEST_FILENAME);
    if (!Files.exists(manifestPath)) {
      return;
    }
    try {
      byte[] data = Files.readAllBytes(manifestPath);
      SnapshotManifest manifest = JsonUtil.fromJson(data, SnapshotManifest.class);
      currentManifestId = manifest.manifestId;
    } catch (Exception e) {
      LOG.warn("Failed to read local manifest: {}", e.getMessage());
    }
  }

  private void syncFromS3() {
    try {
      Optional<S3Helper.ObjectContent> pointerData =
          currentManifestId == null
              ? s3Helper.getObject(SNAPSHOT_POINTER_KEY)
              : s3Helper.getObject(SNAPSHOT_POINTER_KEY, pointerEtag);
      if (pointerData.isEmpty()) {
        if (currentManifestId == null) {
          LOG.debug("Snapshot pointer not available yet; retrying.");
        }
        return;
      }
      SnapshotPointer pointer = JsonUtil.fromJson(pointerData.get().data(), SnapshotPointer.class);
      if (pointer.latestManifestId == null
          || pointer.latestManifestId.equals(currentManifestId)) {
        if (currentManifestId == null) {
          LOG.debug("No manifest applied yet; waiting for writer to publish snapshot.");
        }
        return;
      }
      if (applySnapshot(pointer)) {
        pointerEtag = pointerData.get().eTag();
      }
    } catch (IOException e) {
      LOG.warn("Failed to sync snapshot: {}", e.getMessage());
    }
  }

  private boolean applySnapshot(SnapshotPointer pointer) {
    try {
      String manifestKey = s3Helper.relativeKeyFromUri(pointer.snapshotManifestUri);
      SnapshotManifest snapshotManifest =
          s3Helper
              .getObject(manifestKey)
              .map(content -> JsonUtil.fromJson(content.data(), SnapshotManifest.class))
              .orElseThrow(() -> new IllegalStateException("Snapshot manifest missing"));

      downloadSnapshotFiles(snapshotManifest);
      persistLocalManifest(snapshotManifest);
      reloadDatabase(snapshotManifest.manifestId);
      return true;
    } catch (Exception e) {
      LOG.error("Failed to apply snapshot {}", pointer.latestManifestId, e);
      return false;
    }
  }

  private void downloadSnapshotFiles(SnapshotManifest manifest) throws IOException {
    SnapshotFileEntry currentEntry = null;
    for (SnapshotFileEntry entry : manifest.files) {
      if (entry.type == SnapshotFileType.CURRENT) {
        currentEntry = entry;
        continue;
      }
      downloadIfNeeded(entry);
    }
    if (currentEntry != null) {
      downloadIfNeeded(currentEntry);
    } else {
      throw new IllegalStateException("Snapshot missing CURRENT file");
    }
  }

  private void downloadIfNeeded(SnapshotFileEntry entry) throws IOException {
    Path target = config.dbPath().resolve(entry.name);
    boolean shouldDownload =
        entry.type != SnapshotFileType.SST || Files.notExists(target);
    if (!shouldDownload) {
      return;
    }
    String relativeKey = s3Helper.relativeKeyFromUri(entry.s3Uri);
    s3Helper.downloadObject(relativeKey, target);
  }

  private void persistLocalManifest(SnapshotManifest manifest) throws IOException {
    Path manifestPath = config.dbPath().resolve(LOCAL_MANIFEST_FILENAME);
    Files.write(
        manifestPath,
        JsonUtil.toJsonBytes(manifest),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE);
  }

  private void reloadDatabase(String manifestId) {
    dbLock.writeLock().lock();
    try {
      if (db != null) {
        db.close();
        db = null;
      }

      try (Options writeOptions = new Options().setCreateIfMissing(true);
          RocksDB writable = RocksDB.open(writeOptions, config.dbPath().toString())) {
        writable.compactRange();
      } catch (RocksDBException e) {
        throw new IllegalStateException("Failed to compact reader DB", e);
      }

      try {
        db = RocksDB.openReadOnly(readOnlyOptions, config.dbPath().toString());
      } catch (RocksDBException e) {
        throw new IllegalStateException("Failed to reopen reader DB", e);
      }
      currentManifestId = manifestId;
      LOG.info("Reader switched to manifest {}", manifestId);
    } catch (Exception e) {
      LOG.error("Failed to reopen RocksDB", e);
    } finally {
      dbLock.writeLock().unlock();
    }
  }

  private class ReadHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
        sendResponse(exchange, 405, "Method Not Allowed");
        return;
      }
      Map<String, String> query = parseQuery(exchange.getRequestURI());
      String key = query.get("key");
      if (key == null) {
        sendResponse(exchange, 400, "Missing key parameter");
        return;
      }

      dbLock.readLock().lock();
      try {
        if (db == null) {
          sendResponse(exchange, 503, "DB not ready");
          return;
        }
        byte[] value = db.get(key.getBytes(StandardCharsets.UTF_8));
        if (value == null) {
          sendResponse(exchange, 404, "Not Found");
          return;
        }
        Map<String, String> response = Map.of("key", key, "value", new String(value));
        byte[] payload = MAPPER.writeValueAsBytes(response);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, payload.length);
        try (OutputStream os = exchange.getResponseBody()) {
          os.write(payload);
        }
      } catch (Exception e) {
        LOG.error("Read failed for key {}", key, e);
        sendResponse(exchange, 500, "Read failed");
      } finally {
        dbLock.readLock().unlock();
      }
    }
  }

  private Map<String, String> parseQuery(URI uri) throws IOException {
    Map<String, String> params = new HashMap<>();
    String query = uri.getRawQuery();
    if (query == null || query.isEmpty()) {
      return params;
    }
    for (String part : query.split("&")) {
      String[] kv = part.split("=", 2);
      if (kv.length == 2) {
        params.put(
            URLDecoder.decode(kv[0], StandardCharsets.UTF_8),
            URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
      }
    }
    return params;
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
    dbLock.writeLock().lock();
    try {
      if (db != null) {
        db.close();
        db = null;
      }
      if (readOnlyOptions != null) {
        readOnlyOptions.close();
      }
    } finally {
      dbLock.writeLock().unlock();
    }
  }
}
