package com.slack.rocksdbsync.reader;

import com.slack.rocksdbsync.CloudConfig;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.rocksdb.CloudEnvOptions;
import org.rocksdb.DBCloud;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReaderServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ReaderServer.class);
  private static final byte[] CHECKPOINT_KEY = "__checkpoint__".getBytes(StandardCharsets.UTF_8);

  static {
    RocksDB.loadLibrary();
  }

  private final ReaderConfig config;
  private final CloudConfig cloudConfig;
  private final Object dbLock = new Object();
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private HttpServer server;
  private ScheduledExecutorService resyncExecutor;
  private Options options;
  private CloudEnvOptions cloudOptions;
  private DBCloud db;

  public ReaderServer(ReaderConfig config) {
    this.config = Objects.requireNonNull(config, "config");
    this.cloudConfig = config.cloud();
  }

  public void start() {
    try {
      initializeDatabase();
      startResyncTask();
      startHttpServer();
      Runtime.getRuntime().addShutdownHook(new Thread(this::safeClose, "reader-http-shutdown"));
      LOG.info("Reader server listening on http://localhost:{}/kv", config.httpPort());
      shutdownLatch.await();
    } catch (IOException | RocksDBException | InterruptedException e) {
      throw new IllegalStateException("Reader server failed", e);
    }
  }

  private void initializeDatabase() throws IOException, RocksDBException {
    this.options = new Options().setCreateIfMissing(false);
    this.cloudOptions = cloudConfig.toCloudEnvOptions();
    Files.createDirectories(cloudConfig.localDbPath());
    LOG.info(
        "Opening reader database in read-only mode at {} (resyncOnOpen={})",
        cloudConfig.localDbPath(), cloudConfig.resyncOnOpen());

    synchronized (dbLock) {
      db = DBCloud.open(
          options,
          cloudConfig.localDbPath().toString(),
          cloudOptions,
          "",
          0,
          true);
      LOG.info("Reader opened read-only with cloud manifest loaded on open");
    }
  }

  private void startHttpServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(config.httpPort()), 0);
    server.createContext("/healthz", exchange -> respond(exchange, 200, "ok"));
    server.createContext("/checkpoint", this::handleCheckpoint);
    server.createContext("/kv", this::handleRead);
    server.createContext("/sync", this::handleSync);
    server.setExecutor(Executors.newCachedThreadPool());
    server.start();
  }

  private void handleCheckpoint(HttpExchange exchange) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "only GET supported");
      return;
    }
    try {
      byte[] value = getValue(CHECKPOINT_KEY);
      long checkpoint =
          value == null ? 0L : Long.parseLong(new String(value, StandardCharsets.UTF_8));
      respond(exchange, 200, "{\"checkpoint\":" + checkpoint + "}");
    } catch (Exception e) {
      LOG.error("Failed to fetch checkpoint", e);
      respond(exchange, 500, "checkpoint failed");
    }
  }

  private void handleRead(HttpExchange exchange) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "only GET supported");
      return;
    }
    Map<String, String> params = parseParams(exchange);
    String key = params.get("key");
    if (key == null) {
      respond(exchange, 400, "missing key");
      return;
    }
    try {
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
      byte[] value = getValue(keyBytes);
      if (value == null) {
        respond(exchange, 404, "not_found");
        return;
      }
      writeBinaryResponse(exchange, value);
    } catch (RocksDBException e) {
      LOG.error("Failed to read key {}", key, e);
      respond(exchange, 500, "read failed");
    }
  }

  private void handleSync(HttpExchange exchange) throws IOException {
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "only POST supported");
      return;
    }
    try {
      reopenReadOnly();
      respond(exchange, 200, "synced");
    } catch (RocksDBException e) {
      LOG.error("Resync failed", e);
      respond(exchange, 500, "sync failed");
    }
  }

  private static Map<String, String> parseParams(HttpExchange exchange) throws IOException {
    Map<String, String> params = new LinkedHashMap<>();
    URI uri = exchange.getRequestURI();
    if (uri.getRawQuery() != null) {
      decodeInto(uri.getRawQuery(), params);
    }
    byte[] bodyBytes = exchange.getRequestBody().readAllBytes();
    if (bodyBytes.length > 0) {
      decodeInto(new String(bodyBytes, StandardCharsets.UTF_8), params);
    }
    return params;
  }

  private static void decodeInto(String raw, Map<String, String> params) throws IOException {
    for (String pair : raw.split("&")) {
      if (pair.isEmpty()) {
        continue;
      }
      String[] kv = pair.split("=", 2);
      String key = URLDecoder.decode(kv[0], StandardCharsets.UTF_8);
      String value =
          kv.length > 1 ? URLDecoder.decode(kv[1], StandardCharsets.UTF_8) : "";
      params.put(key, value);
    }
  }

  private static void respond(HttpExchange exchange, int status, String body)
      throws IOException {
    byte[] payload = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, payload.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(payload);
    } finally {
      exchange.close();
    }
  }

  @Override
  public void close() {
    safeClose();
  }

  private void safeClose() {
    try {
      if (server != null) {
        server.stop(0);
      }
    } catch (Exception e) {
      LOG.warn("Failed to stop HTTP server", e);
    }
    try {
      if (resyncExecutor != null) {
        resyncExecutor.shutdownNow();
      }
    } catch (Exception e) {
      LOG.warn("Failed to stop resync executor", e);
    }
    try {
      synchronized (dbLock) {
        if (db != null) {
          db.close();
          db = null;
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to close DB", e);
    }
    try {
      if (options != null) {
        options.close();
        options = null;
      }
    } catch (Exception e) {
      LOG.warn("Failed to close options", e);
    } finally {
      shutdownLatch.countDown();
    }
  }

  private void writeBinaryResponse(HttpExchange exchange, byte[] payload) throws IOException {
    exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
    exchange.sendResponseHeaders(200, payload.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(payload);
    } finally {
      exchange.close();
    }
  }

  private byte[] getValue(byte[] key) throws RocksDBException {
    synchronized (dbLock) {
      return db.get(key);
    }
  }

  private void reopenReadOnly() throws RocksDBException {
    synchronized (dbLock) {
      if (db != null) {
        try {
          db.close();
        } catch (Exception e) {
          LOG.warn("Failed closing DB before reopen", e);
        } finally {
          db = null;
        }
      }
      LOG.info("Reopening reader read-only (resyncOnOpen={})", cloudConfig.resyncOnOpen());
      db = DBCloud.open(
          options,
          cloudConfig.localDbPath().toString(),
          cloudOptions,
          "",
          0,
          true);
      LOG.info("Reopen complete - cloud manifest refreshed on open");
    }
  }

  private void startResyncTask() {
    if (!config.enablePeriodicResync()) {
      LOG.info("Periodic resync is disabled - reader will only sync on startup");
      return;
    }
    LOG.info("Starting periodic resync task with interval: {} seconds", config.resyncIntervalSeconds());
    resyncExecutor = Executors.newSingleThreadScheduledExecutor();
    resyncExecutor.scheduleWithFixedDelay(
        () -> {
          try {
            reopenReadOnly();
          } catch (RocksDBException e) {
            LOG.warn("Background resync failed", e);
          }
        },
        config.resyncIntervalSeconds(),
        config.resyncIntervalSeconds(),
        TimeUnit.SECONDS);
  }
}
