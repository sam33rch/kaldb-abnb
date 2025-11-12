package com.slack.rocksdbsync.writer;

import com.slack.rocksdbsync.CloudConfig;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.rocksdb.CloudEnvOptions;
import org.rocksdb.DBCloud;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WriterServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(WriterServer.class);

  static {
    RocksDB.loadLibrary();
  }

  private final WriterConfig config;
  private static final byte[] CHECKPOINT_KEY = "__checkpoint__".getBytes(StandardCharsets.UTF_8);
  private final Object writeLock = new Object();
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private HttpServer server;
  private DBCloud db;
  private FlushOptions flushOptions;
  private WriteOptions writeOptions;
  private ScheduledExecutorService flushScheduler;
  private long checkpointCounter = 0L;

  public WriterServer(WriterConfig config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  public void start() {
    try {
      initializeDatabase(config.cloud());
      startHttpServer();
      startPeriodicFlush();
      Runtime.getRuntime().addShutdownHook(new Thread(this::safeClose, "writer-http-shutdown"));
      LOG.info("Writer server listening on http://localhost:{}/kv", config.httpPort());
      shutdownLatch.await();
    } catch (IOException | RocksDBException | InterruptedException e) {
      throw new IllegalStateException("Writer server failed", e);
    }
  }

  private void initializeDatabase(CloudConfig cloudConfig) throws IOException, RocksDBException {
    CloudEnvOptions cloudOptions = cloudConfig.toCloudEnvOptions();
    Options options = new Options().setCreateIfMissing(true).setAtomicFlush(true);
    Files.createDirectories(cloudConfig.localDbPath());
    LOG.info("Opening writer database at {}", cloudConfig.localDbPath());
    LOG.info("AWS_ENDPOINT={}, AWS_ENDPOINT_URL={}",
        System.getenv("AWS_ENDPOINT"), System.getenv("AWS_ENDPOINT_URL"));
    this.db =
        DBCloud.open(
            options, cloudConfig.localDbPath().toString(), cloudOptions, "", 0, false);
    this.flushOptions = new FlushOptions().setWaitForFlush(true);
    this.writeOptions = new WriteOptions().setDisableWAL(true);
    // initialize checkpoint from existing value if present
    byte[] ck = db.get(CHECKPOINT_KEY);
    if (ck != null) {
      try {
        checkpointCounter = Long.parseLong(new String(ck, StandardCharsets.UTF_8));
      } catch (NumberFormatException ignore) {
        checkpointCounter = 0L;
      }
    }
    LOG.info("Writer initialized with blocking flush and WAL disabled; atomic_flush=true; start checkpoint={}", checkpointCounter);
  }

  private void startHttpServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(config.httpPort()), 0);
    server.createContext("/healthz", exchange -> respond(exchange, 200, "ok"));
    server.createContext("/kv", this::handleWrite);
    server.createContext("/kv/batch", this::handleBatchWrite);
    server.createContext("/flush", this::handleFlush);
    server.setExecutor(Executors.newCachedThreadPool());
    server.start();
  }

  private void startPeriodicFlush() {
    if (!config.enablePeriodicFlush()) {
      LOG.info("Periodic flush is disabled - using manual/auto flush");
      return;
    }
    if (flushScheduler != null) {
      return;
    }
    long interval = Math.max(1, config.flushIntervalSeconds());
    flushScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "writer-periodic-flush");
              t.setDaemon(true);
              return t;
            });
    LOG.info("Starting periodic flush every {} seconds", interval);
    flushScheduler.scheduleWithFixedDelay(
        () -> {
          try {
            synchronized (writeLock) {
              flushInternal();
            }
          } catch (RocksDBException e) {
            LOG.warn("Periodic flush failed", e);
          }
        },
        interval,
        interval,
        TimeUnit.SECONDS);
  }

  private void handleWrite(HttpExchange exchange) throws IOException {
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "only POST supported");
      return;
    }
    Map<String, String> params = parseParams(exchange);
    String key = params.get("key");
    String value = params.get("value");
    if (key == null || value == null) {
      respond(exchange, 400, "missing key or value");
      return;
    }
    try {
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
      byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
      synchronized (writeLock) {
        // Increment simulated Kafka offset and write data + checkpoint atomically
        long nextCk = ++checkpointCounter;
        WriteBatch batch = new WriteBatch();
        batch.put(keyBytes, valueBytes);
        batch.put(CHECKPOINT_KEY, Long.toString(nextCk).getBytes(StandardCharsets.UTF_8));
        db.write(writeOptions, batch);
        if (config.flushOnWrite()) {
          flushInternal();
        }
      }
    } catch (RocksDBException e) {
      LOG.error("Failed to write key {}", key, e);
      respond(exchange, 500, "write failed");
      return;
    }
    respond(exchange, 202, "queued");
  }

  private void handleBatchWrite(HttpExchange exchange) throws IOException {
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "only POST supported");
      return;
    }
    // Expect body as newline-separated key=value pairs, URL-encoded if needed.
    // Example:
    //   key=a&value=1\n
    byte[] bodyBytes = exchange.getRequestBody().readAllBytes();
    String body = new String(bodyBytes, StandardCharsets.UTF_8);
    ArrayList<Map.Entry<String, String>> entries = new ArrayList<>();
    for (String line : body.split("\n")) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) continue;
      int eq = trimmed.indexOf('=');
      if (eq <= 0) {
        respond(exchange, 400, "invalid line: expected key=value");
        return;
      }
      String k = URLDecoder.decode(trimmed.substring(0, eq), StandardCharsets.UTF_8);
      String v = URLDecoder.decode(trimmed.substring(eq + 1), StandardCharsets.UTF_8);
      entries.add(Map.entry(k, v));
    }
    if (entries.isEmpty()) {
      respond(exchange, 400, "empty batch");
      return;
    }
    // Sort by key as recommended for WriteBatch performance
    Collections.sort(entries, (a, b) -> a.getKey().compareTo(b.getKey()));
    int count = entries.size();
    try {
      synchronized (writeLock) {
        long nextCk = checkpointCounter + count;
        WriteBatch batch = new WriteBatch();
        for (Map.Entry<String, String> e : entries) {
          batch.put(
              e.getKey().getBytes(StandardCharsets.UTF_8),
              e.getValue().getBytes(StandardCharsets.UTF_8));
        }
        batch.put(CHECKPOINT_KEY, Long.toString(nextCk).getBytes(StandardCharsets.UTF_8));
        db.write(writeOptions, batch);
        checkpointCounter = nextCk;
        if (config.flushOnWrite()) {
          flushInternal();
        }
      }
    } catch (RocksDBException e) {
      LOG.error("Failed to write batch of {} records", entries.size(), e);
      respond(exchange, 500, "batch write failed");
      return;
    }
    respond(exchange, 202, "{\"status\":\"queued\",\"count\":" + count + "}");
  }

  private void handleFlush(HttpExchange exchange) throws IOException {
    if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      respond(exchange, 405, "only POST supported");
      return;
    }
    try {
      synchronized (writeLock) {
        flushInternal();
      }
      respond(exchange, 200, "{\"status\":\"flushed\"}");
    } catch (RocksDBException e) {
      LOG.error("Manual flush failed", e);
      respond(exchange, 500, "flush failed");
    }
  }

  private void flushInternal() throws RocksDBException {
    LOG.info("flushInternal() called - attempting flush");
    try {
      db.flush(flushOptions);
      LOG.info("flushInternal() completed successfully");
    } catch (Exception e) {
      LOG.error("flushInternal() failed with exception", e);
      throw e;
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
    stopPeriodicFlush();
    try {
      if (flushOptions != null) {
        flushOptions.close();
      }
    } catch (Exception ignore) {
    }
    try {
      if (writeOptions != null) {
        writeOptions.close();
      }
    } catch (Exception ignore) {
    }
    try {
      if (db != null) {
        db.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close DB", e);
    } finally {
      shutdownLatch.countDown();
    }
  }

  private void stopPeriodicFlush() {
    if (flushScheduler == null) {
      return;
    }
    flushScheduler.shutdownNow();
    try {
      if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.warn("Timed out waiting for periodic flush scheduler to terminate");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    flushScheduler = null;
  }
}
