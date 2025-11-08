package com.slack.rocksdbsync.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReaderService implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ReaderService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    RocksDB.loadLibrary();
  }

  private final ReaderConfig config;
  private HttpServer httpServer;
  private ScheduledExecutorService scheduler;
  private S3Helper s3Helper;
  private Options options;
  private RocksDB db;
  private final ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();

  public ReaderService(ReaderConfig config) {
    this.config = config;
  }

  public void start() {
    try {
      Files.createDirectories(config.activeDbPath());
      Files.createDirectories(config.stagingPath());
      options = new Options().setCreateIfMissing(true);
      db = RocksDB.open(options, config.activeDbPath().toString());

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

  private void syncFromS3() {
    try {
      s3Helper.downloadToDirectory(config.stagingPath());
    } catch (IOException e) {
      LOG.warn("No snapshot available yet: {}", e.getMessage());
      return;
    }

    dbLock.writeLock().lock();
    try {
      if (db != null) {
        db.close();
        db = null;
      }

      deleteDirectory(config.activeDbPath());
      Files.createDirectories(config.activeDbPath().getParent());
      Files.move(
          config.stagingPath(),
          config.activeDbPath(),
          StandardCopyOption.REPLACE_EXISTING);
      Files.createDirectories(config.stagingPath());

      db = RocksDB.open(options, config.activeDbPath().toString());
      LOG.info("Reader DB refreshed from S3.");
    } catch (Exception e) {
      LOG.error("Failed to swap reader DB", e);
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

  private void deleteDirectory(Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (Stream<Path> stream = Files.walk(path)) {
      stream
          .sorted((a, b) -> b.compareTo(a))
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException e) {
                  throw new RuntimeException("Failed to delete " + p, e);
                }
              });
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
    dbLock.writeLock().lock();
    try {
      if (db != null) {
        db.close();
        db = null;
      }
      if (options != null) {
        options.close();
      }
    } finally {
      dbLock.writeLock().unlock();
    }
  }
}
