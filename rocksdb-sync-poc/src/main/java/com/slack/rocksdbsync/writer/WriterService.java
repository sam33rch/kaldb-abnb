package com.slack.rocksdbsync.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WriterService implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(WriterService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    RocksDB.loadLibrary();
  }

  private final WriterConfig config;
  private RocksDB db;
  private Options options;
  private HttpServer httpServer;
  private ScheduledExecutorService scheduler;
  private S3Helper s3Helper;

  public WriterService(WriterConfig config) {
    this.config = config;
  }

  public void start() {
    try {
      Files.createDirectories(config.dbPath());
      options = new Options().setCreateIfMissing(true);
      db = RocksDB.open(options, config.dbPath().toString());

      s3Helper =
          S3Helper.builder()
              .bucket(config.bucket())
              .prefix(config.prefix())
              .endpoint(config.endpoint())
              .region(config.region())
              .credentials(config.accessKey(), config.secretKey())
              .build();

      httpServer = HttpServer.create(new InetSocketAddress(config.httpPort()), 0);
      httpServer.createContext("/kv", new WriteHandler());
      httpServer.createContext("/healthz", exchange -> sendResponse(exchange, 200, "ok"));
      httpServer.setExecutor(Executors.newCachedThreadPool());
      httpServer.start();

      scheduler = Executors.newSingleThreadScheduledExecutor();
      scheduler.scheduleAtFixedRate(
          this::flushAndUpload,
          config.backupInterval().toSeconds(),
          config.backupInterval().toSeconds(),
          TimeUnit.SECONDS);

      LOG.info("Writer service listening on port {}", config.httpPort());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to start writer service", e);
    }
  }

  private void flushAndUpload() {
    try {
      flushAndCompact();
      uploadDatabaseFiles();
    } catch (Exception e) {
      LOG.error("Flush/upload cycle failed", e);
    }
  }

  private void flushAndCompact() throws Exception {
    try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      db.flush(flushOptions);
    }
    db.compactRange();
    try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
      db.flush(flushOptions);
    }
  }

  private void uploadDatabaseFiles() throws IOException {
    List<Path> sstFiles = new ArrayList<>();
    List<Path> manifestFiles = new ArrayList<>();
    Path currentFile = null;

    try (Stream<Path> stream = Files.list(config.dbPath())) {
      stream.forEach(
          path -> {
            String name = path.getFileName().toString();
            if (name.endsWith(".sst")) {
              sstFiles.add(path);
            } else if (name.startsWith("MANIFEST")) {
              manifestFiles.add(path);
            } else if ("CURRENT".equals(name)) {
              currentFile = path;
            } else {
              LOG.debug("Skipping non-essential file {}", name);
            }
          });
    }

    sstFiles.sort(Comparator.comparing(Path::getFileName));
    manifestFiles.sort(Comparator.comparing(Path::getFileName));

    for (Path sst : sstFiles) {
      uploadAndValidate(sst, true);
    }
    for (Path manifest : manifestFiles) {
      uploadAndValidate(manifest, true);
    }
    if (currentFile != null) {
      uploadAndValidate(currentFile, false);
    } else {
      LOG.warn("CURRENT file missing in {}", config.dbPath());
    }
  }

  private void uploadAndValidate(Path file, boolean ifAbsent) {
    String key = file.getFileName().toString();
    try {
      S3Helper.UploadResult result = s3Helper.uploadFile(file, key, ifAbsent);
      switch (result) {
        case CREATED -> LOG.debug("Uploaded {} to s3://{}/{}", file, config.bucket(), key);
        case ALREADY_EXISTS ->
            LOG.debug("S3 already has {}, skipping upload (key {})", file.getFileName(), key);
        default -> throw new IllegalStateException("Unhandled upload result: " + result);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to upload " + file, e);
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
