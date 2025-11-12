package com.slack.rocksdbsync.loadgen;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple load generator that:
 * - writes N deterministic key/value pairs to the writer in batches of B via /kv/batch
 * - triggers a reader /sync and waits for the checkpoint to reach N
 * - verifies all keys via reader /kv GET endpoint
 *
 * Config via environment variables (with defaults):
 *  WRITER_URL=http://localhost:8081
 *  READER_URL=http://localhost:8082
 *  TOTAL_KEYS=10000
 *  BATCH_SIZE=100
 *  PARALLEL_BATCHES=20
 *  KEY_FORMAT=key-%05d
 *  VALUE_FORMAT=value-%05d
 *  WAIT_AFTER_WRITES_SECONDS=25
 */
public final class KVLoadGen {
  private static final Logger LOG = LoggerFactory.getLogger(KVLoadGen.class);

  private final HttpClient http;

  private final String writerUrl; // e.g., http://localhost:8081
  private final String readerUrl; // e.g., http://localhost:8082
  private final int totalKeys;
  private final int batchSize;
  private final int parallelBatches;
  private final String keyFormat;
  private final String valueFormat;
  private final int waitAfterWritesSeconds;

  public KVLoadGen(
      String writerUrl,
      String readerUrl,
      int totalKeys,
      int batchSize,
      int parallelBatches,
      String keyFormat,
      String valueFormat,
      int waitAfterWritesSeconds) {
    this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    this.writerUrl = Objects.requireNonNull(writerUrl, "writerUrl");
    this.readerUrl = Objects.requireNonNull(readerUrl, "readerUrl");
    this.totalKeys = totalKeys;
    this.batchSize = batchSize;
    this.parallelBatches = Math.max(1, parallelBatches);
    this.keyFormat = keyFormat;
    this.valueFormat = valueFormat;
    this.waitAfterWritesSeconds = waitAfterWritesSeconds;
  }

  public static void main(String[] args) throws Exception {
    KVLoadGen cfg = fromEnv();
    cfg.run();
  }

  private static KVLoadGen fromEnv() {
    String writer = getenv("WRITER_URL", "http://localhost:8081");
    String reader = getenv("READER_URL", "http://localhost:8082");
    int total = parseInt(getenv("TOTAL_KEYS", "10000"), 10000);
    int batch = parseInt(getenv("BATCH_SIZE", "100"), 100);
    int parallel = parseInt(getenv("PARALLEL_BATCHES", "20"), 20);
    String keyFmt = getenv("KEY_FORMAT", "key-%05d");
    String valFmt = getenv("VALUE_FORMAT", "value-%05d");
    int waitAfter = parseInt(getenv("WAIT_AFTER_WRITES_SECONDS", "25"), 25);
    return new KVLoadGen(writer, reader, total, batch, parallel, keyFmt, valFmt, waitAfter);
  }

  private static String getenv(String key, String def) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? def : v;
  }

  private static int parseInt(String raw, int def) {
    try {
      return Integer.parseInt(raw);
    } catch (Exception e) {
      return def;
    }
  }

  public void run() throws Exception {
    LOG.info("Loadgen starting: totalKeys={}, batchSize={}, parallelBatches={}, writerUrl={}, readerUrl={}"
        + ", keyFormat={}, valueFormat={}, waitAfterWritesSeconds={}",
        totalKeys, batchSize, parallelBatches, writerUrl, readerUrl, keyFormat, valueFormat, waitAfterWritesSeconds);

    long t0 = System.nanoTime();
    writeBatches();
    long writeDone = System.nanoTime();

    LOG.info("Waiting {} seconds to allow writer flush + reader resync...", waitAfterWritesSeconds);
    try {
      Thread.sleep(Math.max(0, waitAfterWritesSeconds) * 1000L);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    long synced = System.nanoTime();

    int failures = verifyAll();
    long verified = System.nanoTime();

    LOG.info("Timings: write={} ms, sync={} ms, verify={} ms",
        (writeDone - t0) / 1_000_000,
        (synced - writeDone) / 1_000_000,
        (verified - synced) / 1_000_000);

    if (failures == 0) {
      LOG.info("Loadgen success: verified {} keys ✔", totalKeys);
      System.exit(0);
    } else {
      LOG.error("Loadgen FAILED: {} verification errors", failures);
      System.exit(1);
    }
  }

  private void writeBatches() throws IOException, InterruptedException {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicInteger completedBatches = new AtomicInteger(0);
    int totalBatches = (totalKeys + batchSize - 1) / batchSize;

    int sent = 0;
    while (sent < totalKeys) {
      int start = sent;
      int end = Math.min(totalKeys, sent + batchSize);

      CompletableFuture<Void> future = sendBatchAsync(start, end, completedBatches, totalBatches);
      futures.add(future);
      sent = end;

      // Control in-flight requests to avoid overwhelming the writer
      if (futures.size() >= parallelBatches) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        futures.clear();
      }
    }

    // Wait for remaining batches
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    LOG.info("Wrote {} keys in {} batches (parallelism={})", totalKeys, totalBatches, parallelBatches);
  }

  private CompletableFuture<Void> sendBatchAsync(int start, int end, AtomicInteger completedBatches, int totalBatches) {
    StringBuilder body = new StringBuilder((end - start) * 1100); // Estimate ~1KB per key
    for (int i = start; i < end; i++) {
      String k = String.format(keyFormat, i);
      String v = String.format(valueFormat, i);
      body.append(encode(k)).append('=').append(encode(v)).append('\n');
    }

    HttpRequest req = HttpRequest.newBuilder()
        .uri(URI.create(writerUrl + "/kv/batch"))
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
        .timeout(Duration.ofSeconds(60))
        .header("Content-Type", "text/plain")
        .build();

    return http.sendAsync(req, HttpResponse.BodyHandlers.ofString())
        .handle((resp, ex) -> {
          if (ex != null) {
            LOG.error("Batch [" + start + "-" + end + "] failed with exception", ex);
            throw new RuntimeException("Batch send failed", ex);
          }
          if (resp.statusCode() >= 300) {
            LOG.error("Batch [" + start + "-" + end + "] failed: status={}, body={}",
                resp.statusCode(), resp.body());
            throw new RuntimeException("Batch failed with status " + resp.statusCode());
          }
          int completed = completedBatches.incrementAndGet();
          // Use >= to avoid missing logs due to race conditions
          if (completed >= totalBatches || (completed % 100 == 0 && completed <= totalBatches)) {
            LOG.info("Progress: {} / {} batches completed", completed, totalBatches);
          }
          return null;
        });
  }

  // Intentionally no explicit sync/flush calls here; rely on configured flush/resync intervals.

  private int verifyAll() throws IOException, InterruptedException {
    AtomicInteger failures = new AtomicInteger(0);
    AtomicInteger verified = new AtomicInteger(0);
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (int i = 0; i < totalKeys; i++) {
      final int index = i;
      CompletableFuture<Void> future = verifyKeyAsync(index, verified, failures);
      futures.add(future);

      // Control in-flight requests
      if (futures.size() >= parallelBatches * 10) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        futures.clear();
      }
    }

    // Wait for remaining verifications
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    return failures.get();
  }

  private CompletableFuture<Void> verifyKeyAsync(int index, AtomicInteger verified, AtomicInteger failures) {
    String k = String.format(keyFormat, index);
    String expected = String.format(valueFormat, index);
    String url = readerUrl + "/kv?key=" + URLEncoder.encode(k, StandardCharsets.UTF_8);

    HttpRequest req = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(10))
        .GET()
        .build();

    return http.sendAsync(req, HttpResponse.BodyHandlers.ofByteArray())
        .handle((resp, ex) -> {
          if (ex != null) {
            LOG.error("Verification failed for key: {}", k, ex);
            failures.incrementAndGet();
            return null;
          }
          if (resp.statusCode() != 200) {
            LOG.error("Missing key: {} (status={})", k, resp.statusCode());
            failures.incrementAndGet();
            return null;
          }
          String actual = new String(resp.body(), StandardCharsets.UTF_8);
          if (!expected.equals(actual)) {
            LOG.error("Mismatched value for {}: expected='{}' actual='{}'", k, expected, actual);
            failures.incrementAndGet();
          }
          int count = verified.incrementAndGet();
          // Use >= to handle potential race conditions in modulo check
          if (count % 10000 == 0 || count >= totalKeys) {
            LOG.info("Verified {} / {}", count, totalKeys);
          }
          return null;
        });
  }

  private static String encode(String s) {
    // For line bodies we URL-encode both key and value to be safe
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }
}
