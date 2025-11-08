package com.slack.rocksdbsync.loadgen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LoadGeneratorMain {
  private static final Logger LOG = LoggerFactory.getLogger(LoadGeneratorMain.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    LoadGeneratorConfig config = LoadGeneratorConfig.fromEnv();
    new LoadGeneratorMain(config).run();
  }

  private final LoadGeneratorConfig config;
  private final HttpClient httpClient;

  private LoadGeneratorMain(LoadGeneratorConfig config) {
    this.config = Objects.requireNonNull(config);
    this.httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  public void run() throws Exception {
    LOG.info(
        "Loadgen starting: batches={} batchBytes={} keySize={} valueSize={} writer={} reader={}",
        config.totalBatches(),
        config.batchBytes(),
        config.keySizeBytes(),
        config.valueSizeBytes(),
        config.writerUri(),
        config.readerUri());

    long recordsPerBatch = recordsPerBatch();
    for (int batch = 0; batch < config.totalBatches(); batch++) {
      long start = System.nanoTime();
      writeBatch(batch, recordsPerBatch);
      long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      LOG.info(
          "Finished batch {} ({} records, {} MB) in {}s",
          batch,
          recordsPerBatch,
          config.batchBytes() / (1024 * 1024),
          millis / 1000.0);
    }

    LOG.info("All batches written. Waiting 30s for reader to ingest...");
    Thread.sleep(Duration.ofSeconds(30));
    validate(recordsPerBatch);
  }

  private long recordsPerBatch() {
    if (config.keysPerBatch() > 0) {
      return config.keysPerBatch();
    }
    return Math.max(1, config.batchBytes() / (config.keySizeBytes() + config.valueSizeBytes()));
  }

  private void writeBatch(int batch, long recordCount) throws Exception {
    Semaphore permits = new Semaphore(config.writerConcurrency());
    AtomicLong inFlight = new AtomicLong();
    for (long i = 0; i < recordCount; i++) {
      permits.acquire();
      inFlight.incrementAndGet();
      String key = buildFixedString("b" + batch + ":k" + i, config.keySizeBytes());
      String value = buildFixedString("v" + batch + ":" + i, config.valueSizeBytes());
      HttpRequest request = buildWriteRequest(config.writerUri(), key, value);
      httpClient
          .sendAsync(request, HttpResponse.BodyHandlers.ofString())
          .whenComplete(
              (resp, err) -> {
                permits.release();
                inFlight.decrementAndGet();
                if (err != null) {
                  LOG.error("Write failed", err);
                  throw new RuntimeException(err);
                }
                if (resp.statusCode() != 202) {
                  throw new IllegalStateException(
                      "Writer returned "
                          + resp.statusCode()
                          + " :: "
                          + resp.body());
                }
              });
      if ((i + 1) % 10000 == 0) {
        LOG.info("Batch {} progress: {}/{} writes in-flight={} ", batch, i + 1, recordCount, inFlight.get());
      }
    }
    permits.acquire(config.writerConcurrency());
    permits.release(config.writerConcurrency());
  }

  private void validate(long recordsPerBatch) throws Exception {
    LOG.info("Starting validation against reader {}", config.readerUri());
    Semaphore permits = new Semaphore(config.readerConcurrency());
    AtomicLong checked = new AtomicLong();
    for (int batch = 0; batch < config.totalBatches(); batch++) {
      for (long i = 0; i < recordsPerBatch; i++) {
        permits.acquire();
        String key = buildFixedString("b" + batch + ":k" + i, config.keySizeBytes());
        String value = buildFixedString("v" + batch + ":" + i, config.valueSizeBytes());
        HttpRequest request = buildReadRequest(config.readerUri(), key);
        httpClient
            .sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .whenComplete(
                (resp, err) -> {
                  permits.release();
                  long c = checked.incrementAndGet();
                  if (err != null) {
                    throw new RuntimeException(err);
                  }
                  if (resp.statusCode() != 200) {
                    throw new IllegalStateException(
                        "Reader missing key: status " + resp.statusCode() + " body=" + resp.body());
                  }
                  try {
                    JsonNode node = MAPPER.readTree(resp.body());
                    if (!value.equals(node.get("value").asText())) {
                      throw new IllegalStateException("Value mismatch for key");
                    }
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  if (c % 10000 == 0) {
                    LOG.info("Validated {} keys", c);
                  }
                });
      }
    }
    permits.acquire(config.readerConcurrency());
    LOG.info("Validation complete: {} keys", checked.get());
  }

  private static HttpRequest buildWriteRequest(URI uri, String key, String value) {
    String json =
        String.format(Locale.US, "{\"key\":\"%s\",\"value\":\"%s\"}", escape(key), escape(value));
    return HttpRequest.newBuilder(uri)
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(json))
        .build();
  }

  private static HttpRequest buildReadRequest(URI readerUri, String key) {
    String encoded = URLEncoder.encode(key, StandardCharsets.UTF_8);
    URI uri = URI.create(readerUri.toString() + "/kv?key=" + encoded);
    return HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(15)).GET().build();
  }

  private static String buildFixedString(String prefix, int targetLength) {
    if (prefix.length() >= targetLength) {
      return prefix.substring(0, targetLength);
    }
    StringBuilder sb = new StringBuilder(targetLength);
    sb.append(prefix);
    while (sb.length() < targetLength) {
      sb.append('x');
    }
    return sb.toString();
  }

  private static String escape(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
