package com.slack.rocksdbsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.slack.rocksdbsync.reader.ReaderConfig;
import com.slack.rocksdbsync.reader.ReaderService;
import com.slack.rocksdbsync.s3.S3Helper;
import com.slack.rocksdbsync.writer.WriterConfig;
import com.slack.rocksdbsync.writer.WriterService;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RocksDbSyncIntegrationTest {
  private static final String ENDPOINT = "http://localhost:9000";
  private static final String REGION = "us-east-1";
  private static final String BUCKET = "test-bucket";

  private WriterService writerService;
  private ReaderService readerService;
  private S3Helper s3Helper;
  private String prefix;

  @BeforeEach
  void setUp() {
    assumeTrue(minioAvailable(), "MinIO must be running at " + ENDPOINT);
    prefix = "rocksdb-sync-tests/" + UUID.randomUUID();
    s3Helper =
        S3Helper.builder()
            .bucket(BUCKET)
            .prefix(prefix)
            .endpoint(ENDPOINT)
            .region(REGION)
            .credentials(
                System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"))
            .build();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (writerService != null) {
      writerService.close();
    }
    if (readerService != null) {
      readerService.close();
    }
    if (s3Helper != null) {
      s3Helper.deletePrefix();
      s3Helper.close();
    }
  }

  @Test
  void writerBackupsAreRestoredByReader() throws Exception {
    Path writerDb = Files.createTempDirectory("writer-db");
    Path readerDb = Files.createTempDirectory("reader-db");
    Path readerStaging = Files.createTempDirectory("reader-staging");

    WriterConfig writerConfig =
        new WriterConfig.Builder()
            .dbPath(writerDb)
            .bucket(BUCKET)
            .prefix(prefix)
            .endpoint(ENDPOINT)
            .region(REGION)
            .credentials(
                System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"))
            .httpPort(19081)
            .backupInterval(Duration.ofSeconds(5))
            .build();

    ReaderConfig readerConfig =
        new ReaderConfig.Builder()
            .activeDbPath(readerDb)
            .stagingPath(readerStaging)
            .bucket(BUCKET)
            .prefix(prefix)
            .endpoint(ENDPOINT)
            .region(REGION)
            .credentials(
                System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"))
            .httpPort(19082)
            .restoreInterval(Duration.ofSeconds(5))
            .build();

    writerService = new WriterService(writerConfig);
    readerService = new ReaderService(readerConfig);
    writerService.start();
    readerService.start();

    HttpClient client = HttpClient.newHttpClient();
    String key = "k-" + UUID.randomUUID();
    String value = "v-" + UUID.randomUUID();
    HttpRequest writeRequest =
        HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:19081/kv"))
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    "{\"key\":\"" + key + "\",\"value\":\"" + value + "\"}"))
            .build();
    HttpResponse<String> writeResponse =
        client.send(writeRequest, HttpResponse.BodyHandlers.ofString());
    assertThat(writeResponse.statusCode()).isEqualTo(202);

    boolean found = false;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
    while (System.nanoTime() < deadline) {
      HttpRequest readRequest =
          HttpRequest.newBuilder()
              .uri(URI.create("http://localhost:19082/kv?key=" + key))
              .GET()
              .build();
      HttpResponse<String> readResponse =
          client.send(readRequest, HttpResponse.BodyHandlers.ofString());
      if (readResponse.statusCode() == 200 && readResponse.body().contains(value)) {
        found = true;
        break;
      }
      Thread.sleep(1000);
    }
    assertThat(found).isTrue();
  }

  private boolean minioAvailable() {
    try {
      HttpURLConnection connection =
          (HttpURLConnection) URI.create(ENDPOINT + "/minio/health/live").toURL().openConnection();
      connection.setConnectTimeout(1000);
      connection.setReadTimeout(1000);
      return connection.getResponseCode() == 200;
    } catch (IOException e) {
      return false;
    }
  }
}
