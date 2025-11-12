package com.slack.rocksdbsync.writer;

public final class WriterServerMain {
  public static void main(String[] args) {
    WriterConfig config = WriterConfig.fromEnv();
    try (WriterServer server = new WriterServer(config)) {
      server.start();
    }
  }
}
