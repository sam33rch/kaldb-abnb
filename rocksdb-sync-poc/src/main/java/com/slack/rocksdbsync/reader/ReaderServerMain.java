package com.slack.rocksdbsync.reader;

public final class ReaderServerMain {
  public static void main(String[] args) {
    ReaderConfig config = ReaderConfig.fromEnv();
    try (ReaderServer server = new ReaderServer(config)) {
      server.start();
    }
  }
}
