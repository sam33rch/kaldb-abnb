package com.slack.rocksdbsync.reader;

public final class ReaderMain {
  public static void main(String[] args) {
    ReaderConfig config = ReaderConfig.fromEnv();
    new ReaderService(config).runOnce();
  }
}
