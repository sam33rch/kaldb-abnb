package com.slack.rocksdbsync.reader;

public final class ReaderMain {
  public static void main(String[] args) {
    ReaderConfig config = ReaderConfig.fromEnv();
    ReaderService service = new ReaderService(config);
    service.start();
    Runtime.getRuntime().addShutdownHook(new Thread(service::close));
  }
}
