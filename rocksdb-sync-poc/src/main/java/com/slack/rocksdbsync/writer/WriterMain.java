package com.slack.rocksdbsync.writer;

public final class WriterMain {
  public static void main(String[] args) {
    WriterConfig config = WriterConfig.fromEnv();
    WriterService service = new WriterService(config);
    service.start();
    Runtime.getRuntime().addShutdownHook(new Thread(service::close));
  }
}
