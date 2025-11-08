package com.slack.rocksdbsync.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public final class JsonUtil {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonUtil() {}

  public static <T> T fromJson(byte[] data, Class<T> type) {
    try {
      return MAPPER.readValue(data, type);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse json", e);
    }
  }

  public static byte[] toJsonBytes(Object value) {
    try {
      return MAPPER.writeValueAsBytes(value);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize json", e);
    }
  }
}
