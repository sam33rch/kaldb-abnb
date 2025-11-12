package com.slack.rocksdbsync.tools;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads and displays the contents of a RocksDB CLOUDMANIFEST file.
 *
 * Usage: java CloudManifestReader <path-to-CLOUDMANIFEST>
 */
public class CloudManifestReader {

  public static class EpochEntry {
    public final String epoch;
    public final long timestamp;
    public final int flags;
    public final int position;

    public EpochEntry(String epoch, long timestamp, int flags, int position) {
      this.epoch = epoch;
      this.timestamp = timestamp;
      this.flags = flags;
      this.position = position;
    }

    @Override
    public String toString() {
      return String.format("Epoch: %s, Timestamp: %d (%s), Flags: 0x%04x, Position: %d",
          epoch, timestamp, Instant.ofEpochMilli(timestamp / 1000000), flags, position);
    }
  }

  public static class CloudManifest {
    public final int magic;
    public final int version;
    public final List<EpochEntry> entries;
    public final int totalSize;

    public CloudManifest(int magic, int version, List<EpochEntry> entries, int totalSize) {
      this.magic = magic;
      this.version = version;
      this.entries = entries;
      this.totalSize = totalSize;
    }

    public String getCurrentEpoch() {
      return entries.isEmpty() ? null : entries.get(entries.size() - 1).epoch;
    }
  }

  public static CloudManifest readCloudManifest(Path path) throws IOException {
    byte[] data = Files.readAllBytes(path);
    int totalSize = data.length;

    if (data.length < 8) {
      throw new IOException("CLOUDMANIFEST too small: " + data.length + " bytes");
    }

    int offset = 0;

    // Read magic (4 bytes, little-endian)
    int magic = readIntLE(data, offset);
    offset += 4;

    // Read version (1 byte)
    int version = data[offset] & 0xFF;
    offset += 1;

    System.out.println("Magic: 0x" + Integer.toHexString(magic));
    System.out.println("Version: " + version);
    System.out.println();

    // Parse entries - try to find epoch strings (16 hex chars)
    List<EpochEntry> entries = new ArrayList<>();

    while (offset < data.length) {
      // Look for pattern: 0x01 followed by length 0x10 (16) then 16-char hex string
      if (offset + 2 < data.length && data[offset] == 0x01) {
        int entryMarker = data[offset] & 0xFF;
        int nextByte = data[offset + 1] & 0xFF;

        // Check if next byte looks like epoch length (0x01 or 0x02 for entry number)
        if (offset + 3 < data.length && data[offset + 2] == 0x10) {
          // Found epoch length marker (0x10 = 16 bytes)
          int epochLen = data[offset + 2] & 0xFF;
          offset += 3;

          if (offset + epochLen <= data.length) {
            // Read epoch string
            String epoch = new String(data, offset, epochLen, StandardCharsets.UTF_8);
            int epochPosition = offset;
            offset += epochLen;

            // Try to read additional metadata if available
            long timestamp = 0;
            int flags = 0;

            // Read preceding bytes as timestamp (if available)
            if (epochPosition >= 11) {
              timestamp = readLongLE(data, epochPosition - 11);
            }

            // Read flags after epoch (if available)
            if (offset + 2 <= data.length) {
              // Flags might be before epoch, skip for now
            }

            entries.add(new EpochEntry(epoch, timestamp, flags, epochPosition));
          }
        } else {
          offset++;
        }
      } else {
        offset++;
      }
    }

    return new CloudManifest(magic, version, entries, totalSize);
  }

  private static int readIntLE(byte[] data, int offset) {
    return (data[offset] & 0xFF) |
           ((data[offset + 1] & 0xFF) << 8) |
           ((data[offset + 2] & 0xFF) << 16) |
           ((data[offset + 3] & 0xFF) << 24);
  }

  private static long readLongLE(byte[] data, int offset) {
    return (data[offset] & 0xFFL) |
           ((data[offset + 1] & 0xFFL) << 8) |
           ((data[offset + 2] & 0xFFL) << 16) |
           ((data[offset + 3] & 0xFFL) << 24) |
           ((data[offset + 4] & 0xFFL) << 32) |
           ((data[offset + 5] & 0xFFL) << 40) |
           ((data[offset + 6] & 0xFFL) << 48) |
           ((data[offset + 7] & 0xFFL) << 56);
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: CloudManifestReader <path-to-CLOUDMANIFEST>");
      System.err.println("Example: CloudManifestReader ~/temp/rocksdb-cloud-reader/CLOUDMANIFEST-cloud-poc");
      System.exit(1);
    }

    Path manifestPath = Paths.get(args[0]).toAbsolutePath();

    if (!Files.exists(manifestPath)) {
      System.err.println("File not found: " + manifestPath);
      System.exit(1);
    }

    try {
      CloudManifest manifest = readCloudManifest(manifestPath);

      System.out.println("=== CLOUDMANIFEST Contents ===");
      System.out.println("File: " + manifestPath);
      System.out.println("Size: " + manifest.totalSize + " bytes");
      System.out.println("Magic: 0x" + Integer.toHexString(manifest.magic));
      System.out.println("Version: " + manifest.version);
      System.out.println();

      System.out.println("Epochs: " + manifest.entries.size());
      for (int i = 0; i < manifest.entries.size(); i++) {
        EpochEntry entry = manifest.entries.get(i);
        String marker = (i == manifest.entries.size() - 1) ? " (CURRENT)" : "";
        System.out.println("  [" + (i + 1) + "]" + marker + " " + entry.epoch);

        // Check if corresponding MANIFEST file exists
        Path parentDir = manifestPath.getParent();
        if (parentDir != null) {
          Path manifestFile = parentDir.resolve("MANIFEST-" + entry.epoch);
          if (Files.exists(manifestFile)) {
            long size = Files.size(manifestFile);
            System.out.println("      MANIFEST file: " + size + " bytes");
          } else {
            System.out.println("      MANIFEST file: NOT FOUND");
          }
        }
      }

      System.out.println();
      if (!manifest.entries.isEmpty()) {
        System.out.println("Current (active) epoch: " + manifest.getCurrentEpoch());
      }

    } catch (IOException e) {
      System.err.println("Error reading CLOUDMANIFEST: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}
