package com.slack.rocksdbsync.common;

import java.util.ArrayList;
import java.util.List;

public final class SnapshotDescriptors {
  private SnapshotDescriptors() {}

  public enum SnapshotFileType {
    SST,
    MANIFEST,
    CURRENT
  }

  public static class SnapshotFileEntry {
    public String name;
    public String s3Uri;
    public SnapshotFileType type;

    public SnapshotFileEntry() {}

    public SnapshotFileEntry(String name, String s3Uri, SnapshotFileType type) {
      this.name = name;
      this.s3Uri = s3Uri;
      this.type = type;
    }
  }

  public static class SnapshotManifest {
    public String manifestId;
    public String generatedAt;
    public List<SnapshotFileEntry> files = new ArrayList<>();

    public SnapshotManifest() {}

    public SnapshotManifest(String manifestId, String generatedAt, List<SnapshotFileEntry> files) {
      this.manifestId = manifestId;
      this.generatedAt = generatedAt;
      this.files = files == null ? new ArrayList<>() : files;
    }
  }

  public static class SnapshotHistoryEntry {
    public String manifestId;
    public String snapshotManifestUri;
    public String publishedAt;

    public SnapshotHistoryEntry() {}

    public SnapshotHistoryEntry(String manifestId, String snapshotManifestUri, String publishedAt) {
      this.manifestId = manifestId;
      this.snapshotManifestUri = snapshotManifestUri;
      this.publishedAt = publishedAt;
    }
  }

  public static class SnapshotPointer {
    public int version;
    public String latestManifestId;
    public String snapshotManifestUri;
    public List<SnapshotHistoryEntry> previous = new ArrayList<>();

    public SnapshotPointer() {}
  }
}
