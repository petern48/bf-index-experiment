package org.apache.iceberg.dev;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WriteMetrics {
  // Counts
  public Integer totalRowGroups;
  public Integer totalDataFiles;
  // Disk sizes per file type
  public Long dataFileDiskSizeInBytes;
  public Long manifestDiskSizeInBytes;
  public Long puffinDiskSizeInBytes;
  public Long puffinFooterSizeInBytes;
  // Peak memory (MB) per write phase — no extra work, just wraps existing operations
  public Float writeDataMaxMemory;
  public Float writePuffinMaxMemory;
  public Float maxMemoryUsage;
  // Duration (ms) per write phase
  public Float writeDataDuration;
  public Float writePuffinDuration;
  public Float puffinWriteDuration;
  public Float manifestWriteDuration;
  public Float datafileWriteDuration;
  public Float totalWriteDuration;
}
