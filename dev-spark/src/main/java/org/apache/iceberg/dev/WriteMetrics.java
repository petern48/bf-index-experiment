package org.apache.iceberg.dev;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WriteMetrics {
  // Experiment metadata (for plotting)
  public String writeQuery;
  // Counts
  public Integer totalRowGroups;
  public Integer totalDataFiles;
  // Disk sizes per file type
  public Long dataFileDiskSizeInBytes;
  public Long manifestDiskSizeInBytes;
  public Long puffinDiskSizeInBytes;
  // Peak memory (MB) per write phase — no extra work, just wraps existing operations
  public Float writeDataMaxMemory;  // manifest + data file writing
  public Float writePuffinMaxMemory;
  // Duration (ms) per write phase
  public Float writeDataDuration;
  public Float writePuffinDuration;
}
