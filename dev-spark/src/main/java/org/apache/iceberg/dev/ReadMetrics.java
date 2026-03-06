package org.apache.iceberg.dev;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReadMetrics {
  // Counts (from scan metrics)
  public Integer totalScanDataFiles;
  public Integer resultDataFiles;
  public Integer manifestSkippedDataFiles;
  public Integer bloomFilterSkippedDataFiles;
  public Integer rowGroupsSkippedByFileBloomFilter;
  public Long totalDataFileSizeBytes;
  public Integer totalRowGroups;
  public Integer allSkippedRowGroups;
  public Long puffinStatsFileSizeBytes;
  public Long puffinStatsFooterSizeBytes;
  public Integer numSplits;
  public Long numOutputRows;
  // Memory / durations — not provided by Spark scan metrics
  public Float maxMemoryUsage;
  public Float readPuffinMaxMemory;
  public Float readPuffinDuration;
  public Float manifestReadDuration;
  public Float datafileReadDuration;
  public Float totalReadDuration;
}
