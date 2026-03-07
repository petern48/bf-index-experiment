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
  public Integer rowGroupsRead;
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
  // No-match query (data = 'item_10000000'): guaranteed non-existent value within min/max range,
  // so only bloom filters can prune. Best-case scenario for bloom filter benefit.
  public Float noMatchMaxMemoryUsage;
  public Float noMatchReadPuffinMaxMemory;
  public Float noMatchTotalReadDuration;
}
