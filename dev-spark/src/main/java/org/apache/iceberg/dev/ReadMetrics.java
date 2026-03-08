package org.apache.iceberg.dev;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReadMetrics {
  // Experiment metadata (for plotting)
  public String readQuery;
  // Counts (from scan metrics)
  public Integer manifestSkippedDataFiles;
  public Integer bloomFilterSkippedDataFiles;
  public Integer rowGroupsSkippedByFileBloomFilter;
  public Integer rowGroupsRead;
  // https://github.com/apache/iceberg/blob/ed8a16bbeb549b0286d3c229beb5a0cf165f2f4b/parquet/src/main/java/org/apache/iceberg/parquet/ReadConf.java#L106
  public Integer allSkippedRowGroups;  // includes row groups skipped by row-group bloom filters and stats
  // Memory / durations — not provided by Spark scan metrics
  public Float maxMemoryUsage;
  public Float readPuffinMaxMemory;
  public Float readPuffinDuration;
  public Float totalReadDuration;
}
