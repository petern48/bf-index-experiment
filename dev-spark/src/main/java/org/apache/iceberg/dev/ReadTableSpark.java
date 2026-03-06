/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use it except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.dev;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

// For writing metrics to a json file
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;

import org.apache.iceberg.dev.ReadMetrics;

/**
 * Reads Iceberg tables locally using Spark with a Hadoop catalog and demonstrates file-level bloom
 * filter pruning.
 *
 * <p>Run with: ./gradlew :iceberg-dev-spark:runReadTable
 *
 * <p>Optionally pass a table name as argument, e.g.:
 * ./gradlew :iceberg-dev-spark:runReadTable --args="local.default.sample_table_spark"
 *
 * <p>Requires Spark 4.0 or 4.1 to be in the build (default: 4.1). Tables are stored under
 * ./build/warehouse by default. Use ICEBERG_WAREHOUSE env var to override.
 */
public class ReadTableSpark {

  private static final String DEFAULT_TABLE = "local.default.sample_table_spark";

  /** Must match CreateTableSpark: data = "item_" + (id * 7919 % 10_000_000). */
  private static final long DATA_HASH_MOD = 10_000_000L;
  private static final int DATA_HASH_MULTIPLIER = 7919;

  private static String dataValueForId(long id) {
    long h = (id * DATA_HASH_MULTIPLIER) % DATA_HASH_MOD;
    if (h < 0) {
      h += DATA_HASH_MOD;
    }
    return "item_" + h;
  }

  /** Result of a query run with memory/duration tracking; includes the Dataset for scan metrics. */
  private static class TrackedQueryResult {
    final MemoryTracker.Result metrics;
    final Dataset<Row> dataFrame;

    TrackedQueryResult(MemoryTracker.Result metrics, Dataset<Row> dataFrame) {
      this.metrics = metrics;
      this.dataFrame = dataFrame;
    }
  }

  public static void main(String[] args) throws Exception {
    String tableName = args.length > 0 ? args[0] : DEFAULT_TABLE;

    String warehouse =
        System.getenv("ICEBERG_WAREHOUSE") != null
            ? System.getenv("ICEBERG_WAREHOUSE")
            : "file:" + Paths.get("build", "warehouse").toAbsolutePath();

    SparkSession spark =
        SparkSession.builder()
            .appName("ReadTableSpark")
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse)
            .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    // Get table metadata: file count and total records (sequential ID per file)
    org.apache.iceberg.Table table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, tableName);
    Map<String, String> summary = table.currentSnapshot().summary();
    String totalFilesStr = summary.get("total-data-files");
    int numFiles = totalFilesStr != null ? Integer.parseInt(totalFilesStr) : 10;
    String totalRecordsStr = summary.get("total-records");
    long totalRecords = totalRecordsStr != null ? Long.parseLong(totalRecordsStr) : (long) numFiles * 100_000;
    long recordsPerFile = numFiles > 0 ? totalRecords / numFiles : 100_000;

    // Probe values: mid id, three ids in different files, and their data strings (same formula as CreateTableSpark)
    long midId = totalRecords / 2;
    long id1 = recordsPerFile / 2;
    long id2 = recordsPerFile + recordsPerFile / 2;
    long id3 = 2 * recordsPerFile + recordsPerFile / 2;
    String dataMid = dataValueForId(midId);
    String data1 = dataValueForId(id1);
    String data2 = dataValueForId(id2);
    String data3 = dataValueForId(id3);

    System.out.println("Reading table: " + tableName);
    System.out.println();
    System.out.println("Data layout: " + numFiles + " files, sequential ID per file, uncorrelated data");
    System.out.println("  File 0: IDs [0, " + recordsPerFile + "), file 1: [" + recordsPerFile + ", " + (2 * recordsPerFile) + "), ...");
    System.out.println("  data = 'item_' + (id * " + DATA_HASH_MULTIPLIER + " % " + DATA_HASH_MOD + ") -> string filters need bloom to prune");
    System.out.println();

    // 1. Int equality: id = mid (one row, one file; min/max and bloom both prune)
    TrackedQueryResult t1 =
        runQueryWithMemoryTracking(
            spark, tableName, "id = " + midId,
            "Int eq: id=" + midId + " in one file; min/max and bloom can prune");

    // 2. Int IN: values in different files
    TrackedQueryResult t2 =
        runQueryWithMemoryTracking(
            spark, tableName, "id IN (" + id1 + ", " + id2 + ", " + id3 + ")",
            "Int IN: ids in 3 files; min/max and bloom can prune");

    // 3. String equality: data = 'item_...' (min/max rarely help; bloom is main win — used for metrics export)
    TrackedQueryResult t3 =
        runQueryWithMemoryTracking(
            spark, tableName, "data = '" + dataMid.replace("'", "''") + "'",
            "String eq: data='" + dataMid + "'; bloom prunes, min/max rarely");

    // 4. String IN
    TrackedQueryResult t4 =
        runQueryWithMemoryTracking(
            spark, tableName,
            "data IN ('" + data1.replace("'", "''") + "', '" + data2.replace("'", "''") + "', '" + data3.replace("'", "''") + "')",
            "String IN: 3 values; bloom prunes");

    // 5. No match: id = -1 (bloom skips all files; no bloom reads all)
    TrackedQueryResult t5 =
        runQueryWithMemoryTracking(
            spark, tableName, "id = -1",
            "No match: id=-1; bloom skips all " + numFiles + " files, no bloom reads all");

    System.out.println("=== Memory Summary ===");
    System.out.println("  1 id=" + midId + ": " + t1.metrics);
    System.out.println("  2 id IN (" + id1 + "," + id2 + "," + id3 + "): " + t2.metrics);
    System.out.println("  3 data='" + dataMid + "': " + t3.metrics);
    System.out.println("  4 data IN (3 values): " + t4.metrics);
    System.out.println("  5 id=-1: " + t5.metrics);
    System.out.println();

    // Export metrics from representative query: string equality (best for showing bloom impact)
    Map<String, Long> scanMetrics = getScanMetrics(t3.dataFrame);
    ReadMetrics metrics = new ReadMetrics();
    metrics.allSkippedRowGroups = getIntMetric(scanMetrics, "skippedRowGroups");
    metrics.manifestSkippedDataFiles = getIntMetric(scanMetrics, "skippedDataFiles");
    metrics.bloomFilterSkippedDataFiles = getIntMetric(scanMetrics, "bloomFilterSkippedDataFiles");
    metrics.rowGroupsSkippedByFileBloomFilter =
        getIntMetric(scanMetrics, "rowGroupsSkippedByFileBloomFilter");
    metrics.rowGroupsRead = getIntMetric(scanMetrics, "rowGroupsRead");
    metrics.totalRowGroups = getIntMetric(scanMetrics, "totalRowGroups");
    metrics.totalScanDataFiles = getIntMetric(scanMetrics, "totalScanDataFiles");
    metrics.resultDataFiles = getIntMetric(scanMetrics, "resultDataFiles");  // NOTE: this is misleading. this is (total - manifestSkipped) before BF skipping. do not use
    metrics.totalDataFileSizeBytes = scanMetrics.get("totalDataFileSize");
    metrics.puffinStatsFileSizeBytes = scanMetrics.get("puffinStatsFileSizeInBytes");
    metrics.puffinStatsFooterSizeBytes = scanMetrics.get("puffinStatsFooterSizeInBytes");
    metrics.maxMemoryUsage = (float) t3.metrics.peakMemoryMB();
    metrics.totalReadDuration = (float) t3.metrics.durationMs();
    // puffinReadMaxMemory: MB (stored as MB*1024 in Spark metric)
    Long puffinMaxMem = scanMetrics.get("puffinReadMaxMemory");
    metrics.readPuffinMaxMemory =
        puffinMaxMem != null && puffinMaxMem > 0 ? puffinMaxMem / 1024.0f : null;
    // readPuffinDuration: ms from Spark metric "puffinReadDuration" (-1 when no puffin read)
    Long puffinDur = scanMetrics.get("puffinReadDuration");
    metrics.readPuffinDuration =
        (puffinDur != null && puffinDur >= 0) ? puffinDur.floatValue() : null;
    metrics.manifestReadDuration = null;
    metrics.datafileReadDuration = null;

    exportReadMetrics(metrics);

    spark.stop();
  }

  private static void exportReadMetrics(ReadMetrics metrics) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    String outputPath = "read-metrics.json";
    mapper.writeValue(Paths.get(outputPath).toFile(), metrics);
    System.out.println("Read Metrics exported to " + outputPath);
  }

  private static TrackedQueryResult runQueryWithMemoryTracking(
      SparkSession spark, String tableName, String predicate, String description) {
    System.out.println("=== Query: " + predicate + " ===");
    System.out.println("    " + description);

    Dataset<Row> df = spark.table(tableName).filter(predicate);

    // Track memory and duration while executing the query
    MemoryTracker.TrackedResult<List<Row>> tracked =
        MemoryTracker.trackWithResult(df::collectAsList);

    List<Row> rows = tracked.value();
    MemoryTracker.Result memResult = tracked.metrics();

    System.out.println("  Row count: " + rows.size());
    System.out.println(
        "  Peak memory: " + String.format(Locale.ROOT, "%.2f MB", memResult.peakMemoryMB()));
    System.out.println(
        "  Duration: " + String.format(Locale.ROOT, "%.2f ms", memResult.durationMs()));

    printScanMetrics(df);
    System.out.println();

    return new TrackedQueryResult(memResult, df);
  }

  /** Extracts Iceberg scan metric values from the executed plan of the given Dataset. */
  private static Map<String, Long> getScanMetrics(Dataset<Row> df) {
    Map<String, Long> out = new HashMap<>();
    SparkPlan plan = df.queryExecution().executedPlan();
    List<SparkPlan> leaves = seqAsJavaListConverter(plan.collectLeaves()).asJava();
    if (leaves.isEmpty()) {
      return out;
    }
    Map<String, SQLMetric> metrics = null;
    for (SparkPlan leaf : leaves) {
      Map<String, SQLMetric> m = mapAsJavaMapConverter(leaf.metrics()).asJava();
      if (m.containsKey("totalDataFileSize") || m.containsKey("resultDataFiles")
          || m.containsKey("totalRowGroups")) {
        metrics = m;
        break;
      }
    }
    if (metrics == null) {
      metrics = mapAsJavaMapConverter(leaves.get(0).metrics()).asJava();
    }
    for (Map.Entry<String, SQLMetric> e : metrics.entrySet()) {
      if (e.getValue() != null) {
        out.put(e.getKey(), (long) e.getValue().value());
      }
    }
    return out;
  }

  private static Integer getIntMetric(Map<String, Long> scanMetrics, String name) {
    Long v = scanMetrics.get(name);
    return v != null ? v.intValue() : null;
  }

  private static void printScanMetrics(Dataset<Row> df) {
    SparkPlan plan = df.queryExecution().executedPlan();
    List<SparkPlan> leaves = seqAsJavaListConverter(plan.collectLeaves()).asJava();
    if (leaves.isEmpty()) {
      System.out.println("  Metrics: (no scan in plan)");
      return;
    }

    // Find the leaf node that carries Iceberg scan metrics.
    Map<String, SQLMetric> metrics = null;
    for (SparkPlan leaf : leaves) {
      Map<String, SQLMetric> m = mapAsJavaMapConverter(leaf.metrics()).asJava();
      if (m.containsKey("totalDataFileSize") || m.containsKey("resultDataFiles")
          || m.containsKey("totalRowGroups")) {
        metrics = m;
        break;
      }
    }
    if (metrics == null) {
      metrics = mapAsJavaMapConverter(leaves.get(0).metrics()).asJava();
    }

    System.out.println("Scan metrics:");

    // Data file metrics
    printMetric(metrics, "totalScanDataFiles", "Total data files");
    printMetric(metrics, "resultDataFiles", "Result data files");
    printMetric(metrics, "skippedDataFiles", "Skipped data files (manifest)");
    printMetric(metrics, "bloomFilterSkippedDataFiles", "Skipped data files (bloom filter)");
    printMetric(
        metrics, "rowGroupsSkippedByFileBloomFilter", "Skipped row groups (file-level bloom filter)");
    printMetric(metrics, "rowGroupsRead", "Row groups actually read");
    printMetric(metrics, "totalDataFileSize", "Total data file size (bytes)");

    // Row group metrics
    printMetric(metrics, "totalRowGroups", "Total row groups");
    printMetric(metrics, "skippedRowGroups", "Skipped row groups");

    // Puffin statistics file metrics
    printMetric(
        metrics, "puffinStatsFileSizeInBytes", "Puffin statistics file size (bytes)");
    printMetric(
        metrics,
        "puffinStatsFooterSizeInBytes",
        "Puffin statistics file footer size (bytes)");

    // Other metrics
    printMetric(metrics, "numSplits", "File splits read");
    printMetric(metrics, "numOutputRows", "Output rows");
  }

  private static void printMetric(
      Map<String, SQLMetric> metrics, String name, String description) {
    SQLMetric m = metrics.get(name);
    if (m != null) {
      System.out.println("    " + description + ": " + m.value());
    } else {
      System.out.println("    Metric " + name + " NOT FOUND");
    }
  }
}
