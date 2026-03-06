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

    // Get table metadata to determine file count
    org.apache.iceberg.Table table = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, tableName);
    String totalFilesStr = table.currentSnapshot().summary().get("total-data-files");
    int numFiles = totalFilesStr != null ? Integer.parseInt(totalFilesStr) : 10;

    System.out.println("Reading table: " + tableName);
    System.out.println();
    System.out.println("Data layout: " + numFiles + " files with INTERLEAVED IDs");
    System.out.println("  File 0: IDs 0, " + numFiles + ", " + (2*numFiles) + ", ... (every " + numFiles + "th starting at 0)");
    System.out.println("  File 1: IDs 1, " + (numFiles+1) + ", " + (2*numFiles+1) + ", ... (every " + numFiles + "th starting at 1)");
    System.out.println("  ... etc.");
    System.out.println("  All files have overlapping min/max ranges -> min/max CANNOT prune!");
    System.out.println("  Each ID exists in exactly ONE file -> bloom filters CAN prune!");
    System.out.println();
    System.out.println("ID mapping: id % " + numFiles + " = file number");
    System.out.println("  id=50 -> file " + (50 % numFiles) + ", id=51 -> file " + (51 % numFiles) + ", id=55 -> file " + (55 % numFiles) + ", etc.");
    System.out.println();

    // Query 1: id = 50 — exists in one file only (50 % numFiles)
    // With bloom filter: skip (numFiles-1) files. Without bloom filter: skip 0 files (min/max useless)
    int file50 = 50 % numFiles;
    TrackedQueryResult t1 =
        runQueryWithMemoryTracking(
            spark,
            tableName,
            "id = 50",
            "id=50 is in file " + file50 + ". Bloom: skip " + (numFiles - 1) + ". No bloom: skip 0");

    // Query 2: id = 9999999 — doesn't exist in any file
    // With bloom filter: skip all files. Without bloom: skip 0 (min/max sees all files match)
    TrackedQueryResult t2 =
        runQueryWithMemoryTracking(
            spark,
            tableName,
            "id = 9999999",
            "id=9999999 doesn't exist. Bloom: skip " + numFiles + ". No bloom: skip 0");

    // Query 3: id IN (50, 51, 55) — values in different files
    int file51 = 51 % numFiles;
    int file55 = 55 % numFiles;
    java.util.Set<Integer> inFiles = new java.util.TreeSet<>();
    inFiles.add(file50);
    inFiles.add(file51);
    inFiles.add(file55);
    int skipIn = numFiles - inFiles.size();
    TrackedQueryResult t3 =
        runQueryWithMemoryTracking(
            spark,
            tableName,
            "id IN (50, 51, 55)",
            "ids in files " + inFiles + ". Bloom: skip " + skipIn + ". No bloom: skip 0");

    // Query 4: id = 123456 — exists in one file
    int file123456 = 123456 % numFiles;
    TrackedQueryResult t4 =
        runQueryWithMemoryTracking(
            spark,
            tableName,
            "id = 123456",
            "id=123456 is in file " + file123456 + ". Bloom: skip " + (numFiles - 1) + ". No bloom: skip 0");

    // Print memory summary
    System.out.println("=== Memory Summary ===");
    System.out.println("  Query 1 (id = 50000): " + t1.metrics);
    System.out.println("  Query 2 (id = 9999999): " + t2.metrics);
    System.out.println("  Query 3 (id IN (50000, 550000)): " + t3.metrics);
    System.out.println("  Query 4 (id BETWEEN 150000 AND 150100): " + t4.metrics);
    System.out.println();

    Map<String, Long> scanMetrics = getScanMetrics(t3.dataFrame);
    ReadMetrics metrics = new ReadMetrics();
    metrics.skippedRowGroups = getIntMetric(scanMetrics, "skippedRowGroups");
    metrics.skippedDataFiles = getIntMetric(scanMetrics, "skippedDataFiles");
    metrics.totalRowGroups = getIntMetric(scanMetrics, "totalRowGroups");
    metrics.totalScanDataFiles = getIntMetric(scanMetrics, "totalScanDataFiles");
    metrics.resultDataFiles = getIntMetric(scanMetrics, "resultDataFiles");
    metrics.totalDataFileSizeBytes = scanMetrics.get("totalDataFileSize");
    metrics.puffinStatsFileSizeBytes = scanMetrics.get("puffinStatsFileSizeInBytes");
    metrics.puffinStatsFooterSizeBytes = scanMetrics.get("puffinStatsFooterSizeInBytes");
    metrics.maxMemoryUsage = (float) t3.metrics.peakMemoryMB();
    metrics.totalReadDuration = (float) t3.metrics.durationMs();
    // puffinReadMaxMemory: MB (stored as MB*1024 in Spark metric)
    Long puffinMaxMem = scanMetrics.get("puffinReadMaxMemory");
    metrics.readPuffinMaxMemory =
        puffinMaxMem != null && puffinMaxMem > 0 ? puffinMaxMem / 1024.0f : null;
    // manifest/puffin/data read durations: set when real Spark metrics are collected
    metrics.puffinReadDuration = null;
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
    printMetric(metrics, "skippedDataFiles", "Skipped data files (min/max)");
    printMetric(metrics, "bloomFilterSkippedDataFiles", "Skipped data files (bloom filter)");
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
