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

// For writing metrics to a json file
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.nio.file.Paths;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputeTableStats;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;

import org.apache.iceberg.dev.MemoryTracker;
import org.apache.iceberg.dev.WriteMetrics;

/**
 * Spark equivalent of CreateTable.java - creates Iceberg tables locally using Spark APIs with a
 * Hadoop catalog, writes data, enables bloom filters, and computes NDV statistics.
 *
 * <p>Run with: ./gradlew :iceberg-dev-spark:run
 * <p>Optional args: [bloom_mode] [num_files] [records_per_file]
 *   - bloom_mode: one of "none", "row_group", "file_level" (default: "file_level")
 *   - num_files: number of data files to create (default: 10)
 *   - records_per_file: rows per file (default: 100000)
 *
 * <p>Example for larger dataset: ./gradlew run -PrunArgs="file_level,50,500000"
 *   Creates 50 files with 500K rows each = 25M total rows
 *
 * <p>Requires Spark 4.0 or 4.1 to be in the build (default: 4.1). Tables are stored under
 * ./build/warehouse by default. Use ICEBERG_WAREHOUSE env var to override.
 */
public class CreateTableSpark {

  /** Bloom mode: none, row_group (Parquet only), file_level (Parquet + Puffin file-level). */
  public static String bloomModeFromArgs(String[] args) {
    if (args == null || args.length == 0) {
      return "file_level";
    }
    String mode = args[0].trim().toLowerCase(Locale.ROOT);
    return mode.isEmpty() ? "file_level" : mode;
  }

  /** Number of data files to create. */
  public static int numFilesFromArgs(String[] args) {
    if (args == null || args.length < 2) {
      return 10;
    }
    try {
      return Integer.parseInt(args[1].trim());
    } catch (NumberFormatException e) {
      return 10;
    }
  }

  /** Number of records per file. */
  public static int recordsPerFileFromArgs(String[] args) {
    if (args == null || args.length < 3) {
      return 100_000;
    }
    try {
      return Integer.parseInt(args[2].trim());
    } catch (NumberFormatException e) {
      return 100_000;
    }
  }

  /** Parquet row group size (1 MiB) so we get many row groups per file for meaningful row-group BF charts. */
  private static final long ROW_GROUP_SIZE_BYTES = 1024L * 1024;

  /** TBLPROPERTIES fragment for CREATE TABLE (no leading/trailing comma). */
  public static String tblPropertiesForBloomMode(String bloomMode) {
    String rowGroupProp =
        "'write.parquet.row-group-size-bytes'='" + ROW_GROUP_SIZE_BYTES + "'";
    switch (bloomMode) {
      case "none":
        return "TBLPROPERTIES (" + rowGroupProp + ")";
      case "row_group":
        return "TBLPROPERTIES ("
            + rowGroupProp + ","
            + "'write.parquet.bloom-filter-enabled.column.id'='true',"
            + "'write.parquet.bloom-filter-enabled.column.data'='true'"
            + ")";
      case "file_level":
      default:
        // Enable both row group-level and file-level bloom filters
        return "TBLPROPERTIES ("
            + rowGroupProp + ","
            + "'write.parquet.bloom-filter-enabled.column.id'='true',"
            + "'write.parquet.bloom-filter-enabled.column.data'='true',"
            + "'write.puffin.bloom-filter-enabled.column.id'='true',"
            + "'write.puffin.bloom-filter-enabled.column.data'='true'"
            + ")";
    }
  }

  public static void main(String[] args) throws Exception {
    String bloomMode = bloomModeFromArgs(args);
    int numDataFiles = numFilesFromArgs(args);
    int recordsPerFile = recordsPerFileFromArgs(args);

    String warehouse =
        System.getenv("ICEBERG_WAREHOUSE") != null
            ? System.getenv("ICEBERG_WAREHOUSE")
            : "file:" + Paths.get("build", "warehouse").toAbsolutePath();

    SparkSession spark =
        SparkSession.builder()
            .appName("CreateTableSpark")
            .master("local[2]")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse)
            .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    // Spark's Iceberg write path may ignore the table property; set Hadoop config so writers can use it.
    spark.sparkContext().hadoopConfiguration().set(
        "write.parquet.row-group-size-bytes", String.valueOf(ROW_GROUP_SIZE_BYTES));

    String tableName = "local.default.sample_table_spark";

    spark.sql("USE local");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS default");
    spark.sql("DROP TABLE IF EXISTS " + tableName);

    String tblProps = tblPropertiesForBloomMode(bloomMode);
    String createSql =
        "CREATE TABLE "
            + tableName
            + " (id BIGINT, data STRING, created_at TIMESTAMP) "
            + "USING iceberg "
            + (tblProps.isEmpty() ? "" : " " + tblProps);
    spark.sql(createSql);

    System.out.println("Created table: " + tableName);
    System.out.println("Bloom mode: " + bloomMode);
    System.out.println("Configuration: " + numDataFiles + " files x " + recordsPerFile + " rows/file");

    // Sequential ID per file + uncorrelated data (fair benchmark for bloom filters).
    // - Sequential IDs: file k has ids [k*R, (k+1)*R), so min/max can prune on id.
    // - Uncorrelated data: data = "item_" + (id * 7919 % 10000000), so string values are spread
    //   across files and min/max on data rarely help; bloom filters are the main win for string filters.
    long totalRecords = (long) numDataFiles * recordsPerFile;
    final long dataHashMod = 10_000_000L;
    final int dataHashMultiplier = 7919;

    System.out.println("Generating " + totalRecords + " records across " + numDataFiles + " files...");
    System.out.println("Estimated total data size: ~" + (totalRecords * 25 / 1_000_000) + " MB (before compression)");
    System.out.println("Sequential ID per file + uncorrelated data (fair benchmark):");
    System.out.println("  File 0: IDs [0, " + recordsPerFile + "), file 1: [" + recordsPerFile + ", " + (2 * recordsPerFile) + "), ...");
    System.out.println("  data = 'item_' + (id * " + dataHashMultiplier + " % " + dataHashMod + ") -> min/max on data rarely prune");

    Dataset<Row> df =
        spark.range(totalRecords)
            .withColumn(
                "data",
                concat(
                    lit("item_"),
                    expr("cast((id * " + dataHashMultiplier + " % " + dataHashMod + ") as string)")))
            .withColumn(
                "created_at",
                expr("timestampadd(SECOND, id, timestamp('2024-01-15T10:00:00Z'))"))
            .repartition(numDataFiles, expr("id / " + recordsPerFile))
            .sortWithinPartitions("id");


    // Phase 1: data file write — single Spark job, no extra work
    MemoryTracker.Result datafileResult =
        MemoryTracker.track(
            () -> {
              try {
                df.writeTo(tableName).append();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    float writeDataMaxMemory = (float) datafileResult.peakMemoryMB();
    float writeDataDuration = (float) datafileResult.durationMs();
    System.out.println("Data file write: " + datafileResult);

    System.out.println(
        "Wrote " + totalRecords + " records in " + numDataFiles + " data files (sequential ID per file)");

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    table.refresh();

    // Phase 2: puffin file write (ComputeTableStats) — no extra work
    // Metrics default to zero for non-file_level bloom modes
    float writePuffinMaxMemory = 0;
    float writePuffinDuration = 0;
    long puffinDiskSizeInBytes = 0;
    long puffinFooterSizeInBytes = 0;

    if (bloomMode.equals("file_level")) {
        MemoryTracker.TrackedResult<ComputeTableStats.Result> puffinTracked =
            MemoryTracker.trackWithResult(
                () ->
                    SparkActions.get()
                        .computeTableStats(table)
                        .columns("id", "data")
                        .execute());
    
        System.out.println("Puffin write: " + puffinTracked.metrics());

        ComputeTableStats.Result result = puffinTracked.value();

        // Save metrics
        writePuffinMaxMemory = (float) puffinTracked.metrics().peakMemoryMB();
        writePuffinDuration = (float) puffinTracked.metrics().durationMs();
        puffinDiskSizeInBytes = result.statisticsFile().fileSizeInBytes();
        puffinFooterSizeInBytes = result.statisticsFile().fileFooterSizeInBytes();

        long ndvBlobs = result.statisticsFile().blobMetadata().stream()
            .filter(m -> m.properties().containsKey("ndv"))
            .count();
        long bloomBlobs = result.statisticsFile().blobMetadata().stream()
            .filter(m -> m.properties().containsKey("data-file-path"))
            .count();
        System.out.println("Puffin stats file: " + result.statisticsFile().path());
        System.out.println("  NDV blobs:               " + ndvBlobs);
        System.out.println("  File bloom filter blobs: " + bloomBlobs);
        if (bloomBlobs > 0) {
        System.out.println("  Sample bloom filter files:");
        result.statisticsFile().blobMetadata().stream()
            .filter(m -> m.properties().containsKey("data-file-path"))
            .limit(3)
            .forEach(m -> System.out.println("    " + m.properties().get("data-file-path")));
        }
    }

    // Count data files, row groups, and total data file disk size from manifest metadata (no file I/O)
    int actualDataFiles = 0;
    int totalRowGroups = 0;
    long totalDataFileSizeBytes = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        actualDataFiles++;
        totalDataFileSizeBytes += task.file().fileSizeInBytes();
        List<Long> offsets = task.file().splitOffsets();
        if (offsets != null) {
          totalRowGroups += offsets.size();
        }
      }
    }

    // Sum manifest file sizes from snapshot metadata (no file I/O beyond manifest listing)
    long totalManifestSizeBytes = 0;
    for (org.apache.iceberg.ManifestFile manifest : table.currentSnapshot().allManifests(table.io())) {
      totalManifestSizeBytes += manifest.length();
    }

    System.out.println("Table stats: " + actualDataFiles + " data files, " + totalRowGroups + " row groups");
    System.out.println("  Data file disk size: " + totalDataFileSizeBytes + " bytes");
    System.out.println("  Manifest disk size:  " + totalManifestSizeBytes + " bytes");

    // Assertions for validation (linters complain if we use assert statements)
    if (totalRowGroups <= 0) { throw new IllegalStateException("Total row groups (" + totalRowGroups + ") is 0"); }
    // NOTE: this doesn't actually set the exact number of datafiles we specified any more after changing to purely a spark query
    // if (actualDataFiles != numDataFiles) { throw new IllegalStateException("Actual data files (" + actualDataFiles + ") != expected data files (" + numDataFiles + ")"); }
    WriteMetrics metrics = new WriteMetrics();
    metrics.totalDataFiles = actualDataFiles;
    metrics.totalRowGroups = totalRowGroups;
    metrics.dataFileDiskSizeInBytes = totalDataFileSizeBytes;
    metrics.manifestDiskSizeInBytes = totalManifestSizeBytes;
    metrics.puffinDiskSizeInBytes = puffinDiskSizeInBytes;
    metrics.puffinFooterSizeInBytes = puffinFooterSizeInBytes;
    metrics.writeDataMaxMemory = writeDataMaxMemory;
    metrics.writePuffinMaxMemory = writePuffinMaxMemory;
    metrics.writeDataDuration = writeDataDuration;
    metrics.writePuffinDuration = writePuffinDuration;
    metrics.maxMemoryUsage =
        Math.max(writeDataMaxMemory, writePuffinMaxMemory);

    exportWriteMetrics(metrics);

    spark.stop();
  }

  private static void exportWriteMetrics(WriteMetrics metrics) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    String outputPath = "write-metrics.json";
    mapper.writeValue(Paths.get(outputPath).toFile(), metrics);
    System.out.println("Write metrics exported to " + outputPath);
  }
}
