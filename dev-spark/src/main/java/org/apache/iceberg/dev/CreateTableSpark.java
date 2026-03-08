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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.spark.sql.SparkSession;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputeTableStats;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;

import org.apache.iceberg.dev.MemoryTracker;
import org.apache.iceberg.dev.WriteMetrics;

/**
 * Creates Iceberg tables from SQL experiments (high/medium cardinality) with configurable bloom
 * filters. Run with: ./gradlew :iceberg-dev-spark:run
 * <p>Args: [bloom_mode] [experiment_id]
 *   - bloom_mode: "none", "row_group", "file_level"
 *   - experiment_id: "high_cardinality" or "medium_cardinality"
 */
public class CreateTableSpark {

  private static final long ROW_GROUP_SIZE_BYTES = 1024L * 1024;
  private static final int NUM_DATA_FILES = 100;

  public static String bloomModeFromArgs(String[] args) {
    if (args == null || args.length == 0) {
      return "file_level";
    }
    return args[0].trim().toLowerCase(Locale.ROOT);
  }

  public static String experimentIdFromArgs(String[] args) {
    if (args == null || args.length < 2) {
      return "high_cardinality";
    }
    return args[1].trim().toLowerCase(Locale.ROOT);
  }

  /** TBLPROPERTIES for bloom filters. Columns vary by experiment. */
  private static String tblPropertiesForExperiment(String bloomMode, String experimentId) {
    String rowGroupProp =
        "'write.parquet.row-group-size-bytes'='" + ROW_GROUP_SIZE_BYTES + "'";
    switch (bloomMode) {
      case "none":
        return "TBLPROPERTIES (" + rowGroupProp + ")";
      case "row_group":
        if ("high_cardinality".equals(experimentId)) {
          return "TBLPROPERTIES ("
              + rowGroupProp + ","
              + "'write.parquet.bloom-filter-enabled.column.id'='true',"
              + "'write.parquet.bloom-filter-enabled.column.user_id'='true',"
              + "'write.parquet.bloom-filter-enabled.column.payload'='true'"
              + ")";
        } else {
          return "TBLPROPERTIES ("
              + rowGroupProp + ","
              + "'write.parquet.bloom-filter-enabled.column.rand_id'='true',"
              + "'write.parquet.bloom-filter-enabled.column.rand_str'='true'"
              + ")";
        }
      case "file_level":
      default:
        if ("high_cardinality".equals(experimentId)) {
          return "TBLPROPERTIES ("
              + rowGroupProp + ","
              + "'write.parquet.bloom-filter-enabled.column.id'='true',"
              + "'write.parquet.bloom-filter-enabled.column.user_id'='true',"
              + "'write.parquet.bloom-filter-enabled.column.payload'='true',"
              + "'write.puffin.bloom-filter-enabled.column.id'='true',"
              + "'write.puffin.bloom-filter-enabled.column.user_id'='true',"
              + "'write.puffin.bloom-filter-enabled.column.payload'='true'"
              + ")";
        } else {
          return "TBLPROPERTIES ("
              + rowGroupProp + ","
              + "'write.parquet.bloom-filter-enabled.column.rand_id'='true',"
              + "'write.parquet.bloom-filter-enabled.column.rand_str'='true',"
              + "'write.puffin.bloom-filter-enabled.column.rand_id'='true',"
              + "'write.puffin.bloom-filter-enabled.column.rand_str'='true'"
              + ")";
        }
    }
  }

  public static void main(String[] args) throws Exception {
    String bloomMode = bloomModeFromArgs(args);
    String experimentId = experimentIdFromArgs(args);

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
    spark.sparkContext().hadoopConfiguration().set(
        "write.parquet.row-group-size-bytes", String.valueOf(ROW_GROUP_SIZE_BYTES));

    spark.sql("USE local");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS default");

    String writeQuery;
    String tableName;

    float writeDataMaxMemory = 0;
    float writeDataDuration = 0;

    if ("high_cardinality".equals(experimentId)) {
      tableName = "local.default.users_random";
      writeQuery =
          "CREATE TABLE users_random AS SELECT id, uuid() AS user_id, substr(md5(cast(rand() as string)),1,20) AS payload FROM range(100000000) DISTRIBUTE BY (id % " + NUM_DATA_FILES + ")";
      spark.sql("DROP TABLE IF EXISTS " + tableName);
      String tblProps = tblPropertiesForExperiment(bloomMode, experimentId);
      String createSql =
          "CREATE TABLE "
              + tableName
              + " USING iceberg "
              + tblProps
              + " AS SELECT id, uuid() AS user_id, substr(md5(cast(rand() as string)),1,20) AS payload FROM range(100000000) DISTRIBUTE BY (id % " + NUM_DATA_FILES + ")";
      System.out.println("Running: " + createSql);
      MemoryTracker.Result dataResult = MemoryTracker.track(() -> spark.sql(createSql));
      writeDataMaxMemory = (float) dataResult.peakMemoryMB();
      writeDataDuration = (float) dataResult.durationMs();
      System.out.println("Write: " + dataResult);
    } else {
      tableName = "local.default.events_medium_cardinality";
      writeQuery =
          "CREATE TABLE events_medium_cardinality USING iceberg PARTITIONED BY (truncate(2000000, id)) AS SELECT id, cast(rand()*4000000 as int) AS rand_id, substr(md5(cast(rand() as string)),1,20) AS rand_str FROM range(200000000)";
      spark.sql("DROP TABLE IF EXISTS " + tableName);
      String tblProps = tblPropertiesForExperiment(bloomMode, experimentId);
      String createSql =
          "CREATE TABLE "
              + tableName
              + " USING iceberg "
              + " PARTITIONED BY (truncate(2000000, id)) "
              + tblProps
              + " AS SELECT id, cast(rand()*4000000 as int) AS rand_id, substr(md5(cast(rand() as string)),1,20) AS rand_str FROM range(200000000)";
      System.out.println("Running: " + createSql);
      MemoryTracker.Result dataResult = MemoryTracker.track(() -> spark.sql(createSql));
      writeDataMaxMemory = (float) dataResult.peakMemoryMB();
      writeDataDuration = (float) dataResult.durationMs();
      System.out.println("Write: " + dataResult);
    }

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    table.refresh();

    float writePuffinMaxMemory = 0;
    float writePuffinDuration = 0;
    long puffinDiskSizeInBytes = 0;

    if (bloomMode.equals("file_level")) {
      String[] bloomCols = "high_cardinality".equals(experimentId)
          ? new String[]{"id", "user_id", "payload"}
          : new String[]{"rand_id", "rand_str"};
      MemoryTracker.TrackedResult<ComputeTableStats.Result> puffinTracked =
          MemoryTracker.trackWithResult(
              () -> SparkActions.get().computeTableStats(table).columns(bloomCols).execute());
      writePuffinMaxMemory = (float) puffinTracked.metrics().peakMemoryMB();
      writePuffinDuration = (float) puffinTracked.metrics().durationMs();
      puffinDiskSizeInBytes = puffinTracked.value().statisticsFile().fileSizeInBytes();
    }

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

    long totalManifestSizeBytes = 0;
    for (org.apache.iceberg.ManifestFile manifest : table.currentSnapshot().allManifests(table.io())) {
      totalManifestSizeBytes += manifest.length();
    }

    WriteMetrics metrics = new WriteMetrics();
    metrics.writeQuery = writeQuery;
    metrics.totalDataFiles = actualDataFiles;
    metrics.totalRowGroups = totalRowGroups;
    metrics.dataFileDiskSizeInBytes = totalDataFileSizeBytes;
    metrics.manifestDiskSizeInBytes = totalManifestSizeBytes;
    metrics.puffinDiskSizeInBytes = puffinDiskSizeInBytes;
    metrics.writeDataMaxMemory = writeDataMaxMemory;
    metrics.writePuffinMaxMemory = writePuffinMaxMemory;
    metrics.writeDataDuration = writeDataDuration;
    metrics.writePuffinDuration = writePuffinDuration;

    exportWriteMetrics(metrics);
    spark.stop();
  }

  private static void exportWriteMetrics(WriteMetrics metrics) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.writeValue(Paths.get("write-metrics.json").toFile(), metrics);
    System.out.println("Write metrics exported to write-metrics.json");
  }
}
