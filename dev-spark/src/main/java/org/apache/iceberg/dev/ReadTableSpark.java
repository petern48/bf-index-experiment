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

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;
import static org.apache.spark.sql.functions.col;

/**
 * Reads Iceberg tables locally using Spark with a Hadoop catalog.
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
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse)
            .getOrCreate();

    // To avoid the verbose INFO logs
    spark.sparkContext().setLogLevel("WARN");

    System.out.println("Reading table: " + tableName);

    // --- Query 1: id = 99 (value EXISTS in the table) ---
    System.out.println("\n=== Query 1: id = 99 (value exists in table) ===");
    System.out.println("Expected: bloom filter passes, files scanned normally");
    Dataset<Row> df = spark.table(tableName);
    Dataset<Row> filteredDf99 = df.filter("id = 99");
    // Use collect() - count() uses a different plan (e.g. WholeStageCodegen) that doesn't populate scan metrics
    List<Row> rows99 = filteredDf99.collectAsList();
    System.out.println("Row count: " + rows99.size());
    if (rows99.size() > 0) {
      System.out.println("Bloom filter result: MIGHT CONTAIN -> scan proceeded normally");
    }
    printScanMetrics(filteredDf99);
    filteredDf99.show(5, false);

    // --- Query 2: id = 9999 (value ABSENT from the table) ---
    // System.out.println("\n=== Query 2: id = 9999 (value absent from table) ===");
    // System.out.println("Expected: bloom filter fires, entire scan skipped (0 files, 0 rows)");
    // Dataset<Row> filteredDf9999 = spark.table(tableName).filter("id = 9999");
    // List<Row> rows9999 = filteredDf9999.collectAsList();
    // System.out.println("Row count: " + rows9999.size());
    // if (rows9999.isEmpty()) {
    //   System.out.println("Bloom filter result: CANNOT CONTAIN -> scan was skipped");
    // } else {
    //   System.out.println("Bloom filter result: false positive (bloom filter passed but value absent)");
    // }
    // printScanMetrics(filteredDf9999);

    // System.out.println("\nSchema:");
    // df.printSchema();


    // --- Query 3: JOIN on data = data2 ---
    System.out.println("\n=== Query 3: JOIN on data = data2 ===");
    Dataset<Row> joinedDf = df.as("a").join(df.as("b"), col("a.data").equalTo(col("b.data2")));
    List<Row> rowsJoined = joinedDf.collectAsList();
    System.out.println("Row count: " + rowsJoined.size());
    if (rowsJoined.size() > 0) {
      System.out.println("Bloom filter result: MIGHT CONTAIN -> scan proceeded normally");
    }
    printScanMetrics(joinedDf);
    joinedDf.show(5, false);

    // End Program
    spark.stop();
  }

  private static void printScanMetrics(Dataset<Row> df) {
    SparkPlan plan = df.queryExecution().executedPlan();
    List<SparkPlan> leaves = seqAsJavaListConverter(plan.collectLeaves()).asJava();
    if (leaves.isEmpty()) {
      System.out.println("Metrics: (no scan in plan)");
      return;
    }

    // Find leaf with Iceberg metrics (BatchScan)
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
    printMetric(metrics, "skippedDataFiles", "Skipped data files");
    printMetric(metrics, "totalDataFileSize", "Total data file size (bytes)");

    // Row group metrics
    printMetric(metrics, "totalRowGroups", "Total row groups");
    printMetric(metrics, "skippedRowGroups", "Skipped row groups");

    // Other metrics
    printMetric(metrics, "numSplits", "File splits read");
    printMetric(metrics, "numDeletes", "Row deletes applied");
    printMetric(metrics, "numOutputRows", "Output rows");
  }

  private static void printMetric(
      Map<String, SQLMetric> metrics, String name, String description) {
    SQLMetric m = metrics.get(name);
    if (m != null) {
      System.out.println("  " + description + ": " + m.value());
    } else {
      System.out.println("  " + name + ": (metric not found)");
    }
  }
}
