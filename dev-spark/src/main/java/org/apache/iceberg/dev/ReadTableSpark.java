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

/**
 * Runs read queries for bloom filter experiments. Run with: ./gradlew :iceberg-dev-spark:runReadTable
 * <p>Args: [bloom_mode] [experiment_id] [read_query_id]
 *   - bloom_mode: "none", "row_group", "file_level"
 *   - experiment_id: "high_cardinality" or "medium_cardinality"
 *   - read_query_id: for high_cardinality: "in"; for medium_cardinality: "where", "false_positive", "range"
 */
public class ReadTableSpark {

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

  public static String readQueryIdFromArgs(String[] args) {
    if (args == null || args.length < 3) {
      return "in";
    }
    return args[2].trim().toLowerCase(Locale.ROOT);
  }

  public static void main(String[] args) throws Exception {
    // String bloomMode = bloomModeFromArgs(args);
    String experimentId = experimentIdFromArgs(args);
    String readQueryId = readQueryIdFromArgs(args);

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

    spark.sparkContext().setLogLevel("ERROR");

    // String tableName;
    String readQuery;
    Dataset<Row> df;

    if ("high_cardinality".equals(experimentId)) {
      // tableName = "local.default.users_random";
      if ("in".equals(readQueryId)) {
        List<Row> sample = spark.sql("SELECT user_id FROM local.default.users_random LIMIT 3").collectAsList();
        StringBuilder inClause = new StringBuilder();
        for (int i = 0; i < sample.size(); i++) {
          if (i > 0) {
            inClause.append(",");
          }
          String uid = sample.get(i).getString(0);
          inClause.append("'").append(uid != null ? uid.replace("'", "''") : "").append("'");
        }
        readQuery = "SELECT * FROM local.default.users_random WHERE user_id IN (" + inClause + ")";
        df = spark.sql(readQuery);
      } else {
        throw new IllegalArgumentException("Unknown read_query_id for high_cardinality: " + readQueryId);
      }
    } else {
      // tableName = "local.default.events_medium_cardinality";
      switch (readQueryId) {
        case "where":
          readQuery = "SELECT * FROM local.default.events_medium_cardinality WHERE rand_id = 123456 AND id BETWEEN 0 AND 169999999";
          df = spark.sql(readQuery);
          break;
        case "false_positive":
          readQuery = "SELECT * FROM local.default.events_medium_cardinality WHERE rand_id = -1";
          df = spark.sql(readQuery);
          break;
        case "range":
          readQuery = "SELECT * FROM local.default.events_medium_cardinality WHERE rand_id BETWEEN 1000 AND 2000";
          df = spark.sql(readQuery);
          break;
        default:
          throw new IllegalArgumentException("Unknown read_query_id for medium_cardinality: " + readQueryId);
      }
    }

    MemoryTracker.TrackedResult<List<Row>> tracked = MemoryTracker.trackWithResult(df::collectAsList);
    Map<String, Long> scanMetrics = getScanMetrics(df);

    ReadMetrics metrics = new ReadMetrics();
    metrics.readQuery = readQuery;
    metrics.allSkippedRowGroups = getIntMetric(scanMetrics, "skippedRowGroups");
    metrics.manifestSkippedDataFiles = getIntMetric(scanMetrics, "skippedDataFiles");
    metrics.bloomFilterSkippedDataFiles = getIntMetric(scanMetrics, "bloomFilterSkippedDataFiles");
    metrics.rowGroupsSkippedByFileBloomFilter = getIntMetric(scanMetrics, "rowGroupsSkippedByFileBloomFilter");
    metrics.rowGroupsRead = getIntMetric(scanMetrics, "rowGroupsRead");
    metrics.totalRowGroups = getIntMetric(scanMetrics, "totalRowGroups");
    metrics.totalScanDataFiles = getIntMetric(scanMetrics, "totalScanDataFiles");
    metrics.resultDataFiles = getIntMetric(scanMetrics, "resultDataFiles");
    metrics.totalDataFileSizeBytes = scanMetrics.get("totalDataFileSize");
    metrics.puffinStatsFileSizeBytes = scanMetrics.get("puffinStatsFileSizeInBytes");
    metrics.puffinStatsFooterSizeBytes = scanMetrics.get("puffinStatsFooterSizeInBytes");
    metrics.maxMemoryUsage = (float) tracked.metrics().peakMemoryMB();
    metrics.totalReadDuration = (float) tracked.metrics().durationMs();
    Long puffinMaxMem = scanMetrics.get("puffinReadMaxMemory");
    metrics.readPuffinMaxMemory = puffinMaxMem != null && puffinMaxMem > 0 ? puffinMaxMem / 1024.0f : null;
    Long puffinDur = scanMetrics.get("puffinReadDuration");
    metrics.readPuffinDuration = (puffinDur != null && puffinDur >= 0) ? puffinDur.floatValue() : null;

    exportReadMetrics(metrics);
    spark.stop();
  }

  private static void exportReadMetrics(ReadMetrics metrics) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.writeValue(Paths.get("read-metrics.json").toFile(), metrics);
    System.out.println("Read metrics exported to read-metrics.json");
  }

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
}
