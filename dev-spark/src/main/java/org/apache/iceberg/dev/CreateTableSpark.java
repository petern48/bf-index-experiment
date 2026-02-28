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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputeTableStats;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;

/**
 * Spark equivalent of CreateTable.java - creates Iceberg tables locally using Spark APIs with a
 * Hadoop catalog, writes data, enables bloom filters, and computes NDV statistics.
 *
 * <p>Run with: ./gradlew :iceberg-dev-spark:run
 *
 * <p>Requires Spark 4.0 or 4.1 to be in the build (default: 4.1). Tables are stored under
 * ./build/warehouse by default. Use ICEBERG_WAREHOUSE env var to override.
 */
public class CreateTableSpark {

  public static void main(String[] args) throws Exception {
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

    spark.sparkContext().setLogLevel("WARN");

    String tableName = "local.default.sample_table_spark";

    spark.sql("USE local");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS default");
    spark.sql("DROP TABLE IF EXISTS " + tableName);

    spark.sql(
        "CREATE TABLE "
            + tableName
            + " (id BIGINT, data STRING, created_at TIMESTAMP) "
            + "USING iceberg "
            + "TBLPROPERTIES ("
            + "'write.parquet.table-level-bloom-filter-enabled.column.id'='true',"
            + "'write.parquet.table-level-bloom-filter-enabled.column.data'='true'"
            + ")");

    System.out.println("Created table: " + tableName);
    System.out.println("Table-level bloom filters: enabled for id, data (row-group bloom filters also enabled implicitly)");

    int numDataFiles = 10;
    int recordsPerFile = 10;
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numDataFiles * recordsPerFile; i++) {
      rows.add(
          org.apache.spark.sql.RowFactory.create(
              (long) (i + 1),
              "item_" + (i + 1),
              java.sql.Timestamp.from(
                  java.time.OffsetDateTime.parse("2024-01-15T10:00:00Z").plusDays(i).toInstant())));
    }

    StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
              DataTypes.createStructField("data", DataTypes.StringType, false),
              DataTypes.createStructField("created_at", DataTypes.TimestampType, true)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    df.repartition(numDataFiles).writeTo(tableName).append();

    System.out.println("Wrote " + (numDataFiles * recordsPerFile) + " records in " + numDataFiles + " data files");

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    table.refresh();

    ComputeTableStats.Result result =
        SparkActions.get().computeTableStats(table).columns("id", "data").execute();

    System.out.println(
        "Created Puffin file with NDV for data field: "
            + result.statisticsFile().blobMetadata().stream()
                .filter(m -> m.properties().containsKey("ndv"))
                .map(m -> m.properties().get("ndv"))
                .findFirst()
                .orElse("N/A")
            + " distinct values");

    long bloomFilterCount =
        result.statisticsFile().blobMetadata().stream()
            .filter(m -> m.type().equals("apache-datasketches-bloom-filter-v1"))
            .count();
    System.out.println("Created " + bloomFilterCount + " table-level bloom filter blob(s) in Puffin file");
    result.statisticsFile().blobMetadata().stream()
        .filter(m -> m.type().equals("apache-datasketches-bloom-filter-v1"))
        .forEach(
            m ->
                System.out.println(
                    "  Bloom filter blob: fields="
                        + m.fields()
                        + " fpp="
                        + m.properties().get("fpp")));

    spark.stop();
  }
}
