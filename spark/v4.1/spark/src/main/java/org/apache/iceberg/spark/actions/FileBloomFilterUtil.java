/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.apache.iceberg.spark.actions;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.classic.ExpressionColumnNode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.stats.BloomFilterAgg;

/**
 * Utilities for generating external file-level bloom filter blobs stored in Puffin statistics
 * files. One bloom filter is produced per (data file, column) pair, covering all values of that
 * column within the file.
 *
 * <p>The file path for each blob is stored in the {@value DATA_FILE_PATH_PROPERTY} blob property
 * so it can be correlated with a specific data file during scan planning.
 */
class FileBloomFilterUtil {

  static final String DATA_FILE_PATH_PROPERTY = "data-file-path";
  static final String BLOOM_FILTER_FPP_PROPERTY = "fpp";

  static final long DEFAULT_NDV = 1_000_000L;
  static final double DEFAULT_FPP = TableProperties.PARQUET_BLOOM_FILTER_COLUMN_FPP_DEFAULT;

  private FileBloomFilterUtil() {}

  /**
   * Computes per-file bloom filter blobs for the given columns in a single Spark job.
   *
   * <p>Uses {@code input_file_name()} to group rows by their originating data file, then
   * aggregates a DataSketches BloomFilter per column per file.
   */
  static List<Blob> generateBlobs(
      SparkSession spark, Table table, Snapshot snapshot, List<String> columns) {
    Schema schema = table.schemas().get(snapshot.schemaId());
    Map<String, String> props = table.properties();

    Map<String, Long> ndvMap = readNdvPerColumn(columns, props);
    Map<String, Double> fppMap = readFppPerColumn(columns, props);

    Dataset<Row> inputDF = SparkTableUtil.loadTable(spark, table, snapshot.snapshotId());

    Column[] aggCols = toAggColumns(columns, ndvMap, fppMap);
    List<Row> fileRows =
        inputDF
            .withColumn("_file_path", functions.input_file_name())
            .groupBy("_file_path")
            .agg(aggCols[0], Arrays.copyOfRange(aggCols, 1, aggCols.length))
            .collectAsList();

    List<Blob> blobs = Lists.newArrayList();
    for (Row row : fileRows) {
      String filePath = row.getString(0);
      for (int i = 0; i < columns.size(); i++) {
        String colName = columns.get(i);
        Types.NestedField field = schema.findField(colName);
        byte[] bfBytes = (byte[]) row.get(i + 1);
        double fpp = fppMap.get(colName);
        blobs.add(toBlob(field, bfBytes, filePath, snapshot, fpp));
      }
    }
    return blobs;
  }

  private static Blob toBlob(
      Types.NestedField field,
      byte[] bfBytes,
      String filePath,
      Snapshot snapshot,
      double fpp) {
    return new Blob(
        StandardBlobTypes.APACHE_DATASKETCHES_BLOOM_FILTER_V1,
        ImmutableList.of(field.fieldId()),
        snapshot.snapshotId(),
        snapshot.sequenceNumber(),
        ByteBuffer.wrap(bfBytes),
        PuffinCompressionCodec.ZSTD,
        ImmutableMap.of(DATA_FILE_PATH_PROPERTY, filePath, BLOOM_FILTER_FPP_PROPERTY,
            String.valueOf(fpp)));
  }

  private static Column[] toAggColumns(
      List<String> colNames, Map<String, Long> ndvMap, Map<String, Double> fppMap) {
    return colNames.stream().map(col -> toAggColumn(col, ndvMap.get(col), fppMap.get(col)))
        .toArray(Column[]::new);
  }

  private static Column toAggColumn(String colName, long ndv, double fpp) {
    BloomFilterAgg agg = new BloomFilterAgg(colName, ndv, fpp);
    return new Column(ExpressionColumnNode.apply(agg.toAggregateExpression()));
  }

  private static Map<String, Long> readNdvPerColumn(List<String> columns, Map<String, String> props) {
    Map<String, Long> ndvMap = Maps.newHashMap();
    Map<String, String> configured =
        PropertyUtil.propertiesWithPrefix(props, TableProperties.PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX);
    for (String col : columns) {
      String raw = configured.get(col);
      ndvMap.put(col, raw != null ? Long.parseLong(raw) : DEFAULT_NDV);
    }
    return ndvMap;
  }

  private static Map<String, Double> readFppPerColumn(
      List<String> columns, Map<String, String> props) {
    Map<String, Double> fppMap = Maps.newHashMap();
    Map<String, String> configured =
        PropertyUtil.propertiesWithPrefix(props, TableProperties.PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX);
    for (String col : columns) {
      String raw = configured.get(col);
      fppMap.put(col, raw != null ? Double.parseDouble(raw) : DEFAULT_FPP);
    }
    return fppMap;
  }
}
