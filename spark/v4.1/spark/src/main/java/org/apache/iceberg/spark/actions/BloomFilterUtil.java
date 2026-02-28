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
import java.util.List;
import java.util.Map;
import org.apache.datasketches.filters.bloomfilter.BloomFilter;
import org.apache.datasketches.memory.Memory;
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
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.classic.ExpressionColumnNode;
import org.apache.spark.sql.stats.BloomFilterAgg;

public class BloomFilterUtil {

  private BloomFilterUtil() {}

  static final long DEFAULT_NDV = 1_000_000L;
  static final double DEFAULT_FPP = TableProperties.PARQUET_BLOOM_FILTER_COLUMN_FPP_DEFAULT;

  public static final String BLOOM_FILTER_FPP_PROPERTY = "fpp";

  static List<Blob> generateBlobs(
      SparkSession spark, Table table, Snapshot snapshot, List<String> columns) {
    Map<String, String> tableProperties = table.properties();
    Map<String, String> columnNdvStrings =
        PropertyUtil.propertiesWithPrefix(
            tableProperties, TableProperties.PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX);
    Map<String, String> columnFppStrings =
        PropertyUtil.propertiesWithPrefix(
            tableProperties, TableProperties.PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX);

    Row filters = computeBloomFilters(spark, table, snapshot, columns, columnNdvStrings, columnFppStrings);
    Schema schema = table.schemas().get(snapshot.schemaId());
    List<Blob> blobs = Lists.newArrayListWithExpectedSize(columns.size());

    for (int i = 0; i < columns.size(); i++) {
      String colName = columns.get(i);
      Types.NestedField field = schema.findField(colName);
      BloomFilter bloomFilter = BloomFilter.heapify(Memory.wrap((byte[]) filters.get(i)));
      double fpp = parseFpp(columnFppStrings.get(colName));
      blobs.add(toBlob(field, bloomFilter, snapshot, fpp));
    }
    return blobs;
  }

  private static Blob toBlob(
      Types.NestedField field, BloomFilter bloomFilter, Snapshot snapshot, double fpp) {
    return new Blob(
        StandardBlobTypes.APACHE_DATASKETCHES_BLOOM_FILTER_V1,
        ImmutableList.of(field.fieldId()),
        snapshot.snapshotId(),
        snapshot.sequenceNumber(),
        ByteBuffer.wrap(bloomFilter.toByteArray()),
        PuffinCompressionCodec.ZSTD,
        ImmutableMap.of(BLOOM_FILTER_FPP_PROPERTY, String.valueOf(fpp)));
  }

  private static Row computeBloomFilters(
      SparkSession spark,
      Table table,
      Snapshot snapshot,
      List<String> colNames,
      Map<String, String> columnNdvStrings,
      Map<String, String> columnFppStrings) {
    Dataset<Row> inputDF = SparkTableUtil.loadTable(spark, table, snapshot.snapshotId());
    return inputDF.select(toAggColumns(colNames, columnNdvStrings, columnFppStrings)).first();
  }

  private static Column[] toAggColumns(
      List<String> colNames,
      Map<String, String> columnNdvStrings,
      Map<String, String> columnFppStrings) {
    return colNames.stream()
        .map(col -> toAggColumn(col, columnNdvStrings, columnFppStrings))
        .toArray(Column[]::new);
  }

  private static Column toAggColumn(
      String colName,
      Map<String, String> columnNdvStrings,
      Map<String, String> columnFppStrings) {
    long ndv = parseNdv(columnNdvStrings.get(colName));
    double fpp = parseFpp(columnFppStrings.get(colName));
    BloomFilterAgg agg = new BloomFilterAgg(colName, ndv, fpp);
    return new Column(ExpressionColumnNode.apply(agg.toAggregateExpression()));
  }

  private static long parseNdv(String ndvString) {
    return ndvString != null ? Long.parseLong(ndvString) : DEFAULT_NDV;
  }

  private static double parseFpp(String fppString) {
    return fppString != null ? Double.parseDouble(fppString) : DEFAULT_FPP;
  }
}
