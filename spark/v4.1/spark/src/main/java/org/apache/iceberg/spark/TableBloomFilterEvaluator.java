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
package org.apache.iceberg.spark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.datasketches.filters.bloomfilter.BloomFilter;
import org.apache.datasketches.memory.Memory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates a scan filter expression against table-level bloom filter statistics stored in a
 * Puffin statistics file. Returns {@code false} from {@link #mightContain()} only when the bloom
 * filter can definitively prove that no row in the snapshot matches the predicate, allowing the
 * entire scan to be skipped.
 *
 * <p>Only {@code =} and {@code IN} predicates on columns with bloom filter blobs can prune. All
 * other predicates are conservatively treated as "might contain".
 */
public class TableBloomFilterEvaluator {

  private static final Logger LOG = LoggerFactory.getLogger(TableBloomFilterEvaluator.class);

  private final Schema schema;
  private final Expression boundExpr;
  private final Map<Integer, BloomFilter> bloomFiltersByFieldId;

  private TableBloomFilterEvaluator(
      Schema schema,
      Expression boundExpr,
      Map<Integer, BloomFilter> bloomFiltersByFieldId) {
    this.schema = schema;
    this.boundExpr = boundExpr;
    this.bloomFiltersByFieldId = bloomFiltersByFieldId;
  }

  /**
   * Creates an evaluator for the given table, snapshot, and filter expression. Returns {@code
   * null} if no applicable bloom filter blobs exist (caller should treat as "might contain").
   */
  public static TableBloomFilterEvaluator create(
      Table table, Snapshot snapshot, Expression filter, boolean caseSensitive) {
    // Find the statistics file for this snapshot
    StatisticsFile statsFile = null;
    for (StatisticsFile sf : table.statisticsFiles()) {
      if (sf.snapshotId() == snapshot.snapshotId()) {
        statsFile = sf;
        break;
      }
    }
    if (statsFile == null) {
      return null;
    }

    Schema schema = table.schemas().get(snapshot.schemaId());

    // Bind the expression and extract referenced field IDs
    Expression boundExpr;
    try {
      boundExpr =
          Binder.bind(schema.asStruct(), Expressions.rewriteNot(filter), caseSensitive);
    } catch (Exception e) {
      LOG.warn("Failed to bind expression for bloom filter evaluation: {}", e.getMessage());
      return null;
    }

    Set<Integer> referencedFieldIds =
        Binder.boundReferences(schema.asStruct(), ImmutableList.of(filter), caseSensitive);
    if (referencedFieldIds.isEmpty()) {
      return null;
    }

    // Open the Puffin file and load bloom filter blobs for referenced fields
    Map<Integer, BloomFilter> bloomFilters = Maps.newHashMap();
    try (PuffinReader reader =
        Puffin.read(table.io().newInputFile(statsFile.path()))
            .withFileSize(statsFile.fileSizeInBytes())
            .withFooterSize(statsFile.fileFooterSizeInBytes())
            .build()) {

      List<BlobMetadata> bloomBlobs =
          reader.fileMetadata().blobs().stream()
              .filter(
                  b ->
                      StandardBlobTypes.APACHE_DATASKETCHES_BLOOM_FILTER_V1.equals(b.type())
                          && !b.inputFields().isEmpty()
                          && referencedFieldIds.contains(b.inputFields().get(0)))
              .collect(Collectors.toList());

      if (bloomBlobs.isEmpty()) {
        return null;
      }

      for (Pair<BlobMetadata, ByteBuffer> pair : reader.readAll(bloomBlobs)) {
        int fieldId = pair.first().inputFields().get(0);
        byte[] bytes = toByteArray(pair.second());
        bloomFilters.put(fieldId, BloomFilter.heapify(Memory.wrap(bytes)));
      }

    } catch (IOException e) {
      LOG.warn("Failed to read bloom filter statistics from {}: {}", statsFile.path(), e.getMessage());
      return null;
    }

    if (bloomFilters.isEmpty()) {
      return null;
    }

    return new TableBloomFilterEvaluator(schema, boundExpr, bloomFilters);
  }

  /**
   * Returns {@code true} if the predicate might match some row (scan must proceed), or {@code
   * false} if the bloom filter proves no row can match (scan can be skipped entirely).
   */
  public boolean mightContain() {
    return ExpressionVisitors.visitEvaluator(boundExpr, new BloomFilterExprVisitor());
  }

  private class BloomFilterExprVisitor extends BoundExpressionVisitor<Boolean> {

    private static final boolean MIGHT_CONTAIN = true;
    private static final boolean CANNOT_CONTAIN = false;

    @Override
    public Boolean alwaysTrue() {
      return MIGHT_CONTAIN;
    }

    @Override
    public Boolean alwaysFalse() {
      return CANNOT_CONTAIN;
    }

    @Override
    public Boolean not(Boolean result) {
      // rewriteNot() eliminates NOT before binding — conservatively pass through
      return MIGHT_CONTAIN;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      return leftResult && rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public <T> Boolean isNull(BoundReference<T> ref) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean notNull(BoundReference<T> ref) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean isNaN(BoundReference<T> ref) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean notNaN(BoundReference<T> ref) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
      return MIGHT_CONTAIN;
    }

    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      BloomFilter bloom = bloomFiltersByFieldId.get(ref.fieldId());
      if (bloom == null) {
        return MIGHT_CONTAIN;
      }
      return queryBloom(bloom, ref.field().type(), lit.value());
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      BloomFilter bloom = bloomFiltersByFieldId.get(ref.fieldId());
      if (bloom == null) {
        return MIGHT_CONTAIN;
      }
      Type fieldType = ref.field().type();
      for (T value : literalSet) {
        if (queryBloom(bloom, fieldType, value)) {
          return MIGHT_CONTAIN;
        }
      }
      return CANNOT_CONTAIN;
    }

    private <T> boolean queryBloom(BloomFilter bloom, Type fieldType, T value) {
      try {
        ByteBuffer buf = Conversions.toByteBuffer(fieldType, value);
        return bloom.query(toByteArray(buf));
      } catch (Exception e) {
        // Conservatively allow the scan if encoding fails
        return MIGHT_CONTAIN;
      }
    }
  }

  private static byte[] toByteArray(ByteBuffer buf) {
    if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0) {
      return buf.array();
    } else {
      byte[] bytes = new byte[buf.remaining()];
      buf.duplicate().get(bytes);
      return bytes;
    }
  }
}
