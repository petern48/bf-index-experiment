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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleConsumer;
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
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.Timer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates a scan filter expression against per-file bloom filter statistics stored in a Puffin
 * statistics file. Returns {@code false} from {@link #fileMightContain(String)} only when the
 * bloom filter for a specific data file can definitively prove that no row in that file matches
 * the predicate, allowing that file to be skipped during scan planning without opening it.
 *
 * <p>Only {@code =} and {@code IN} predicates on columns with bloom filter blobs can prune. All
 * other predicates are conservatively treated as "might contain".
 */
public class FileBloomFilterEvaluator {

  public static final String DATA_FILE_PATH_PROPERTY = "data-file-path";

  private static final Logger LOG = LoggerFactory.getLogger(FileBloomFilterEvaluator.class);

  private final Expression boundExpr;
  private final Map<String, Map<Integer, BloomFilter>> bloomFiltersByFilePath;

  private FileBloomFilterEvaluator(
      Expression boundExpr, Map<String, Map<Integer, BloomFilter>> bloomFiltersByFilePath) {
    this.boundExpr = boundExpr;
    this.bloomFiltersByFilePath = bloomFiltersByFilePath;
  }

  private static final int MEMORY_POLL_INTERVAL_MS = 5;

  /**
   * Creates an evaluator for the given table, snapshot, and filter expression. Returns {@code
   * null} if no applicable file-level bloom filter blobs exist in the statistics file (caller
   * should treat as "might contain" for all files).
   */
  public static FileBloomFilterEvaluator create(
      Table table, Snapshot snapshot, Expression filter, boolean caseSensitive,
      ScanMetrics scanMetrics) {
    return create(table, snapshot, filter, caseSensitive, scanMetrics, null);
  }

  /**
   * Creates an evaluator for the given table, snapshot, and filter expression. Returns {@code
   * null} if no applicable file-level bloom filter blobs exist in the statistics file (caller
   * should treat as "might contain" for all files).
   *
   * @param memoryRecorder if non-null, records peak heap memory (MB) during puffin file read
   */
  public static FileBloomFilterEvaluator create(
      Table table,
      Snapshot snapshot,
      Expression filter,
      boolean caseSensitive,
      ScanMetrics scanMetrics,
      DoubleConsumer memoryRecorder) {
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
      boundExpr = Binder.bind(schema.asStruct(), Expressions.rewriteNot(filter), caseSensitive);
    } catch (Exception e) {
      LOG.warn("Failed to bind expression for bloom filter evaluation: {}", e.getMessage());
      return null;
    }

    Set<Integer> referencedFieldIds =
        Binder.boundReferences(schema.asStruct(), ImmutableList.of(filter), caseSensitive);
    if (referencedFieldIds.isEmpty()) {
      return null;
    }

    // Open the Puffin file and load file-level bloom filter blobs for referenced fields.
    // File-level blobs are distinguished by the presence of the "data-file-path" property.
    Map<String, Map<Integer, BloomFilter>> bloomFiltersByFilePath = Maps.newHashMap();
    scanMetrics.puffinFilesRead().increment();
    Timer.Timed puffinTimer = scanMetrics.puffinReadDuration().start();

    final StatisticsFile statsFileForRead = statsFile;
    Runnable puffinRead =
        () -> {
          try (PuffinReader reader =
              Puffin.read(table.io().newInputFile(statsFileForRead.path()))
                  .withFileSize(statsFileForRead.fileSizeInBytes())
                  .withFooterSize(statsFileForRead.fileFooterSizeInBytes())
                  .build()) {

            List<BlobMetadata> bloomBlobs =
                reader.fileMetadata().blobs().stream()
                    .filter(
                        b ->
                            StandardBlobTypes.APACHE_DATASKETCHES_BLOOM_FILTER_V1.equals(b.type())
                                && b.properties().containsKey(DATA_FILE_PATH_PROPERTY)
                                && !b.inputFields().isEmpty()
                                && referencedFieldIds.contains(b.inputFields().get(0)))
                    .collect(Collectors.toList());

            if (bloomBlobs.isEmpty()) {
              return;
            }

            for (Pair<BlobMetadata, ByteBuffer> pair : reader.readAll(bloomBlobs)) {
              BlobMetadata blobMeta = pair.first();
              String filePath = blobMeta.properties().get(DATA_FILE_PATH_PROPERTY);
              int fieldId = blobMeta.inputFields().get(0);
              byte[] bytes = toByteArray(pair.second());
              bloomFiltersByFilePath
                  .computeIfAbsent(filePath, k -> Maps.newHashMap())
                  .put(fieldId, BloomFilter.heapify(Memory.wrap(bytes)));
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };

    try {
      if (memoryRecorder != null) {
        double peakMB = trackPeakMemoryMB(puffinRead);
        memoryRecorder.accept(peakMB);
      } else {
        puffinRead.run();
      }
    } catch (UncheckedIOException e) {
      LOG.warn(
          "Failed to read bloom filter statistics from {}: {}",
          statsFileForRead.path(),
          e.getCause().getMessage());
      return null;
    } finally {
      puffinTimer.stop();
    }

    if (bloomFiltersByFilePath.isEmpty()) {
      return null;
    }

    return new FileBloomFilterEvaluator(boundExpr, bloomFiltersByFilePath);
  }

  /**
   * Returns {@code true} if the predicate might match some row in the given file (file must be
   * scanned), or {@code false} if the bloom filter proves no row in that file can match the
   * predicate (file can be skipped).
   *
   * <p>If no bloom filter exists for {@code filePath} (e.g., the file was added after statistics
   * were last computed), conservatively returns {@code true}.
   */
  public boolean fileMightContain(String filePath) {
    Map<Integer, BloomFilter> fileBloomFilters = bloomFiltersByFilePath.get(filePath);
    if (fileBloomFilters == null) {
      return true; // no bloom filter for this file — conservative
    }
    return ExpressionVisitors.visitEvaluator(boundExpr, new BloomFilterExprVisitor(fileBloomFilters));
  }

  private static class BloomFilterExprVisitor extends BoundExpressionVisitor<Boolean> {

    private static final boolean MIGHT_CONTAIN = true;
    private static final boolean CANNOT_CONTAIN = false;

    private final Map<Integer, BloomFilter> fileBloomFilters;

    BloomFilterExprVisitor(Map<Integer, BloomFilter> fileBloomFilters) {
      this.fileBloomFilters = fileBloomFilters;
    }

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
      BloomFilter bloom = fileBloomFilters.get(ref.fieldId());
      if (bloom == null) {
        return MIGHT_CONTAIN;
      }
      return queryBloom(bloom, ref.field().type(), lit.value());
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      BloomFilter bloom = fileBloomFilters.get(ref.fieldId());
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
        return MIGHT_CONTAIN; // conservatively allow the scan if encoding fails
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

  /** Tracks peak heap memory (MB) while running the operation. */
  private static double trackPeakMemoryMB(Runnable operation) {
    Runtime runtime = Runtime.getRuntime();
    AtomicLong peakBytes = new AtomicLong(0);
    AtomicBoolean running = new AtomicBoolean(true);

    Thread monitor =
        new Thread(
            () -> {
              while (running.get()) {
                long used = runtime.totalMemory() - runtime.freeMemory();
                peakBytes.updateAndGet(current -> Math.max(current, used));
                try {
                  Thread.sleep(MEMORY_POLL_INTERVAL_MS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                }
              }
            });
    monitor.setDaemon(true);
    monitor.start();

    try {
      operation.run();
    } finally {
      running.set(false);
      try {
        monitor.join(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    return peakBytes.get() / (1024.0 * 1024.0);
  }
}
