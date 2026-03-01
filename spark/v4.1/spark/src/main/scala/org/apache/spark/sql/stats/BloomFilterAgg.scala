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
package org.apache.spark.sql.stats

import java.nio.ByteBuffer
import org.apache.datasketches.filters.bloomfilter.BloomFilter
import org.apache.datasketches.filters.bloomfilter.BloomFilterBuilder
import org.apache.datasketches.memory.Memory
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Conversions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

/**
 * BloomFilterAgg computes a DataSketches BloomFilter over the values of a single column.
 * Values are serialized using Iceberg's canonical single-value byte encoding so that the
 * resulting filter can be queried on the read side with the same encoding.
 *
 * The result returned by eval() is the raw byte array of the serialized BloomFilter.
 */
case class BloomFilterAgg(
    child: Expression,
    expectedDistinct: Long,
    targetFpp: Double,
    seed: Long,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[BloomFilter]
    with UnaryLike[Expression] {

  private lazy val icebergType = SparkSchemaUtil.convert(child.dataType)

  def this(colName: String, expectedDistinct: Long, targetFpp: Double) = {
    this(
      analysis.UnresolvedAttribute.quotedString(colName),
      expectedDistinct,
      targetFpp,
      BloomFilterAgg.DEFAULT_SEED)
  }

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = false

  override def createAggregationBuffer(): BloomFilter = {
    BloomFilterBuilder.createByAccuracy(expectedDistinct, targetFpp, seed)
  }

  override def update(buffer: BloomFilter, input: InternalRow): BloomFilter = {
    val value = child.eval(input)
    if (value != null) {
      val icebergValue = toIcebergValue(value)
      val byteBuffer = Conversions.toByteBuffer(icebergType, icebergValue)
      val arr = toByteArray(byteBuffer)
      buffer.update(arr)
    }
    buffer
  }

  private def toIcebergValue(value: Any): Any = {
    value match {
      case s: UTF8String => s.toString
      case d: Decimal => d.toJavaBigDecimal
      case b: Array[Byte] => ByteBuffer.wrap(b)
      case _ => value
    }
  }

  private def toByteArray(buf: ByteBuffer): Array[Byte] = {
    if (buf.hasArray && buf.arrayOffset() == 0 && buf.position() == 0) {
      buf.array()
    } else {
      val arr = new Array[Byte](buf.remaining())
      buf.duplicate().get(arr)
      arr
    }
  }

  override def merge(buffer: BloomFilter, input: BloomFilter): BloomFilter = {
    buffer.union(input)
    buffer
  }

  override def eval(buffer: BloomFilter): Any = {
    buffer.toByteArray
  }

  override def serialize(buffer: BloomFilter): Array[Byte] = {
    buffer.toByteArray
  }

  override def deserialize(storageFormat: Array[Byte]): BloomFilter = {
    BloomFilter.heapify(Memory.wrap(storageFormat))
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}

object BloomFilterAgg {
  val DEFAULT_SEED: Long = 0L
}
