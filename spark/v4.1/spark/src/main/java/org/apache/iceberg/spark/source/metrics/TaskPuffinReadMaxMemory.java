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
package org.apache.iceberg.spark.source.metrics;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

public class TaskPuffinReadMaxMemory implements CustomTaskMetric {

  private final long value;

  /** @param valueMBx1024 peak memory in MB * 1024 (for 0.001 MB precision) */
  public TaskPuffinReadMaxMemory(long valueMBx1024) {
    this.value = valueMBx1024;
  }

  @Override
  public String name() {
    return PuffinReadMaxMemory.NAME;
  }

  @Override
  public long value() {
    return value;
  }
}
