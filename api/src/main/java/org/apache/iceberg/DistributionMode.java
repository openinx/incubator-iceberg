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

package org.apache.iceberg;

/**
 * Enum of supported write distribution mode, it defines the write behavior of batch or streaming job:
 * <p>
 * 1. none: don't shuffle rows. It is suitable for scenarios where the rows are located in only few
 * partitions, otherwise that may produce too many small files because each task is writing rows into different
 * partitions randomly.
 * <p>
 * 2. hash-partition: hash distribute by partition keys, which is suitable for the scenarios where the rows are located
 * into different partitions evenly.
 * <p>
 * 3. range-partition: range distribute by partition key (or sort key if table has an {@link SortOrder}), which is
 * suitable for the scenarios where rows are located into different partitions with skew distribution.
 */
public enum DistributionMode {
  NONE("none"), HASH("hash-partition"), RANGE("range-partition");

  private final String name;

  DistributionMode(String mode) {
    this.name = mode;
  }

  public String modeName() {
    return name;
  }

  public static DistributionMode fromName(String name) {
    for (DistributionMode mode : DistributionMode.values()) {
      if (mode.name.equals(name)) {
        return mode;
      }
    }

    throw new IllegalArgumentException("Invalid write.distribution-mode name: " + name);
  }
}
