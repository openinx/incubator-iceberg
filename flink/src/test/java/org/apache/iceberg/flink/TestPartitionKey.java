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

package org.apache.iceberg.flink;

import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestPartitionKey {

  @Test
  public void testPartitionKey() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "level", Types.StringType.get()),
        Types.NestedField.optional(2, "sequence_number", Types.LongType.get()),
        Types.NestedField.optional(3, "message", Types.StringType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("level")
        .identity("sequence_number")
        .build();
    PartitionKey.Builder builder = new PartitionKey.Builder(spec);

    Row row = Row.of("info", 100L, "This is an info message");
    PartitionKey pk = builder.build(row);
    Assert.assertArrayEquals(new Object[]{"info", 100L}, pk.getPartitionTuple());
  }
}
