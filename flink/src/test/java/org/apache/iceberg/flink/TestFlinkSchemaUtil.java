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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestFlinkSchemaUtil {

  @Test
  public void testConvertFlinkSchemaToIcebergSchema() {
    TableSchema flinkSchema = TableSchema.builder()
        .field("id", DataTypes.INT().notNull())
        .field("name", DataTypes.STRING()) /* optional by default */
        .field("salary", DataTypes.DOUBLE().notNull())
        .field("locations", DataTypes.MAP(DataTypes.STRING(),
            DataTypes.ROW(DataTypes.FIELD("posX", DataTypes.DOUBLE().notNull(), "X field"),
                DataTypes.FIELD("posY", DataTypes.DOUBLE().notNull(), "Y field"))))
        .field("strArray", DataTypes.ARRAY(DataTypes.STRING()).nullable())
        .field("intArray", DataTypes.ARRAY(DataTypes.INT()).nullable())
        .build();

    Schema actualSchema = FlinkSchemaUtil.convert(flinkSchema);
    Schema expectedSchema = new Schema(
        Types.NestedField.required(0, "id", Types.IntegerType.get(), null),
        Types.NestedField.optional(1, "name", Types.StringType.get(), null),
        Types.NestedField.required(2, "salary", Types.DoubleType.get(), null),
        Types.NestedField.optional(3, "locations", Types.MapType.ofOptional(8, 9,
            Types.StringType.get(),
            Types.StructType.of(
                Types.NestedField.required(6, "posX", Types.DoubleType.get(), "X field"),
                Types.NestedField.required(7, "posY", Types.DoubleType.get(), "Y field")
            ))),
        Types.NestedField.optional(4, "strArray", Types.ListType.ofOptional(10, Types.StringType.get())),
        Types.NestedField.optional(5, "intArray", Types.ListType.ofOptional(11, Types.IntegerType.get()))
    );

    Assert.assertEquals(expectedSchema.toString(), actualSchema.toString());
    FlinkSchemaUtil.validate(expectedSchema, actualSchema, false, false);
  }
}
