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

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestFlinkSchemaUtil {

  @Test
  public void testConvertFlinkSchemaToIcebergSchema() {
    List<TypeInformation<?>> types = new ArrayList<>();
    types.add(org.apache.flink.api.common.typeinfo.Types.INT);
    types.add(org.apache.flink.api.common.typeinfo.Types.STRING);
    types.add(org.apache.flink.api.common.typeinfo.Types.DOUBLE);
    types.add(org.apache.flink.api.common.typeinfo.Types.MAP(
        org.apache.flink.api.common.typeinfo.Types.STRING,
        org.apache.flink.api.common.typeinfo.Types.ROW(
            org.apache.flink.api.common.typeinfo.Types.DOUBLE,
            org.apache.flink.api.common.typeinfo.Types.DOUBLE
        )
    ));
    types.add(org.apache.flink.api.common.typeinfo.Types.OBJECT_ARRAY(
        org.apache.flink.api.common.typeinfo.Types.STRING
    ));
    types.add(org.apache.flink.api.common.typeinfo.Types.PRIMITIVE_ARRAY(
        org.apache.flink.api.common.typeinfo.Types.INT
    ));

    String[] fieldNames = new String[]{
        "id", "name", "salary", "locations", "groupIds", "intArray"
    };
    RowTypeInfo flinkSchema = new RowTypeInfo(types.toArray(new TypeInformation[0]), fieldNames);
    Schema actualSchema = FlinkSchemaUtil.convert(flinkSchema);
    Schema expectedSchema = new Schema(
        Types.NestedField.optional(0, "id", Types.IntegerType.get(), null),
        Types.NestedField.optional(1, "name", Types.StringType.get(), null),
        Types.NestedField.optional(2, "salary", Types.DoubleType.get(), null),
        Types.NestedField.optional(3, "locations", Types.MapType.ofOptional(8, 9,
            Types.StringType.get(),
            Types.StructType.of(
                Types.NestedField.optional(6, "f0", Types.DoubleType.get(), null),
                Types.NestedField.optional(7, "f1", Types.DoubleType.get(), null)
            ))),
        Types.NestedField.optional(4, "groupIds", Types.ListType.ofOptional(10, Types.StringType.get())),
        Types.NestedField.optional(5, "intArray", Types.ListType.ofOptional(11, Types.IntegerType.get()))
    );
    Assert.assertEquals(expectedSchema.toString(), actualSchema.toString());
  }
}
