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

package org.apache.iceberg.flink.table;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.TableSink;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergTableFactory {
  private static final String COL_0 = "C0";
  private static final String COL_1 = "C1";
  private static final String COL_2 = "C2";
  private static final String COL_3 = "C3";

  private static final String FIELD_0 = "f0";
  private static final String FIELD_1 = "f1";
  private static final String FIELD_2 = "f2";
  private static final String FIELD_3 = "f3";

  private DescriptorProperties createDescriptor(TableSchema tableSchema, String tableLocation) {
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put(IcebergValidator.CONNECTOR_TYPE, IcebergValidator.CONNECTOR_TYPE_VALUE);
    tableProperties.put(IcebergValidator.CONNECTOR_VERSION, IcebergValidator.CONNECTOR_VERSION_VALUE);
    tableProperties.put(IcebergValidator.CONNECTOR_PROPERTY_VERSION,
        String.valueOf(IcebergValidator.CONNECTOR_PROPERTY_VERSION_VALUE));
    tableProperties.put(IcebergValidator.CONNECTOR_ICEBERG_TABLE_IDENTIFIER, tableLocation);

    DescriptorProperties descriptorProperties = new DescriptorProperties(true);
    descriptorProperties.putTableSchema(Schema.SCHEMA, tableSchema);
    descriptorProperties.putString(StreamTableDescriptorValidator.UPDATE_MODE,
        StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND);
    descriptorProperties.putProperties(tableProperties);
    return descriptorProperties;
  }

  @Test
  public void testTableSinkFactory() {
    TableSchema schema = TableSchema.builder()
        .field(COL_0, DataTypes.ROW(DataTypes.FIELD(COL_1, DataTypes.INT())))
        .field(COL_1, DataTypes.ROW(
            DataTypes.FIELD(FIELD_0, DataTypes.INT()),
            DataTypes.FIELD(FIELD_1, DataTypes.BIGINT())))
        .field(COL_2, DataTypes.BIGINT())
        .field(COL_3, DataTypes.ROW(
            DataTypes.FIELD(FIELD_2, DataTypes.DOUBLE()),
            DataTypes.FIELD(FIELD_3, DataTypes.BOOLEAN())))
        .build();

    DescriptorProperties descriptorProperties = createDescriptor(schema, "/tmp/test.txt");
    TableSink sink = TableFactoryService
        .find(IcebergTableFactory.class, descriptorProperties.asMap(), this.getClass().getClassLoader())
        .createTableSink(descriptorProperties.asMap());

    Assert.assertTrue(sink instanceof IcebergTableSink);
    IcebergTableSink iSink = (IcebergTableSink) sink;

    Assert.assertEquals(schema, iSink.getTableSchema());
  }
}
