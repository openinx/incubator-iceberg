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

import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class IcebergTableFactory implements StreamTableSinkFactory<RowData> {
  private final FlinkCatalog catalog;

  public IcebergTableFactory(FlinkCatalog catalog) {
    this.catalog = catalog;
  }

  @Override
  public StreamTableSink<RowData> createTableSink(Context context) {
    ObjectIdentifier identifier = context.getObjectIdentifier();
    ObjectPath objectPath = new ObjectPath(identifier.getDatabaseName(), identifier.getObjectName());
    TableIdentifier icebergIdentifier = catalog.toIdentifier(objectPath);
    try {
      Table table = catalog.getIcebergTable(objectPath);
      return new IcebergTableSink(icebergIdentifier, table,
          catalog.getCatalogLoader(), catalog.getHadoopConf(),
          FlinkSchemaUtil.toSchema(table.schema()));
    } catch (TableNotExistException e) {
      throw new ValidationException(String.format("Iceberg table(%s) not exist.", icebergIdentifier), e);
    }
  }

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = Maps.newHashMap();
    context.put(ConnectorDescriptorValidator.CONNECTOR_TYPE, IcebergValidator.CONNECTOR_TYPE_VALUE);
    context.put(ConnectorDescriptorValidator.CONNECTOR_VERSION, IcebergValidator.CONNECTOR_VERSION_VALUE);
    context.put(ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION, "1");
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    List<String> properties = Lists.newArrayList();
    // update mode
    properties.add(StreamTableDescriptorValidator.UPDATE_MODE);

    // Flink schema properties
    properties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_DATA_TYPE);
    properties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_NAME);
    properties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_FROM);

    // watermark
    properties.add(Schema.SCHEMA + "." + DescriptorProperties.WATERMARK +
        ".#." + DescriptorProperties.WATERMARK_ROWTIME);
    properties.add(Schema.SCHEMA + "." + DescriptorProperties.WATERMARK +
        ".#." + DescriptorProperties.WATERMARK_STRATEGY_EXPR);
    properties.add(Schema.SCHEMA + "." + DescriptorProperties.WATERMARK +
        ".#." + DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE);
    return properties;
  }
}
