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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.IcebergSource;

/**
 * Factory for creating configured instances of {@link IcebergTableSink} or source.
 */
public class IcebergTableFactory implements
    StreamTableSourceFactory<Row>,
    StreamTableSinkFactory<Tuple2<Boolean, Row>> {

  @Override
  public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
    DescriptorProperties descProperties = getValidatedProperties(properties);

    // Create the IcebergTableSink instance.
    boolean isAppendOnly = descProperties
        .isValue(StreamTableDescriptorValidator.UPDATE_MODE, StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND);
    String tableIdentifier = descProperties.getString(IcebergValidator.CONNECTOR_ICEBERG_TABLE_IDENTIFIER);
    TableSchema schema = descProperties.getTableSchema(Schema.SCHEMA);
    return new IcebergTableSink(isAppendOnly, tableIdentifier, schema);
  }

  @Override
  public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
    DescriptorProperties descProperties = getValidatedProperties(properties);

    // Initialize the iceberg table source instance.
    String tableIdentifier = descProperties.getString(IcebergValidator.CONNECTOR_ICEBERG_TABLE_IDENTIFIER);
    TableSchema schema = descProperties.getTableSchema(Schema.SCHEMA);
    // TODO consider to pass a configuration from user side.
    Configuration conf = new Configuration();
    // Get the optional config keys.
    long fromSnapshotId = descProperties.getOptionalLong(IcebergValidator.CONNECTOR_ICEBERG_TABLE_FROM_SNAPSHOT_ID)
        .orElse(IcebergSource.NON_CONSUMED_SNAPSHOT_ID);
    long snapshotPollingIntervalMillis = descProperties
        .getOptionalLong(IcebergValidator.CONNECTOR_ICEBERG_TABLE_SNAP_POLLING_INTERVAL_MILLIS)
        .orElse(1000L);

    return new IcebergTableSource(tableIdentifier, conf, fromSnapshotId, snapshotPollingIntervalMillis, schema);
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

    // Iceberg properties
    properties.add(IcebergValidator.CONNECTOR_ICEBERG_TABLE_IDENTIFIER);
    properties.add(IcebergValidator.CONNECTOR_ICEBERG_TABLE_FROM_SNAPSHOT_ID);
    properties.add(IcebergValidator.CONNECTOR_ICEBERG_TABLE_SNAP_POLLING_INTERVAL_MILLIS);

    // Flink schema properties
    properties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_DATA_TYPE);
    properties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_TYPE);
    properties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_NAME);
    properties.add(Schema.SCHEMA + ".#." + Schema.SCHEMA_FROM);
    // computed column
    properties.add(Schema.SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_EXPR);

    // watermark
    properties.add(Schema.SCHEMA + "." + DescriptorProperties.WATERMARK +
        ".#." + DescriptorProperties.WATERMARK_ROWTIME);
    properties.add(Schema.SCHEMA + "." + DescriptorProperties.WATERMARK +
        ".#." + DescriptorProperties.WATERMARK_STRATEGY_EXPR);
    properties.add(Schema.SCHEMA + "." + DescriptorProperties.WATERMARK +
        ".#." + DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE);
    return properties;
  }

  private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
    DescriptorProperties descProperties = new DescriptorProperties(true);
    descProperties.putProperties(properties);
    // Validate the properties values.
    IcebergValidator.getInstance().validate(descProperties);
    return descProperties;
  }
}
