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

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

class IcebergValidator extends ConnectorDescriptorValidator {
  public static final String CONNECTOR_TYPE = "connector.type";
  public static final String CONNECTOR_TYPE_VALUE = "iceberg";

  public static final String CONNECTOR_VERSION = "connector.version";
  public static final String CONNECTOR_VERSION_VALUE = "0.8.0";

  public static final String CONNECTOR_PROPERTY_VERSION = "connector.property-version";
  public static final int CONNECTOR_PROPERTY_VERSION_VALUE = 1;

  public static final String CONNECTOR_ICEBERG_TABLE_IDENTIFIER = "connector.iceberg-table.identifier";

  public static final String CONNECTOR_ICEBERG_TABLE_FROM_SNAPSHOT_ID = "connector.iceberg-table.from-snapshot-id";

  public static final String CONNECTOR_ICEBERG_TABLE_SNAP_POLLING_INTERVAL_MILLIS =
      "connector.iceberg-table.snapshots-polling-interval-millis";

  private static final IcebergValidator INSTANCE = new IcebergValidator();

  @Override
  public void validate(DescriptorProperties properties) {
    super.validate(properties);
    properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE, false);
    properties.validateString(CONNECTOR_ICEBERG_TABLE_IDENTIFIER, false, 1);
    properties.validateLong(CONNECTOR_ICEBERG_TABLE_FROM_SNAPSHOT_ID, true, 1);
    properties.validateLong(CONNECTOR_ICEBERG_TABLE_SNAP_POLLING_INTERVAL_MILLIS, true, 1);
  }

  public static IcebergValidator getInstance() {
    return INSTANCE;
  }
}
