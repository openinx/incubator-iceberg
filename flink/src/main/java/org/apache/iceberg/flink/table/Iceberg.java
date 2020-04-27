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

import java.util.Map;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class Iceberg extends ConnectorDescriptor {

  private DescriptorProperties properties = new DescriptorProperties();

  private Iceberg() {
    super(IcebergValidator.CONNECTOR_TYPE_VALUE, IcebergValidator.CONNECTOR_PROPERTY_VERSION_VALUE, false);
  }

  public static Iceberg newInstance() {
    return new Iceberg();
  }

  public Iceberg withVersion(String version) {
    this.properties.putString(IcebergValidator.CONNECTOR_VERSION, version);
    return this;
  }

  public Iceberg withTableIdentifier(String tableIdentifier) {
    this.properties.putString(IcebergValidator.CONNECTOR_ICEBERG_TABLE_IDENTIFIER, tableIdentifier);
    return this;
  }

  @Override
  protected Map<String, String> toConnectorProperties() {
    return properties.asMap();
  }
}
