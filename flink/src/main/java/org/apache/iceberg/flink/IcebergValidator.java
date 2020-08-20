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

import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergValidator extends ConnectorDescriptorValidator {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergValidator.class);

  public static final String CONNECTOR_TYPE = "connector.type";
  public static final String CONNECTOR_TYPE_VALUE = "iceberg";

  public static final String CONNECTOR_VERSION = "connector.version";
  public static final String CONNECTOR_VERSION_VALUE = "1.10.0";

  public static final String CONNECTOR_PROPERTY_VERSION = "connector.property-version";
  public static final int CONNECTOR_PROPERTY_VERSION_VALUE = 1;

  public static final String CONNECTOR_ICEBERG_CATALOG_NAME = "connector.iceberg.catalog-name";
  public static final String CONNECTOR_ICEBERG_TABLE_NAME = "connector.iceberg.table-name";
  public static final String CONNECTOR_ICEBERG_CONFIGURATION_PATH = "connector.iceberg.config-path";

  public static final String CONNECTOR_ICEBERG_CATALOG_TYPE = "connector.iceberg.catalog-type";
  public static final String CONNECTOR_ICEBERG_HIVE_URI = "connector.iceberg.hive-uri";
  public static final String CONNECTOR_ICEBERG_HIVE_CLIENT_POOL_SIZE = "connector.iceberg.hive-client-pool-size";
  public static final String CONNECTOR_ICEBERG_HADOOP_WAREHOUSE = "connector.iceberg.hadoop-warehouse";
  public static final String CONNECTOR_ICEBERG_DEFAULT_DATABASE = "connector.iceberg.default-database";
  public static final String CONNECTOR_ICEBERG_BASE_NAMESPACE = "connector.iceberg.base-namespace";

  private static final IcebergValidator INSTANCE = new IcebergValidator();

  @Override
  public void validate(DescriptorProperties properties) {
    super.validate(properties);
    properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE, false);
    properties.validateString(CONNECTOR_ICEBERG_CATALOG_NAME, false, 1);
    properties.validateString(CONNECTOR_ICEBERG_TABLE_NAME, false, 1);
    properties.validateString(CONNECTOR_ICEBERG_CATALOG_TYPE, false, 1);
  }

  static IcebergValidator getInstance() {
    return INSTANCE;
  }

  static Map<String, String> getOptions(DescriptorProperties properties) {
    Map<String, String> keys = Maps.newHashMap();
    keys.put(CONNECTOR_ICEBERG_CATALOG_TYPE, FlinkCatalogFactory.ICEBERG_CATALOG_TYPE);
    keys.put(CONNECTOR_ICEBERG_HIVE_URI, FlinkCatalogFactory.HIVE_URI);
    keys.put(CONNECTOR_ICEBERG_HIVE_CLIENT_POOL_SIZE, FlinkCatalogFactory.HIVE_CLIENT_POOL_SIZE);
    keys.put(CONNECTOR_ICEBERG_HADOOP_WAREHOUSE, FlinkCatalogFactory.HADOOP_WAREHOUSE_LOCATION);
    keys.put(CONNECTOR_ICEBERG_DEFAULT_DATABASE, FlinkCatalogFactory.DEFAULT_DATABASE);
    keys.put(CONNECTOR_ICEBERG_BASE_NAMESPACE, FlinkCatalogFactory.BASE_NAMESPACE);

    Map<String, String> options = Maps.newHashMap();
    for (String icebergKey : keys.keySet()) {
      String catalogKey = keys.get(icebergKey);
      properties.getOptionalString(icebergKey).ifPresent(value -> options.put(catalogKey, value));
    }

    return options;
  }

  static Configuration getConfiguration(DescriptorProperties properties) {
    Optional<String> confPathOptional = properties
        .getOptionalString(IcebergValidator.CONNECTOR_ICEBERG_CONFIGURATION_PATH);
    if (!confPathOptional.isPresent()) {
      return new Configuration(false);
    } else {
      String confPath = null;
      try {
        confPath = confPathOptional.get();
        Configuration conf = new Configuration(false);
        conf.addResource(Paths.get(confPath, "hdfs-site.xml").toUri().toURL());
        conf.addResource(Paths.get(confPath, "core-site.xml").toUri().toURL());
        return conf;
      } catch (MalformedURLException e) {
        LOG.error("cannot find resource from path: {}.", confPath, e);
        throw new RuntimeException(String.format("cannot find resource from path: %s.", confPath));
      }
    }
  }
}
