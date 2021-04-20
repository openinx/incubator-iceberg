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

package org.apache.iceberg.aliyun.dlf;

import com.aliyun.datalake20200710.Client;
import com.aliyun.datalake20200710.models.CreateDatabaseRequest;
import com.aliyun.datalake20200710.models.Database;
import com.aliyun.datalake20200710.models.DeleteDatabaseRequest;
import com.aliyun.datalake20200710.models.DeleteTableRequest;
import com.aliyun.datalake20200710.models.GetDatabaseRequest;
import com.aliyun.datalake20200710.models.GetDatabaseResponse;
import com.aliyun.datalake20200710.models.ListDatabasesRequest;
import com.aliyun.datalake20200710.models.ListDatabasesResponse;
import com.aliyun.datalake20200710.models.ListTablesRequest;
import com.aliyun.datalake20200710.models.ListTablesResponse;
import com.aliyun.datalake20200710.models.RenameTableRequest;
import com.aliyun.datalake20200710.models.Table;
import com.aliyun.datalake20200710.models.TableInput;
import com.aliyun.datalake20200710.models.UpdateDatabaseRequest;
import com.aliyun.tea.TeaException;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aliyun.AliyunClientFactory;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.aliyun.oss.OSSFileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DlfCatalog extends BaseMetastoreCatalog implements Closeable, SupportsNamespaces, Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(DlfCatalog.class);
  private static final String EMPTY_PAGE_TOKEN = "";
  private static final String DEFAULT_TABLE_NAME_PATTERN = ".*";
  private static final String DEFAULT_NAMESPACE_NAME_PATTERN = ".*";
  private static final int DEFAULT_PAGE_SIZE = 10;
  static final String NO_SUCH_OBJECT = "NoSuchObject";
  static final String ALREADY_EXISTS = "AlreadyExists";

  private String catalogName;
  private String warehousePath;
  private AliyunProperties aliyunProperties;
  private Client dlfClient;
  private FileIO fileIO;

  private Configuration hadoopConf;

  /**
   * No-arg constructor to load the catalog dynamically. All fields are initialized by calling
   * {@link DlfCatalog#initialize(String, Map)} later.
   */
  public DlfCatalog() {
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    initialize(
        name,
        properties.get(CatalogProperties.WAREHOUSE_LOCATION),
        new AliyunProperties(properties),
        AliyunClientFactory.load(properties).dlfClient(),
        initializeFileIO(properties)
    );
  }

  private FileIO initializeFileIO(Map<String, String> properties) {
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    if (fileIOImpl == null) {
      FileIO io = new OSSFileIO();
      io.initialize(properties);
      return io;
    } else {
      return CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
    }
  }

  void initialize(String name,
                  String path,
                  AliyunProperties properties,
                  Client client,
                  FileIO io) {
    this.catalogName = name;
    this.warehousePath = cleanWarehousePath(path);
    this.aliyunProperties = properties;
    this.dlfClient = client;
    this.fileIO = io;
  }

  private String cleanWarehousePath(String path) {
    Preconditions.checkArgument(path != null && path.length() > 0,
        "Cannot initialize DlfCatalog because warehousePath must not be null");
    int len = path.length();
    if (path.charAt(len - 1) == '/') {
      return path.substring(0, len - 1);
    } else {
      return path;
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new DlfTableOperations(dlfClient, warehousePath, catalogName, aliyunProperties, fileIO, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String dbLocationUri;

    // Request data lake format services to get the database location uri.
    try {
      GetDatabaseRequest request = new GetDatabaseRequest();
      request.setCatalogId(aliyunProperties.dlfCatalogId());
      request.setName(IcebergToDlfConverter.toDatabaseName(tableIdentifier.namespace()));

      GetDatabaseResponse response = dlfClient.getDatabase(request);
      dbLocationUri = response.getBody().getDatabase().getLocationUri();
    } catch (TeaException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (dbLocationUri != null) {
      return String.format("%s/%s", dbLocationUri, tableIdentifier.name());
    }

    return String.format("%s/%s.db/%s",
        warehousePath,
        tableIdentifier,
        tableIdentifier.name());
  }

  private static boolean isDlfIcebergTable(Table table) {
    return table.getParameters() != null &&
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
            table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP)
        );
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    namespaceExists(namespace);

    // Should be safe to list all before returning the list, instead of dynamically load the list.
    List<TableIdentifier> results = Lists.newArrayList();
    String nextPageToken = EMPTY_PAGE_TOKEN;
    try {
      do {
        ListTablesRequest request = new ListTablesRequest()
            .setCatalogId(aliyunProperties.dlfCatalogId())
            .setDatabaseName(IcebergToDlfConverter.toDatabaseName(namespace))
            .setTableNamePattern(DEFAULT_TABLE_NAME_PATTERN)
            .setPageSize(DEFAULT_PAGE_SIZE)
            .setNextPageToken(nextPageToken);

        ListTablesResponse response = dlfClient.listTables(request);
        nextPageToken = response.getBody().getNextPageToken();

        List<Table> tables = response.getBody().getTables();
        if (!tables.isEmpty()) {
          results.addAll(tables.stream()
              .filter(DlfCatalog::isDlfIcebergTable)
              .map(DlfToIcebergConverter::toTableId)
              .collect(Collectors.toList()));
        }
      } while (!Objects.equals(nextPageToken, EMPTY_PAGE_TOKEN));

    } catch (TeaException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    LOG.debug("Listing of namespace: {} resulted in the following tables: {}", namespace, results);
    return results;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    try {
      TableOperations ops = newTableOps(identifier);
      TableMetadata lastMetadata = ops.current();

      DeleteTableRequest request = new DeleteTableRequest()
          .setCatalogId(aliyunProperties.dlfCatalogId())
          .setDatabaseName(IcebergToDlfConverter.toDatabaseName(identifier.namespace()))
          .setTableName(identifier.name());

      dlfClient.deleteTable(request);
      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
        LOG.info("DLF table {} data purged", identifier);
      }

      LOG.info("Dropped table: {}", identifier);
      return true;

    } catch (TeaException e) {
      if (Objects.equals(e.getCode(), NO_SUCH_OBJECT)) {
        LOG.error("Cannot drop table {} because table does not found or is not accessible", identifier, e);
        return false;
      }
      throw e;
    } catch (Exception e) {
      LOG.error("Cannot complete drop table operation for {} due to unexpected exception", identifier, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (!Objects.equals(from.namespace(), to.namespace())) {
      throw new IllegalArgumentException(
          String.format("Cannot rename %s to %s because their namespace are different.", from, to));
    }

    try {
      // According to the API doc, we only need to set the tableName.
      TableInput tableInput = new TableInput()
          .setTableName(to.name());

      RenameTableRequest renameTableRequest = new RenameTableRequest()
          .setCatalogId(aliyunProperties.dlfCatalogId())
          .setDatabaseName(IcebergToDlfConverter.toDatabaseName(to.namespace()))
          .setTableInput(tableInput)
          .setTableName(from.name());

      dlfClient.renameTable(renameTableRequest);

      LOG.info("Successfully renamed table from {} to {}", from, to);
    } catch (TeaException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    try {
      CreateDatabaseRequest request = new CreateDatabaseRequest()
          .setCatalogId(aliyunProperties.dlfCatalogId())
          .setDatabaseInput(IcebergToDlfConverter.toDatabaseInput(namespace, metadata));

      dlfClient.createDatabase(request);
      LOG.info("Created namespace: {}", namespace);
    } catch (TeaException e) {
      if (Objects.equals(e.getCode(), ALREADY_EXISTS)) {
        throw new AlreadyExistsException(
            "Cannot create namespace %s because it already exists in DLF", namespace);
      }
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (!namespace.isEmpty()) {
      // If it is not a list all op, just check if the namespace exists and return empty.
      if (namespaceExists(namespace)) {
        return Lists.newArrayList();
      }

      throw new NoSuchNamespaceException(
          "DataLakeFormat does not support nested namespace, cannot list namespaces under %s", namespace);
    }

    String nextPageToken = EMPTY_PAGE_TOKEN;
    List<Namespace> results = Lists.newArrayList();

    try {
      do {
        ListDatabasesRequest request = new ListDatabasesRequest()
            .setCatalogId(aliyunProperties.dlfCatalogId())
            .setNextPageToken(nextPageToken)
            .setNamePattern(DEFAULT_NAMESPACE_NAME_PATTERN)
            .setPageSize(DEFAULT_PAGE_SIZE);

        ListDatabasesResponse response = dlfClient.listDatabases(request);
        nextPageToken = response.getBody().getNextPageToken();

        List<Database> databases = response.getBody().getDatabases();
        if (!databases.isEmpty()) {
          results.addAll(databases.stream()
              .map(DlfToIcebergConverter::toNamespace)
              .collect(Collectors.toList()));
        }

      } while (!Objects.equals(nextPageToken, EMPTY_PAGE_TOKEN));

    } catch (TeaException e) {
      throw e;
    } catch (Throwable throwable) {
      throw new RuntimeException("Failed to list the databases from DataLakeFormat", throwable);
    }

    LOG.debug("Listing namespace {} returned namespaces: {}", namespace, results);
    return results;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    String databaseName = IcebergToDlfConverter.toDatabaseName(namespace);

    try {
      GetDatabaseRequest request = new GetDatabaseRequest()
          .setCatalogId(aliyunProperties.dlfCatalogId())
          .setName(databaseName);

      GetDatabaseResponse response = dlfClient.getDatabase(request);
      Map<String, String> metadata = response.getBody().getDatabase().getParameters();
      LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata);
      return metadata;

    } catch (TeaException e) {
      if (Objects.equals(e.getCode(), NO_SUCH_OBJECT)) {
        throw new NoSuchNamespaceException("DLF database does not find for namespace %s, error message: %s",
            databaseName, e.getMessage());
      }
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(String.format("Failed to find DataLakeFormat database for namespace %s", namespace),
          e);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    namespaceExists(namespace);

    try {
      // List only one page of tables under the given database, for testing whether namespace is empty or not.
      ListTablesRequest request = new ListTablesRequest()
          .setCatalogId(aliyunProperties.dlfCatalogId())
          .setDatabaseName(IcebergToDlfConverter.toDatabaseName(namespace))
          .setTableNamePattern(DEFAULT_TABLE_NAME_PATTERN)
          .setPageSize(DEFAULT_PAGE_SIZE)
          .setNextPageToken(EMPTY_PAGE_TOKEN);

      ListTablesResponse response = dlfClient.listTables(request);
      List<Table> tables = response.getBody().getTables();

      if (!tables.isEmpty()) {
        Table table = tables.get(0);
        if (isDlfIcebergTable(table)) {
          throw new NamespaceNotEmptyException("Cannot drop namespace %s because it still contains iceberg tables",
              namespace);
        } else {
          throw new NamespaceNotEmptyException("Cannot drop namespace %s because it still contains non-iceberg tables",
              namespace);
        }
      }

      // Delete the given database from DataLakeFormat.
      DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest()
          .setCascade(false)
          .setCatalogId(aliyunProperties.dlfCatalogId())
          .setName(IcebergToDlfConverter.toDatabaseName(namespace));

      dlfClient.deleteDatabase(deleteDatabaseRequest);
      LOG.info("Dropped namespace: {}", namespace);

    } catch (TeaException | NamespaceNotEmptyException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to request to data lake services", e);
    }

    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    Map<String, String> newProperties = Maps.newHashMap();
    newProperties.putAll(loadNamespaceMetadata(namespace));
    newProperties.putAll(properties);

    try {
      UpdateDatabaseRequest request = new UpdateDatabaseRequest()
          .setCatalogId(aliyunProperties.dlfCatalogId())
          .setName(IcebergToDlfConverter.toDatabaseName(namespace))
          .setDatabaseInput(IcebergToDlfConverter.toDatabaseInput(namespace, newProperties));

      dlfClient.updateDatabase(request);
      LOG.error("Successfully set properties {} for {}", properties.keySet(), namespace);
      // Always successful, otherwise exception is thrown.
      return true;

    } catch (TeaException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    Map<String, String> metadata = Maps.newHashMap(loadNamespaceMetadata(namespace));
    for (String property : properties) {
      metadata.remove(property);
    }

    try {
      UpdateDatabaseRequest request = new UpdateDatabaseRequest()
          .setCatalogId(aliyunProperties.dlfCatalogId())
          .setName(IcebergToDlfConverter.toDatabaseName(namespace))
          .setDatabaseInput(IcebergToDlfConverter.toDatabaseInput(namespace, metadata));

      dlfClient.updateDatabase(request);
      LOG.debug("Successfully removed properties {} from {}", properties, namespace);
      // Always successful, otherwise exception is thrown.
      return true;

    } catch (TeaException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    // TODO
    return true;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void close() {
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  @Override
  public Configuration getConf() {
    return hadoopConf;
  }
}
