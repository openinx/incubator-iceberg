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
import com.aliyun.datalake20200710.models.CreateLockRequest;
import com.aliyun.datalake20200710.models.CreateLockResponse;
import com.aliyun.datalake20200710.models.CreateTableRequest;
import com.aliyun.datalake20200710.models.GetTableRequest;
import com.aliyun.datalake20200710.models.LockObj;
import com.aliyun.datalake20200710.models.LockStatus;
import com.aliyun.datalake20200710.models.Table;
import com.aliyun.datalake20200710.models.TableInput;
import com.aliyun.datalake20200710.models.UnLockRequest;
import com.aliyun.datalake20200710.models.UpdateTableRequest;
import com.aliyun.tea.TeaException;
import com.aliyun.teautil.models.RuntimeOptions;
import java.util.ConcurrentModificationException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DlfTableOperations extends BaseMetastoreTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(DlfTableOperations.class);
  private static final String DLF_EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";
  private static final String LOCK_STATE_ACQUIRED = "ACQUIRED";
  private static final String METASTORE_LOCK_ID = "metastore-lock-id";

  private final Client dlfClient;
  private final String catalogName;
  private final AliyunProperties aliyunProperties;
  private final FileIO io;

  private final String databaseName;
  private final String tableName;
  private final String fullTableName;

  DlfTableOperations(Client dlfClient,
                     String catalogName,
                     AliyunProperties aliyunProperties,
                     FileIO io,
                     TableIdentifier tableIdentifier) {
    this.dlfClient = dlfClient;
    this.catalogName = catalogName;
    this.aliyunProperties = aliyunProperties;
    this.io = io;

    this.databaseName = IcebergToDlfConverter.toDatabaseName(tableIdentifier.namespace());
    this.tableName = IcebergToDlfConverter.getTableName(tableIdentifier);
    this.fullTableName = String.format("%s.%s.%s", this.catalogName, databaseName, tableName);
  }

  @Override
  public FileIO io() {
    return io;
  }

  @Override
  protected String tableName() {
    return fullTableName;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    Table table = getDlfTable();
    if (table != null) {
      DlfToIcebergConverter.validateTable(table, tableName());
      metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
    } else {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("Cannot find DLF table %s after refresh, " +
            "maybe another process deleted it or revoked your access permission", tableName());
      }
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
    CommitStatus commitStatus = CommitStatus.FAILURE;

    Long lockId = null;
    try {
      lockId = lock(newMetadataLocation);

      Table dlfTable = getDlfTable();
      checkMetadataLocation(dlfTable, base);

      Map<String, String> properties = prepareProperties(dlfTable, newMetadataLocation);
      persistDLFTable(lockId, dlfTable, properties);
      commitStatus = CommitStatus.SUCCESS;

    } catch (ConcurrentModificationException e) {
      throw new CommitFailedException(e, "Cannot commit %s because DLF detected concurrent update", tableName());
    } catch (RuntimeException persistFailure) {
      LOG.error("Confirming if commit to {} indeed failed to persist, attempting to reconnect and check.",
          fullTableName, persistFailure);
      commitStatus = checkCommitStatus(newMetadataLocation, metadata);

      switch (commitStatus) {
        case SUCCESS:
          break;
        case FAILURE:
          throw new CommitFailedException(persistFailure,
              "Cannot commit %s due to unexpected exception", tableName());
        case UNKNOWN:
          throw new CommitStateUnknownException(persistFailure);
      }
    } finally {
      cleanupMetadataAndUnlock(lockId, commitStatus, newMetadataLocation);
    }
  }

  private Long lock(String newMetadataLocation) {
    try {
      LockObj lock = new LockObj()
          .setCatalogId(aliyunProperties.dlfCatalogId())
          .setDatabaseName(databaseName)
          .setTableName(tableName);

      CreateLockRequest request = new CreateLockRequest()
          .setLockObjList(Lists.newArrayList(lock));

      CreateLockResponse response = dlfClient.createLock(request);
      LockStatus lockStatus = response.getBody().getLockStatus();

      if (!Objects.equals(lockStatus.getLockState(), LOCK_STATE_ACQUIRED)) {
        throw new IllegalStateException(String.format("Fail to acquire lock %s to commit new metadata at %s",
            fullTableName, newMetadataLocation));
      }

      return lockStatus.lockId;

    } catch (TeaException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void checkMetadataLocation(Table dlfTable, TableMetadata base) {
    String dlfMetadataLocation = dlfTable != null ? dlfTable.getParameters().get(METADATA_LOCATION_PROP) : null;
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
    if (!Objects.equals(baseMetadataLocation, dlfMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s because base metadata location '%s' is not same as the current DLF location '%s'",
          tableName(), baseMetadataLocation, dlfMetadataLocation);
    }
  }

  private Table getDlfTable() {
    try {
      GetTableRequest request = new GetTableRequest()
          .setCatalogId(aliyunProperties.dlfCatalogId())
          .setDatabaseName(databaseName)
          .setTableName(tableName);

      return dlfClient.getTable(request).getBody().getTable();
    } catch (Exception e) {
      return null;
    }
  }

  private Map<String, String> prepareProperties(Table dlfTable, String newMetadataLocation) {
    Map<String, String> properties = dlfTable != null ? Maps.newHashMap(dlfTable.getParameters()) : Maps.newHashMap();
    properties.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    properties.put(METADATA_LOCATION_PROP, newMetadataLocation);
    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      properties.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    return properties;
  }

  private void persistDLFTable(Long lockId, Table dlfTable, Map<String, String> parameters) {
    // TODO check whether should we fill all the related table information (such as columns, partition specs) so that
    // TODO people could see their table schema in aliyun web console.
    TableInput tableInput = new TableInput()
        .setDatabaseName(databaseName)
        .setTableType(DLF_EXTERNAL_TABLE_TYPE)
        .setTableName(tableName)
        .setParameters(parameters);

    try {
      if (dlfTable != null) {
        LOG.info("Committing existing DLF table: {}", tableName());

        UpdateTableRequest request = new UpdateTableRequest()
            .setCatalogId(aliyunProperties.dlfCatalogId())
            .setDatabaseName(databaseName)
            .setTableName(tableName)
            .setSkipArchive(false) /* TODO Make this to be an configurable option. */
            .setTableInput(tableInput);

        // Update DLF table with acquired lock ID.
        Map<String, String> headers = ImmutableMap.of(METASTORE_LOCK_ID, lockId.toString());
        dlfClient.updateTableWithOptions(request, headers, new RuntimeOptions());

      } else {
        LOG.info("Committing new DLF table: {}", tableName());

        CreateTableRequest request = new CreateTableRequest()
            .setCatalogId(aliyunProperties.dlfCatalogId())
            .setDatabaseName(databaseName)
            .setTableInput(tableInput);

        // Create DLF table with the acquired lock ID.
        Map<String, String> headers = ImmutableMap.of(METASTORE_LOCK_ID, lockId.toString());
        dlfClient.createTableWithOptions(request, headers, new RuntimeOptions());
      }
    } catch (TeaException e) {
      if (Objects.equals(e.getCode(), DlfCatalog.ALREADY_EXISTS)) {
        throw new AlreadyExistsException(e,
            "Cannot commit %s because its DLF table already exists when trying to create one", tableName);
      }
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void cleanupMetadataAndUnlock(Long lockId, CommitStatus commitStatus, String metadataLocation) {
    try {
      if (commitStatus == CommitStatus.FAILURE) {
        // if anything went wrong, clean up the uncommitted metadata file
        io().deleteFile(metadataLocation);
      }
    } catch (RuntimeException e) {
      LOG.error("Fail to cleanup metadata file at {}", metadataLocation, e);
      throw e;
    } finally {
      if (lockId != null) {
        unlock(lockId);
      }
    }
  }

  private void unlock(long lockId) {
    try {
      UnLockRequest request = new UnLockRequest()
          .setLockId(lockId);
      dlfClient.unLock(request);

    } catch (TeaException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
