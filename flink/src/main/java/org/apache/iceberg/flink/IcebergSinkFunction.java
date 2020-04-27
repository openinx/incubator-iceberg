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

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.flink.writer.DynamicPartitionWriter;
import org.apache.iceberg.flink.writer.FileAppenderFactory;
import org.apache.iceberg.flink.writer.PartitionedWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

public class IcebergSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>>
    implements CheckpointedFunction, CheckpointListener {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkFunction.class);

  private final String tableLocation;
  private final SerializableConfiguration hadoopConf;
  private final RowTypeInfo schema;

  private transient Table table;
  private transient FileFormat fileFormat;
  private transient GlobalTableCommitter globalCommitter;

  // TODO should make it to be a persisted state in flink state backend ???
  private transient int fileCount;

  private transient PartitionedWriter<Row> writer;

  // ---- Sink state related fields. ----
  // State descriptors used to identify the listState.
  private static final ListStateDescriptor<byte[]> ICEBERG_STATE = new ListStateDescriptor<>(
      "iceberg-state", BytePrimitiveArraySerializer.INSTANCE);
  private transient ListState<byte[]> globalStates;
  private transient CheckpointTaskState.StateSerializer stateSerializer;
  private transient NavigableMap<Long, List<DataFile>> completeFilesPerCheckpoint;

  /**
   * TODO Need to validate the Flink schema and iceberg schema.
   * NOTICE don't do any initialization work in this constructor, because in {@link DataStream#addSink(SinkFunction)}
   * it will call {@link ClosureCleaner#clean(Object, ExecutionConfig.ClosureCleanerLevel, boolean)} to set all the
   * non-serializable inner members to be null.
   *
   * @param tableLocation What's the base path of the iceberg table.
   * @param schema        The defined Flink table schema. TODO Better to use the new FLINK API: TableSchema.
   */
  public IcebergSinkFunction(String tableLocation, RowTypeInfo schema) {
    this(tableLocation, schema, new Configuration());
  }

  /**
   * TODO Need to validate the Flink schema and iceberg schema.
   * NOTICE don't do any initialization work in this constructor, because in {@link DataStream#addSink(SinkFunction)}
   * it will call {@link ClosureCleaner#clean(Object, ExecutionConfig.ClosureCleanerLevel, boolean)} to set all the
   * non-serializable inner members to be null.
   *
   * @param tableLocation What's the base path of the iceberg table.
   * @param schema        The defined Flink table schema.
   * @param conf          The distribute table configuration.
   */
  public IcebergSinkFunction(String tableLocation, RowTypeInfo schema, Configuration conf) {
    this.tableLocation = tableLocation;
    this.schema = schema;
    if (conf == null) {
      this.hadoopConf = new SerializableConfiguration(new Configuration());
    } else {
      this.hadoopConf = new SerializableConfiguration(conf);
    }
  }

  /**
   * TODO need to handle the restart case which would remain many unfinished orphan data files ???
   *
   * @param context of the state.
   * @throws Exception if any error happen.
   */
  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    this.fileCount = 0;

    // TODO Let the hadoop tables can run on hadoop distributed file system.
    // Initialize the hadoop table and scheam etc.
    Tables tables = new HadoopTables(hadoopConf.get());
    this.table = tables.load(tableLocation);
    // Validate the FLINK schema.
    FlinkSchemaUtil.convert(schema);
    this.fileFormat = getFileFormat();

    // Initialize the global table committer
    this.globalCommitter = new GlobalTableCommitter(
        (StreamingRuntimeContext) this.getRuntimeContext(),
        tableLocation,
        hadoopConf
    );

    // Initialize the PartitionWriter instance.
    FileAppenderFactory<Row> appenderFactory = new FlinkFileAppenderFactory();
    Function<PartitionKey, EncryptedOutputFile> outputFileGetter = this::createEncryptedOutputFile;
    PartitionKey.Builder builder = new PartitionKey.Builder(table.spec());
    this.writer = new DynamicPartitionWriter<>(table.spec(),
        appenderFactory,
        outputFileGetter,
        builder::build,
        getTargetFileSizeBytes(),
        fileFormat);

    // Initialize the FLINK state.
    this.stateSerializer = CheckpointTaskState.createSerializer(table.spec().partitionType());
    this.globalStates = context.getOperatorStateStore().getUnionListState(ICEBERG_STATE);
    this.completeFilesPerCheckpoint = new TreeMap<>();
    if (context.isRestored()) {
      this.completeFilesPerCheckpoint = unpackGlobalStates();
    }
  }

  @Override
  public void invoke(Tuple2<Boolean, Row> tuple, SinkFunction.Context context) throws Exception {
    if (tuple.f0) {
      this.writer.write(tuple.f1);
    } else {
      throw new UnsupportedOperationException("Not support DELETE yet.");
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    long checkpointId = context.getCheckpointId();
    LOG.info("Start to flush snapshot state to state backend, table: {}, checkpointId: {}", table, checkpointId);
    writer.close();
    List<DataFile> dataFiles = writer.getCompleteFiles();
    completeFilesPerCheckpoint.put(checkpointId, Lists.newArrayList(dataFiles.iterator()));
    // Remember to clear the writer's complete files.
    dataFiles.clear();

    // Update the persisted state.
    globalStates.clear();
    globalStates.addAll(packGlobalStates(completeFilesPerCheckpoint));
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    LOG.info("The checkpoint with checkpoint id {} has been completed.", checkpointId);
    NavigableMap<Long, List<DataFile>> dataFilesToCommit = completeFilesPerCheckpoint.headMap(checkpointId, true);
    // NOTICE: we should commit the table committer even if the complete file list is empty, because the
    // GlobalTableCommitter will commit the table only if all tasks have submit their pending data files.
    // If lacking the checkpoint task state from one task, then the table commit will be blocked.
    long committedCkpId = globalCommitter.commit(dataFilesToCommit);
    if (committedCkpId > 0) {
      // Remove all the files whose checkpoint id <= MaxCommittedCheckpointId from the CACHE.
      completeFilesPerCheckpoint.headMap(committedCkpId, true).clear();
    }
  }

  private NavigableMap<Long, List<DataFile>> unpackGlobalStates() throws Exception {
    NavigableMap<Long, List<DataFile>> dataFileMap = new TreeMap<>();
    for (byte[] serializeData : globalStates.get()) {
      CheckpointTaskState ckpTaskState = SimpleVersionedSerialization
          .readVersionAndDeSerialize(stateSerializer, serializeData);
      dataFileMap.compute(ckpTaskState.getCheckpointId(), (ckpId, existedDataFiles) -> {
        existedDataFiles = existedDataFiles == null ? Lists.newArrayList() : existedDataFiles;
        existedDataFiles.addAll(ckpTaskState.getDataFiles());
        return existedDataFiles;
      });
    }
    return dataFileMap;
  }

  private List<byte[]> packGlobalStates(NavigableMap<Long, List<DataFile>> dataFiles) throws Exception {
    List<byte[]> states = Lists.newArrayList();
    for (Map.Entry<Long, List<DataFile>> ckp : dataFiles.entrySet()) {
      CheckpointTaskState ckpTaskTate = new CheckpointTaskState(ckp.getKey(), ckp.getValue());
      byte[] serializeData = SimpleVersionedSerialization.writeVersionAndSerialize(stateSerializer, ckpTaskTate);
      states.add(serializeData);
    }
    return states;
  }

  @Override
  public void close() throws Exception {
    this.writer.close();
    this.writer = null;
  }

  private String generateFilename() {
    String uuid = UUID.randomUUID().toString();
    int hashCode = Math.abs(this.hashCode() % 10 ^ 5);
    int taskId = 0;
    try {
      taskId = this.getRuntimeContext().getIndexOfThisSubtask();
    } catch (IllegalStateException e) {
      // For testing TestIcebergSinkFunction purpose, need to remove this catch in future.
    }
    return fileFormat.addExtension(String.format("%05d-%d-%s-%05d", hashCode, taskId, uuid, fileCount++));
  }

  private EncryptedOutputFile createEncryptedOutputFile(PartitionKey partitionKey) {
    // TODO handle the partition and unpartitioned case.
    assert table.specs().size() > 0 : "Don't support unpartitioned table now, it's a TODO issue";

    String dataLocation = table
        .locationProvider()
        .newDataLocation(table.spec(), partitionKey, generateFilename());
    OutputFile outputFile = table.io().newOutputFile(dataLocation);
    return table.encryption().encrypt(outputFile);
  }

  private FileFormat getFileFormat() {
    String formatString = table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  private long getTargetFileSizeBytes() {
    return PropertyUtil.propertyAsLong(table.properties(),
        WRITE_TARGET_FILE_SIZE_BYTES,
        WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }

  private class FlinkFileAppenderFactory implements FileAppenderFactory<Row> {

    @Override
    public FileAppender<Row> newAppender(OutputFile outputFile, FileFormat format) {
      MetricsConfig metricsConfig = MetricsConfig.fromProperties(table.properties());
      try {
        switch (format) {
          case PARQUET:
            return Parquet.write(outputFile)
                .createWriterFunc(FlinkParquetWriters::buildWriter)
                .setAll(table.properties())
                .metricsConfig(metricsConfig)
                .schema(table.schema())
                .overwrite()
                .build();

          case AVRO:
            return Avro.write(outputFile)
                // TODO implement this write function.
                .createWriterFunc(ignored -> null)
                .setAll(table.properties())
                .schema(table.schema())
                .overwrite()
                .build();

          default:
            throw new UnsupportedOperationException("Cannot write unknown format: " + format);
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }
  }
}
