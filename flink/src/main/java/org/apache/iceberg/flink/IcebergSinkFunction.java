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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.flink.writer.FileAppenderFactory;
import org.apache.iceberg.flink.writer.OutputFileFactory;
import org.apache.iceberg.flink.writer.TaskWriter;
import org.apache.iceberg.flink.writer.TaskWriterFactory;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
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
  private Schema readSchema;

  private transient Table table;
  private transient GlobalTableCommitter globalCommitter;

  private transient TaskWriter<Row> writer;

  // ---- Sink state related fields. ----
  // State descriptors used to identify the listState.
  private static final ListStateDescriptor<byte[]> ICEBERG_STATE = new ListStateDescriptor<>(
      "iceberg-state", BytePrimitiveArraySerializer.INSTANCE);
  private transient ListState<byte[]> globalStates;
  private transient CheckpointTaskState.StateSerializer stateSerializer;
  private transient NavigableMap<Long, List<DataFile>> completeFilesPerCheckpoint;

  /**
   * Be careful to do any initialization in this constructor, because in {@link DataStream#addSink(SinkFunction)}
   * it will call {@link ClosureCleaner#clean(Object, ExecutionConfig.ClosureCleanerLevel, boolean)} to set all the
   * non-serializable inner members to be null.
   *
   * @param tableLocation What's the base path of the iceberg table.
   * @param readSchema    The schema of source data which will be flowed to iceberg table.
   * @param conf          The hadoop's configuration.
   */
  private IcebergSinkFunction(String tableLocation, Schema readSchema, Configuration conf) {
    this.tableLocation = tableLocation;
    this.hadoopConf = new SerializableConfiguration(conf == null ? new Configuration() : conf);
    this.readSchema = readSchema;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    // Load the iceberg table if exist, otherwise just create a new one.
    Tables tables = new HadoopTables(hadoopConf.get());
    this.table = tables.load(tableLocation);
    if (this.readSchema != null) {
      // reassign ids to match the existing table schema
      readSchema = TypeUtil.reassignIds(readSchema, table.schema());
      FlinkSchemaUtil.validate(readSchema, table.schema(), true, true);
    }

    // Initialize the global table committer
    this.globalCommitter = new GlobalTableCommitter(
        (StreamingRuntimeContext) this.getRuntimeContext(),
        tableLocation,
        hadoopConf
    );

    // Initialize the task writer.
    FileFormat fileFormat = getFileFormat();
    FileAppenderFactory<Row> appenderFactory = new FlinkFileAppenderFactory();
    OutputFileFactory outputFileFactory = new OutputFileFactory(table,
        fileFormat, getRuntimeContext().getIndexOfThisSubtask());
    this.writer = TaskWriterFactory.createTaskWriter(table.spec(),
        appenderFactory,
        outputFileFactory,
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
    if (writer != null) {
      writer.close();
      writer = null;
    }
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
            // TODO support to write AVRO file format.
          default:
            throw new UnsupportedOperationException("Cannot write unknown format: " + format);
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String tableLocation;
    private TableSchema tableSchema;
    private Configuration conf = new Configuration();

    public Builder withTableLocation(String newTableLocation) {
      this.tableLocation = newTableLocation;
      return this;
    }

    public Builder withTableSchema(TableSchema newTableSchema) {
      this.tableSchema = newTableSchema;
      return this;
    }

    public Builder withConfiguration(Configuration newConf) {
      this.conf = newConf;
      return this;
    }

    public IcebergSinkFunction build() {
      Preconditions.checkArgument(tableLocation != null, "Iceberg table location should't be null");
      Schema schema = tableSchema == null ? null : FlinkSchemaUtil.convert(tableSchema);
      return new IcebergSinkFunction(tableLocation, schema, conf);
    }
  }
}
