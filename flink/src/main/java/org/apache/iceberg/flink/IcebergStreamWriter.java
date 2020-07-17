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
import java.util.Locale;
import java.util.Map;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.taskio.TaskWriter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

class IcebergStreamWriter<T> extends AbstractStreamOperator<DataFile>
    implements OneInputStreamOperator<T, DataFile>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergStreamWriter.class);

  private final String tablePath;
  private final TaskWriterFactory<T> taskWriterFactory;


  private transient TaskWriter<T> writer;
  private transient int subTaskId;
  private transient int attemptId;

  private IcebergStreamWriter(String tablePath, TaskWriterFactory<T> taskWriterFactory) {
    this.tablePath = tablePath;
    this.taskWriterFactory = taskWriterFactory;
  }

  @Override
  public void open() {
    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();

    // Initialize the task writer.
    this.writer = taskWriterFactory.create(subTaskId, attemptId);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    LOG.info("Iceberg writer({}) begin preparing for checkpoint {}", toString(), checkpointId);
    // close all open files and emit files to downstream committer operator
    writer.close();

    writer.pollCompleteFiles().forEach(this::emit);
    LOG.info("Iceberg writer({}) completed preparing for checkpoint {}", toString(), checkpointId);
  }

  @Override
  public void processElement(StreamRecord<T> element) throws Exception {
    T value = element.getValue();
    writer.write(value);

    // Emit the data file entries to downstream committer operator if there exist any complete files.
    writer.pollCompleteFiles().forEach(this::emit);
  }

  @Override
  public void dispose() throws Exception {
    super.dispose();
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  @Override
  public void endInput() throws IOException {
    // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the remaining
    // data files to downstream before closing the writer so that we won't miss any of them.
    writer.close();
    for (DataFile dataFile : writer.pollCompleteFiles()) {
      emit(dataFile);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table_path", tablePath)
        .add("subtask_id", subTaskId)
        .add("attempt_id", attemptId)
        .toString();
  }

  private void emit(DataFile dataFile) {
    output.collect(new StreamRecord<>(dataFile));
  }

  static IcebergStreamWriter<Row> createStreamWriter(String path, TableSchema tableSchema, Configuration conf) {
    Preconditions.checkArgument(path != null, "Iceberg table path should't be null");

    Table table = TableUtil.findTable(path, conf);
    if (tableSchema != null) {
      Schema writeSchema = FlinkSchemaUtil.convert(tableSchema);
      // Reassign ids to match the existing table schema.
      writeSchema = TypeUtil.reassignIds(writeSchema, table.schema());
      TypeUtil.validateWriteSchema(table.schema(), writeSchema, true, true);
    }

    Map<String, String> props = table.properties();
    long targetFileSize = getTargetFileSizeBytes(props);
    FileFormat fileFormat = getFileFormat(props);

    TaskWriterFactory<Row> factory = new RowTaskWriterFactory(table.schema(), table.spec(),
        table.locationProvider(), table.io(), table.encryption(), targetFileSize, fileFormat, props);

    return new IcebergStreamWriter<>(path, factory);
  }

  static FileFormat getFileFormat(Map<String, String> properties) {
    String formatString = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
  }

  static long getTargetFileSizeBytes(Map<String, String> properties) {
    return PropertyUtil.propertyAsLong(properties,
        WRITE_TARGET_FILE_SIZE_BYTES,
        WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }
}
