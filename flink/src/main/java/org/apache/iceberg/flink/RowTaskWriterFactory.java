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
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.data.FlinkAvroWriter;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.taskio.FileAppenderFactory;
import org.apache.iceberg.taskio.OutputFileFactory;
import org.apache.iceberg.taskio.PartitionedFanoutWriter;
import org.apache.iceberg.taskio.TaskWriter;
import org.apache.iceberg.taskio.UnpartitionedWriter;

class RowTaskWriterFactory implements TaskWriterFactory<Row> {

  private final Schema schema;
  private final PartitionSpec spec;
  private final LocationProvider locationProvider;
  private final FileIO io;
  private final EncryptionManager encryptionManager;
  private final long targetFileSizeBytes;
  private final FileFormat fileFormat;
  private final FileAppenderFactory<Row> fileAppenderFactory;

  RowTaskWriterFactory(Schema schema,
                       PartitionSpec spec,
                       LocationProvider locationProvider,
                       FileIO io,
                       EncryptionManager encryptionManager,
                       long targetFileSizeBytes,
                       FileFormat fileFormat,
                       Map<String, String> tableProperties) {
    this.schema = schema;
    this.spec = spec;
    this.locationProvider = locationProvider;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.fileFormat = fileFormat;
    this.fileAppenderFactory = new RowFileAppenderFactory(schema, tableProperties);
  }

  @Override
  public TaskWriter<Row> create(int taskId, long attemptId) {
    // Initialize the OutputFileFactory firstly.
    OutputFileFactory outputFileFactory = new OutputFileFactory(spec, fileFormat, locationProvider,
        io, encryptionManager, taskId, attemptId);

    if (spec.fields().isEmpty()) {
      return new UnpartitionedWriter<>(spec, fileFormat, fileAppenderFactory,
          outputFileFactory, io, targetFileSizeBytes);
    } else {
      Function<Row, PartitionKey> keyGetter = buildKeyGetter(spec, schema);
      return new PartitionedFanoutWriter<>(spec, fileFormat, fileAppenderFactory, outputFileFactory,
          io, targetFileSizeBytes, keyGetter);
    }
  }

  private static Function<Row, PartitionKey> buildKeyGetter(PartitionSpec spec, Schema schema) {
    PartitionKey partitionKey = new PartitionKey(spec, schema);
    RowWrapper rowWrapper = new RowWrapper(schema.asStruct());

    return row -> {
      partitionKey.partition(rowWrapper.wrap(row));
      return partitionKey;
    };
  }

  private static class RowFileAppenderFactory implements FileAppenderFactory<Row> {
    private final Schema schema;
    private final Map<String, String> props;

    private RowFileAppenderFactory(Schema schema, Map<String, String> props) {
      this.schema = schema;
      this.props = props;
    }

    @Override
    public FileAppender<Row> newAppender(OutputFile outputFile, FileFormat format) {
      MetricsConfig metricsConfig = MetricsConfig.fromProperties(props);
      try {
        switch (format) {
          case PARQUET:
            return Parquet.write(outputFile)
                .createWriterFunc(FlinkParquetWriters::buildWriter)
                .setAll(props)
                .metricsConfig(metricsConfig)
                .schema(schema)
                .overwrite()
                .build();

          case AVRO:
            return Avro.write(outputFile)
                .createWriterFunc(FlinkAvroWriter::new)
                .setAll(props)
                .schema(schema)
                .overwrite()
                .build();

          case ORC:
          default:
            throw new UnsupportedOperationException("Cannot write unknown file format: " + format);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
