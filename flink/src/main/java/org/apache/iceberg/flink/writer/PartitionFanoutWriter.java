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

package org.apache.iceberg.flink.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.PartitionKey;
import org.apache.iceberg.io.FileAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PartitionFanoutWriter will open a writing data file for each partition and route the given record to the
 * corresponding data file in the correct partition.
 *
 * @param <T> defines the data type of record to write.
 */
public class PartitionFanoutWriter<T> implements TaskWriter<T> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionFanoutWriter.class);

  private final PartitionSpec spec;
  private final FileAppenderFactory<T> factory;
  private final Function<PartitionKey, EncryptedOutputFile> outputFileGetter;
  private final Function<T, PartitionKey> keyGetter;
  private final Map<PartitionKey, WrappedFileAppender<T>> writers;
  private final long targetFileSize;
  private final FileFormat fileFormat;
  private final List<DataFile> completeDataFiles;

  public PartitionFanoutWriter(PartitionSpec spec,
                               FileAppenderFactory<T> factory,
                               Function<PartitionKey, EncryptedOutputFile> outputFileGetter,
                               Function<T, PartitionKey> keyGetter,
                               long targetFileSize,
                               FileFormat fileFormat) {
    this.spec = spec;
    this.factory = factory;
    this.outputFileGetter = outputFileGetter;
    this.keyGetter = keyGetter;
    this.writers = new HashMap<>();
    this.targetFileSize = targetFileSize;
    this.fileFormat = fileFormat;
    this.completeDataFiles = new ArrayList<>();
  }

  @Override
  public void write(T record) throws IOException {
    PartitionKey partitionKey = keyGetter.apply(record);
    Preconditions.checkArgument(partitionKey != null, "Partition key shouldn't be null");

    WrappedFileAppender<T> writer = writers.get(partitionKey);
    if (writer == null) {
      writer = createWrappedFileAppender(partitionKey);
      writers.put(partitionKey, writer);
    }
    writer.fileAppender.add(record);

    // Roll the writer if reach the target file size.
    if (writer.fileAppender.length() >= targetFileSize) {
      closeFileAppender(writer);
      // Open a new file appender and put it into the writers map.
      writers.put(partitionKey, createWrappedFileAppender(partitionKey));
    }
  }

  @Override
  public void close() throws IOException {
    for (WrappedFileAppender<T> appender : writers.values()) {
      closeFileAppender(appender);
      LOG.info("Close file appender: {}, completeDataFiles: {}",
          appender.encryptedOutputFile.encryptingOutputFile().location(),
          completeDataFiles.size());
    }
    this.writers.clear();
  }

  @Override
  public List<DataFile> getCompleteFiles() {
    return this.completeDataFiles;
  }

  private WrappedFileAppender<T> createWrappedFileAppender(PartitionKey partitionKey) {
    EncryptedOutputFile outputFile = outputFileGetter.apply(partitionKey);
    FileAppender<T> appender = factory.newAppender(outputFile.encryptingOutputFile(), fileFormat);
    return new WrappedFileAppender<>(partitionKey, outputFile, appender);
  }

  private void closeFileAppender(WrappedFileAppender<T> wrap) throws IOException {
    wrap.fileAppender.close();

    // Construct the DataFile and add it into the completeDataFiles.
    DataFile dataFile = DataFiles.builder(spec)
        .withEncryptedOutputFile(wrap.encryptedOutputFile)
        .withPartition(spec.fields().size() == 0 ? null : wrap.partitionKey)
        .withMetrics(wrap.fileAppender.metrics())
        .withSplitOffsets(wrap.fileAppender.splitOffsets())
        .build();
    completeDataFiles.add(dataFile);
  }

  private static class WrappedFileAppender<T> {
    private final PartitionKey partitionKey;
    private final EncryptedOutputFile encryptedOutputFile;
    private final FileAppender<T> fileAppender;


    WrappedFileAppender(PartitionKey partitionKey,
                        EncryptedOutputFile encryptedOutputFile,
                        FileAppender<T> fileAppender) {
      this.partitionKey = partitionKey;
      this.encryptedOutputFile = encryptedOutputFile;
      this.fileAppender = fileAppender;
    }
  }
}
