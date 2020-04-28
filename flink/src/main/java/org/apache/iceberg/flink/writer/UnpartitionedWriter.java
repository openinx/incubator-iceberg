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
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileAppender;

public class UnpartitionedWriter<T> implements TaskWriter<T> {

  private final FileAppenderFactory<T> factory;
  private final Supplier<EncryptedOutputFile> outputFileSupplier;
  private final long targetFileSize;
  private final FileFormat fileFormat;
  private final List<DataFile> completeDataFiles;

  private EncryptedOutputFile currentOutputFile;
  private FileAppender<T> currentWriter = null;

  public UnpartitionedWriter(FileAppenderFactory<T> factory,
                             Supplier<EncryptedOutputFile> outputFileSupplier,
                             long targetFileSize,
                             FileFormat fileFormat) {
    this.factory = factory;
    this.outputFileSupplier = outputFileSupplier;
    this.targetFileSize = targetFileSize;
    this.fileFormat = fileFormat;
    this.completeDataFiles = new ArrayList<>();
  }

  @Override
  public void write(T record) throws IOException {
    if (currentWriter == null) {
      currentOutputFile = outputFileSupplier.get();
      currentWriter = factory.newAppender(currentOutputFile.encryptingOutputFile(), fileFormat);
    }
    currentWriter.add(record);

    // Roll the writer if reach the target file size.
    if (currentWriter.length() >= targetFileSize) {
      closeCurrentWriter();
    }
  }

  private void closeCurrentWriter() throws IOException {
    if (currentWriter != null) {
      currentWriter.close();

      // Construct the DataFile and add it into the completeDataFiles.
      DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
          .withEncryptedOutputFile(currentOutputFile)
          .withPartition(null)
          .withMetrics(currentWriter.metrics())
          .withSplitOffsets(currentWriter.splitOffsets())
          .build();
      completeDataFiles.add(dataFile);

      // Reset the current output file and writer to be null.
      currentWriter = null;
      currentOutputFile = null;
    }
  }

  @Override
  public void close() throws IOException {
    closeCurrentWriter();
  }

  @Override
  public List<DataFile> getCompleteFiles() {
    return this.completeDataFiles;
  }
}
