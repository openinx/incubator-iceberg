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

package org.apache.iceberg.io;

import java.io.Closeable;
import java.io.IOException;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentFileWriter;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFileWriter;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.util.Tasks;

public abstract class BaseTaskWriter<T> implements TaskWriter<T> {
  private final TaskWriterResult.Builder builder;
  private final PartitionSpec spec;
  private final FileFormat format;
  private final FileAppenderFactory<T> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;

  protected BaseTaskWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                           OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
    this.builder = TaskWriterResult.builder();
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
  }

  @Override
  public void abort() throws IOException {
    close();

    // clean up files created by this writer
    TaskWriterResult result = builder.build();

    Tasks.foreach(result.dataFiles())
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));

    Tasks.foreach(result.deleteFiles())
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public TaskWriterResult complete() throws IOException {
    close();

    return builder.build();
  }

  protected class RollingDataFileWriter extends BaseRollingFileWriter<DataFile, T> {

    public RollingDataFileWriter(PartitionKey partitionKey) {
      super(partitionKey);
    }

    @Override
    ContentFileWriter<DataFile, T> newContentFileWriter(EncryptedOutputFile outputFile, FileFormat fileFormat) {
      FileAppender<T> appender = appenderFactory.newAppender(outputFile.encryptingOutputFile(), fileFormat);
      return new DataFileWriter<>(appender, fileFormat, outputFile.encryptingOutputFile().location(), partitionKey(),
          spec, outputFile.keyMetadata());
    }
  }

  protected abstract class BaseRollingFileWriter<F, R> implements Closeable {
    private static final int ROWS_DIVISOR = 1000;
    private final PartitionKey partitionKey;

    private EncryptedOutputFile currentFile = null;
    private ContentFileWriter<F, R> currentFileWriter = null;
    private long currentRows = 0;

    public BaseRollingFileWriter(PartitionKey partitionKey) {
      this.partitionKey = partitionKey;
      openCurrent();
    }

    protected PartitionKey partitionKey() {
      return partitionKey;
    }

    public void add(R record) throws IOException {
      this.currentFileWriter.write(record);
      this.currentRows++;

      if (shouldRollToNewFile()) {
        closeCurrent();
        openCurrent();
      }
    }

    abstract ContentFileWriter<F, R> newContentFileWriter(EncryptedOutputFile outputFile, FileFormat fileFormat);

    private void openCurrent() {
      if (partitionKey == null) {
        // unpartitioned
        currentFile = fileFactory.newOutputFile();
      } else {
        // partitioned
        currentFile = fileFactory.newOutputFile(partitionKey);
      }
      currentFileWriter = newContentFileWriter(currentFile, format);
      currentRows = 0;
    }

    private boolean shouldRollToNewFile() {
      // TODO: ORC file now not support target file size before closed
      return !format.equals(FileFormat.ORC) &&
          currentRows % ROWS_DIVISOR == 0 && currentFileWriter.length() >= targetFileSize;
    }

    @SuppressWarnings("unchecked")
    private void closeCurrent() throws IOException {
      if (currentFileWriter != null) {
        currentFileWriter.close();
        F contentFile = currentFileWriter.toContentFile();
        Metrics metrics = currentFileWriter.metrics();
        this.currentFileWriter = null;

        if (metrics.recordCount() == 0L) {
          io.deleteFile(currentFile.encryptingOutputFile());
        } else if (contentFile instanceof ContentFile) {
          builder.add((ContentFile) contentFile);
        } else {
          throw new RuntimeException(String.format(
              "The newly generated content file must be DataFile or DeleteFile: %s", contentFile));
        }

        this.currentFile = null;
        this.currentRows = 0;
      }
    }

    @Override
    public void close() throws IOException {
      closeCurrent();
    }
  }
}
