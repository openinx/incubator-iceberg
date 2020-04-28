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

import java.util.UUID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.PartitionKey;
import org.apache.iceberg.io.OutputFile;

public class OutputFileFactory {
  private final String uuid = UUID.randomUUID().toString();

  private final Table table;
  private final FileFormat format;
  private final long taskId;
  private int fileCount;

  public OutputFileFactory(Table table, FileFormat format, long taskId) {
    this.table = table;
    this.format = format;
    this.taskId = taskId;
    this.fileCount = 0;
  }

  /**
   * All the data files inside the same task will share the same uuid identifier but could be distinguished by the
   * increasing file count.
   *
   * @return the data file name to be written.
   */
  private String generateFilename() {
    int hashCode = Math.abs(this.hashCode() % 10 ^ 5);
    return format.addExtension(String.format("%05d-%d-%s-%05d", hashCode, taskId, uuid, fileCount++));
  }

  /**
   * Output file path for unpartitioned table.
   *
   * @return data file path to write the record.
   */
  EncryptedOutputFile newOutputFile() {
    OutputFile file = table.io().newOutputFile(table.locationProvider().newDataLocation(generateFilename()));
    return table.encryption().encrypt(file);
  }

  /**
   * Output file path for partition table.
   *
   * @param key indicate the partition key.
   * @return data file path to write the record.
   */
  EncryptedOutputFile newOutputFile(PartitionKey key) {
    OutputFile file = table.io().newOutputFile(table.locationProvider()
        .newDataLocation(table.spec(), key, generateFilename()));
    return table.encryption().encrypt(file);
  }
}
