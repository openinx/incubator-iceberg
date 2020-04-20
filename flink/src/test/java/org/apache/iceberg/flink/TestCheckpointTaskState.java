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

import com.google.common.collect.Lists;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.OutputFile;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCheckpointTaskState {

  private static final Configuration CONF = new Configuration();

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  private DataFile createTestDataFile(String fileName) throws IOException {
    HadoopFileIO hadoopFileIO = new HadoopFileIO(CONF);
    String absolutePath = temp.newFile(fileName).toPath().toAbsolutePath().toString();
    OutputFile outputFile = hadoopFileIO.newOutputFile(absolutePath);
    EncryptedOutputFile encryptOut = EncryptedFiles.encryptedOutput(outputFile, new byte[0]);
    return DataFiles.builder(SPEC)
        .withEncryptedOutputFile(encryptOut)
        .withPartition(null)
        .withFormat(FileFormat.PARQUET)
        .withRecordCount(0)
        .withSplitOffsets(Lists.newArrayList(100L))
        .build();
  }

  @Test
  public void testSerializer() throws IOException {
    DataFile dataFile0 = createTestDataFile("test0.txt");
    DataFile dataFile1 = createTestDataFile("test1.txt");
    DataFile dataFile2 = createTestDataFile("test2.txt");

    CheckpointTaskState state = new CheckpointTaskState(101L,
        Lists.newArrayList(dataFile0, dataFile1, dataFile2));

    CheckpointTaskState.StateSerializer serializer = new CheckpointTaskState.StateSerializer(SPEC.partitionType());
    byte[] data = serializer.serialize(state);

    CheckpointTaskState actualState = serializer.deserialize(1, data);
    byte[] actualData = serializer.serialize(actualState);
    Assert.assertEquals(state.getCheckpointId(), actualState.getCheckpointId());
    Assert.assertEquals(state.getDataFiles(), actualState.getDataFiles());
    Assert.assertArrayEquals(data, actualData);
  }
}
