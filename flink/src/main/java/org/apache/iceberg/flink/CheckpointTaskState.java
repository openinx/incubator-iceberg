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
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.types.Types;

public class CheckpointTaskState {
  private final long checkpointId;
  private final List<DataFile> dataFiles;

  public CheckpointTaskState(long checkpointId, List<DataFile> newDataFiles) {
    this.checkpointId = checkpointId;
    this.dataFiles = newDataFiles;
  }

  public long getCheckpointId() {
    return this.checkpointId;
  }

  public List<DataFile> getDataFiles() {
    return this.dataFiles;
  }

  static StateSerializer createSerializer(Types.StructType partitionType) {
    return new StateSerializer(partitionType);
  }

  static class StateSerializer implements SimpleVersionedSerializer<CheckpointTaskState> {
    private static final int MAGIC_NUMBER = 0x1e77ab70;

    private final Types.StructType partitionType;

    StateSerializer(Types.StructType partitionType) {
      this.partitionType = partitionType;
    }

    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(CheckpointTaskState state) throws IOException {
      DataOutputSerializer out = new DataOutputSerializer(256);
      out.writeInt(MAGIC_NUMBER);

      // Serialize the checkpointId
      out.writeLong(state.checkpointId);

      // Serialize the data files.
      writeDataFileList(out, state.dataFiles);

      return out.getCopyOfBuffer();
    }

    private void writeDataFileList(DataOutputSerializer out, List<DataFile> dataFiles) throws IOException {
      out.writeInt(dataFiles.size());
      for (DataFile dataFile : dataFiles) {
        byte[] bytes = dataFile.serialize();
        out.writeInt(bytes.length);
        out.write(bytes);
      }
    }

    private List<DataFile> readDataFileList(DataInputDeserializer in) throws IOException {
      int size = in.readInt();
      List<DataFile> dataFiles = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        int bytesLen = in.readInt();
        byte[] bytes = new byte[bytesLen];
        Preconditions.checkArgument(bytesLen == in.read(bytes));
        DataFile dataFile = DataFiles.fromBinary(partitionType, bytes, 0, bytes.length);
        dataFiles.add(dataFile);
      }
      return dataFiles;
    }

    @Override
    public CheckpointTaskState deserialize(int version, byte[] serialized) throws IOException {
      switch (version) {
        case 1:
          DataInputDeserializer in = new DataInputDeserializer(serialized);
          Preconditions.checkArgument(MAGIC_NUMBER == in.readInt(), "Magic number mismatched.");

          // Deserialize the checkpointId
          long checkpointId = in.readLong();

          // Deserialize the dataFiles.
          List<DataFile> dataFiles = readDataFileList(in);

          // construct the iceberg state
          return new CheckpointTaskState(checkpointId, dataFiles);
        default:
          throw new IOException("unrecognized version or corrupt state: " + version);
      }
    }
  }
}
