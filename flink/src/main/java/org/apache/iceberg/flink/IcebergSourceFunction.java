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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSourceFunction extends RichParallelSourceFunction<Row> implements
    CheckpointListener, ResultTypeQueryable<Row>, CheckpointedFunction {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceFunction.class);

  private final String tableLocation;
  private final SerializableConfiguration conf;

  /**
   * checkpointId:
   * 1. currentSnapshotId;
   * 2.    -> current data file index;
   * 3.    -> current data file offset.
   */


  private static final ListStateDescriptor<byte[]> ICEBERG_SOURCE_STATE = new ListStateDescriptor<>(
      "iceberg-source-state", BytePrimitiveArraySerializer.INSTANCE);
  private transient ListState<byte[]> globalStates;

  private transient Table table;

  public IcebergSourceFunction(String tableLocation, Configuration conf) {
    this.tableLocation = tableLocation;
    this.conf = new SerializableConfiguration(conf == null ? new Configuration() : conf);
    Iterable<DataFile> dataFile = table.currentSnapshot().addedFiles();
  }

  @Override
  public TypeInformation<Row> getProducedType() {
    return null;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {

  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {

  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {

  }

  @Override
  public void run(SourceContext<Row> ctx) throws Exception {

  }

  @Override
  public void cancel() {

  }

  private static class State {
    private long snapshotId;

  }
}
