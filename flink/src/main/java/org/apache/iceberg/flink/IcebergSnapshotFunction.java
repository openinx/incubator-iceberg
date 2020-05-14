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
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSnapshotFunction extends RichSourceFunction<CombinedScanTask> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSnapshotFunction.class);
  private static final long serialVersionUID = 1L;
  static final long NON_CONSUMED_SNAPSHOT_ID = -1L;

  static final ListStateDescriptor<Long> LAST_CONSUMED_SNAPSHOT_STATE = new ListStateDescriptor<>(
      "last-consumed-snapshot-state", LongSerializer.INSTANCE);

  /**
   * The interval between consecutive snapshot fetching
   */
  private final long intervalMillis;

  private final String tableLocation;
  private final SerializableConfiguration conf;
  private final long fromSnapshotId;
  private long remainingSnapshots = Long.MAX_VALUE;

  private long lastConsumedSnapId = IcebergSource.NON_CONSUMED_SNAPSHOT_ID;

  private transient Table table;
  private volatile boolean running = true;

  private transient ListState<Long> consumedSnapState;

  public IcebergSnapshotFunction(String tableLocation,
                                 Configuration conf,
                                 long fromSnapshotId,
                                 long intervalMillis,
                                 long remainingSnapshots) {
    this.tableLocation = tableLocation;
    this.conf = new SerializableConfiguration(conf);
    this.fromSnapshotId = fromSnapshotId;
    this.intervalMillis = intervalMillis;
    this.remainingSnapshots = remainingSnapshots;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    this.table = new HadoopTables(conf.get()).load(tableLocation);
    this.consumedSnapState = context.getOperatorStateStore().getListState(LAST_CONSUMED_SNAPSHOT_STATE);
    // When staring the streaming job first time, we could specific a from-snapshot-id to start consuming. It only works
    // in the first job starting, the following restored job would use the last-consumed-snapshot-id maintained in
    // state backend.
    this.lastConsumedSnapId = fromSnapshotId;
    if (context.isRestored()) {
      LOG.info("Restoring state for the {}.", getClass().getSimpleName());
      this.lastConsumedSnapId = this.consumedSnapState.get().iterator().next();
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    this.consumedSnapState.clear();
    this.consumedSnapState.add(this.lastConsumedSnapId);

    LOG.info("{} checkpoint with last consumed snapshot id = {}.", getClass().getSimpleName(), lastConsumedSnapId);
  }

  @Override
  public void run(SourceContext<CombinedScanTask> ctx) throws Exception {
    IncrementalSnapshotFetcher incrementalSnapshotFetcher = new IncrementalSnapshotFetcher(table, lastConsumedSnapId);
    while (running) {
      synchronized (ctx.getCheckpointLock()) {
        // Here we will transform many scan tasks to the downstream operator, it's possible that the job crashed when
        // we collect partial of them. Once restored the task then the downstream operator will receive the duplicated
        // scan task, so the downstream should handle the duplicated task.
        for (CombinedScanTask task : incrementalSnapshotFetcher.consumeNextSnap()) {
          ctx.collect(task);
        }
        // Refresh the last consumed snapshot id to the latest one.
        this.lastConsumedSnapId = incrementalSnapshotFetcher.getLastConsumedSnapshotId();
      }
      if (--remainingSnapshots < 0L) {
        break;
      } else {
        Thread.sleep(intervalMillis);
      }
    }
  }

  @Override
  public void cancel() {
    LOG.info("Cancel the IcebergSnapshotFunction, it will stop to fetch the incremental snapshots.");
    this.running = false;
  }
}
