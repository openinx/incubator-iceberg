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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.TableException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GlobalTableCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(GlobalTableCommitter.class);

  public static final String FLINK_MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint.id";

  private final GlobalAggregateManager aggregateManager;
  private final GlobalCommitFunction commitFunction;
  private final int taskId;

  GlobalTableCommitter(StreamingRuntimeContext context, String tableIdentifier, SerializableConfiguration hadoopConf) {
    this.aggregateManager = context.getGlobalAggregateManager();
    this.taskId = context.getIndexOfThisSubtask();
    this.commitFunction = new GlobalCommitFunction(
        context.getNumberOfParallelSubtasks(),
        tableIdentifier,
        hadoopConf
    );
  }

  long commit(NavigableMap<Long, List<DataFile>> pendingDataFiles) throws Exception {
    return aggregateManager.updateGlobalAggregate(
        "commit",
        new CommitAggregateValue(taskId, pendingDataFiles),
        commitFunction
    );
  }

  static long getMaxCommittedCheckpointId(String tableIdentifier, Configuration conf) {
    HadoopTables tables = new HadoopTables(conf);
    Table table = tables.load(tableIdentifier);
    if (table.currentSnapshot() != null) {
      String value = table.currentSnapshot().summary().get(FLINK_MAX_COMMITTED_CHECKPOINT_ID);
      return value == null ? -1L : Long.parseLong(value);
    } else {
      return -1L;
    }
  }

  private static class GlobalCommitFunction implements
      AggregateFunction<CommitAggregateValue, GlobalJobState, Long> {

    private final int numberOfTasks;
    private final String tableIdentifier;
    private SerializableConfiguration hadoopConf;

    GlobalCommitFunction(int numberOfTasks, String tableIdentifier, SerializableConfiguration hadoopConf) {
      this.numberOfTasks = numberOfTasks;
      this.tableIdentifier = tableIdentifier;
      this.hadoopConf = hadoopConf;
    }

    @Override
    public GlobalJobState createAccumulator() {
      return new GlobalJobState(getMaxCommittedCheckpointId(tableIdentifier, hadoopConf.get()));
    }

    @Override
    public GlobalJobState add(CommitAggregateValue value, GlobalJobState globalJobState) {
      NavigableMap<Long, CpAccumulator> accumulator = globalJobState.accumulator;
      for (Map.Entry<Long, List<DataFile>> entry : value.pendingDataFiles.entrySet()) {
        long checkpointId = entry.getKey();
        if (checkpointId > globalJobState.maxCommittedCheckpointId) {
          accumulator.compute(checkpointId, (cpId, cpAcc) -> {
            cpAcc = cpAcc == null ? new CpAccumulator() : cpAcc;
            cpAcc.add(value.taskId, entry.getValue());
            return cpAcc;
          });
        }
      }
      return globalJobState;
    }

    private void commitUpToTable(long ckpId, Collection<DataFile> dataFiles) {
      if (dataFiles.size() == 0) {
        LOG.info("Skip to commit table: {}, checkpointId: {} because there's no data file to commit now",
            tableIdentifier, ckpId);
        return;
      }
      LOG.info("Committing to iceberg table: {}, the max checkpoint id: {}", tableIdentifier, ckpId);
      // TODO support hive tables ??? distributed HDFS ???
      HadoopTables tables = new HadoopTables(hadoopConf.get());
      Table icebergTable = tables.load(this.tableIdentifier);
      AppendFiles appendFiles = icebergTable.newAppend();
      // Attach the MAX committed checkpoint id to the Iceberg table's properties.
      appendFiles.set(FLINK_MAX_COMMITTED_CHECKPOINT_ID, Long.toString(ckpId));
      dataFiles.forEach(appendFiles::appendFile);
      appendFiles.commit();
      LOG.info("Finished to commit iceberg table with the checkpoint id: {}, data file size: {}.",
          ckpId, dataFiles.size());
    }

    @Override
    public Long getResult(GlobalJobState globalJobState) {
      Optional<Long> allTasksFinishedMaxCkpId = globalJobState.accumulator
          .descendingMap()
          .entrySet()
          .stream()
          .filter(entry -> entry.getValue().taskIds.size() == numberOfTasks)
          .findFirst()
          .map(Map.Entry::getKey);
      Long commitCpId;
      if (allTasksFinishedMaxCkpId.isPresent()) {
        commitCpId = allTasksFinishedMaxCkpId.get();
        NavigableMap<Long, CpAccumulator> acc = globalJobState.accumulator.headMap(commitCpId, true);
        List<DataFile> filesToCommit = Lists.newArrayList();
        acc.values()
            .stream()
            .map(CpAccumulator::getPendingDataFiles)
            .forEach(filesToCommit::addAll);
        // Commit up the chosen checkpoint id.
        try {
          commitUpToTable(commitCpId, filesToCommit);
          acc.clear();
        } catch (Exception e) {
          throw new TableException("Failed to commit to iceberg table " +
              tableIdentifier + " for checkpointId " + commitCpId, e);
        }
      } else {
        commitCpId = getMaxCommittedCheckpointId(tableIdentifier, hadoopConf.get());
      }
      globalJobState.maxCommittedCheckpointId = commitCpId;
      // The max committed checkpoint id will be used for clearing the complete files cache for each task.
      return globalJobState.maxCommittedCheckpointId;
    }

    @Override
    public GlobalJobState merge(GlobalJobState globalJobState,
                                GlobalJobState b) {
      b.accumulator.forEach((cpId, acc) -> {
        if (cpId > globalJobState.maxCommittedCheckpointId) {
          globalJobState.accumulator.compute(cpId, (key, preAcc) -> {
            preAcc = preAcc == null ? new CpAccumulator() : preAcc;
            preAcc.merge(acc);
            return preAcc;
          });
        }
      });
      return globalJobState;
    }
  }

  /**
   * GlobalJobState is the state of a running aggregation, this state maintains
   * in the jobManager side, and contains pending files from all subTasks.
   */
  private static class GlobalJobState implements Serializable {
    // the max committed checkpoint id in iceberg table.
    private long maxCommittedCheckpointId;
    // this map keys mean checkpoint id.
    private final NavigableMap<Long, CpAccumulator> accumulator;

    GlobalJobState(long maxCheckpointId) {
      accumulator = new TreeMap<>();
      maxCommittedCheckpointId = maxCheckpointId;
    }
  }

  /**
   * The accumulator for a given checkpoint.
   */
  private static class CpAccumulator implements Serializable {

    private Set<Integer> taskIds = new HashSet<>();
    private Set<DataFile> pendingDataFiles = new HashSet<>();

    void add(int taskId, List<DataFile> dataFiles) {
      this.taskIds.add(taskId);
      this.pendingDataFiles.addAll(dataFiles);
    }

    void merge(CpAccumulator acc) {
      this.taskIds.addAll(acc.taskIds);
      this.pendingDataFiles.addAll(acc.pendingDataFiles);
    }

    Collection<DataFile> getPendingDataFiles() {
      return this.pendingDataFiles;
    }
  }

  private static class CommitAggregateValue implements Serializable {

    private final int taskId;
    private final NavigableMap<Long, List<DataFile>> pendingDataFiles;

    CommitAggregateValue(int taskId, NavigableMap<Long, List<DataFile>> pendingDataFiles) {
      this.taskId = taskId;
      this.pendingDataFiles = pendingDataFiles;
    }
  }
}
