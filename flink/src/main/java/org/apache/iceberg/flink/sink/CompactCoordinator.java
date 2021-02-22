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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.stream.Collectors;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Predicates;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CompactCoordinator extends AbstractStreamOperator<CombinedScanTask>
    implements OneInputStreamOperator<WriteResult, CombinedScanTask>, BoundedOneInput {

  private static final Logger LOG = LoggerFactory.getLogger(CompactCoordinator.class);
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

  // TableLoader to load iceberg table lazily.
  private final TableLoader tableLoader;

  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();
  private final List<WriteResult> writeResultsOfCurrentCkpt = Lists.newArrayList();

  // Flink state to maintain the job id.
  private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR = buildJobIdDescriptor();
  private transient ListState<String> jobIdState;

  // Flink state to maintain all the un-compacted data files.
  private static final ListStateDescriptor<SortedMap<Long, byte[]>> STATE_DESCRIPTOR = buildStateDescriptor();
  private transient ListState<SortedMap<Long, byte[]>> checkpointsState;

  private transient String flinkJobId;
  private transient Table table;
  private transient long targetSizeInBytes;
  private transient int splitLookback;
  private transient long splitOpenFileCost;
  private transient String schemaString;
  private transient String specString;
  private transient ResidualEvaluator evaluator;
  private transient ManifestOutputFileFactory manifestOutputFileFactory;

  CompactCoordinator(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();

    // Open the table loader and create a new table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();
    this.targetSizeInBytes = parseTargetSizeInBytes(table.properties());
    this.splitLookback = parseSplitLookback(table.properties());
    this.splitOpenFileCost = parseSplitOpenFileCost(table.properties());
    this.schemaString = SchemaParser.toJson(table.schema());
    this.specString = PartitionSpecParser.toJson(table.spec());
    this.evaluator = ResidualEvaluator.of(table.spec(), Expressions.alwaysTrue(), false);

    int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    int attemptId = getRuntimeContext().getAttemptNumber();
    this.manifestOutputFileFactory = FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, subTaskId, attemptId);

    this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);
    this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);

    if (context.isRestored()) {
      String restoredFlinkJobId = jobIdState.get().iterator().next();
      Preconditions.checkState(!Strings.isNullOrEmpty(restoredFlinkJobId),
          "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");

      long maxCommittedCheckpointId = IcebergFilesCommitter.getMaxCommittedCheckpointId(table, restoredFlinkJobId);

      NavigableMap<Long, byte[]> compactedDataFiles = Maps
          .newTreeMap(checkpointsState.get().iterator().next())
          .tailMap(maxCommittedCheckpointId, false);

      if (!compactedDataFiles.isEmpty()) {
        expireCompactedDataFiles(compactedDataFiles, table.io());
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);

    long checkpointId = context.getCheckpointId();
    LOG.info("Start to flush snapshot state to state backend, table:{}, checkpointId:{}", table, checkpointId);

    // Add the data files from current checkpoint id.
    dataFilesPerCheckpoint.put(checkpointId, writeToManifest(checkpointId));

    // Reset the snapshot state to the latest state.
    checkpointsState.clear();
    checkpointsState.add(dataFilesPerCheckpoint);

    jobIdState.clear();
    jobIdState.add(flinkJobId);

    // Clear the local buffer for current checkpoint.
    writeResultsOfCurrentCkpt.clear();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);

    // All data files which have a smaller checkpoint id than this completed id have been compacted in RewriteFunction
    // and the results are persisted into flink state backend in IcebergFilesCommitter. So it's safe to expire those
    // data files from filesystem.
    NavigableMap<Long, byte[]> dataFilesToExpire = dataFilesPerCheckpoint.headMap(checkpointId, false);
    if (!dataFilesToExpire.isEmpty()) {
      expireCompactedDataFiles(dataFilesToExpire, table.io());
    }

    // Emit the data files that was collected from this completed checkpoint id to downstream RewriteFunction.
    DeltaManifests deltaManifests = deserializeDataFiles(dataFilesPerCheckpoint.get(checkpointId), table.io());
    if (deltaManifests == null) {
      return;
    }

    // Convert the data files to FileScanTasks.
    Iterable<FileScanTask> scanTasks = Arrays.stream(deltaManifests.writeResult(table.io()).dataFiles())
        .map(dataFile -> new BaseFileScanTask(dataFile, null, schemaString, specString, evaluator))
        .collect(Collectors.toList());

    List<CombinedScanTask> combinedScanTasks = groupTasksByPartition(scanTasks).values().stream()
        .map(tasks -> {
          CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(
              CloseableIterable.withNoopClose(tasks), targetSizeInBytes);
          return TableScanUtil.planTasks(splitTasks, targetSizeInBytes, splitLookback, splitOpenFileCost);
        })
        .flatMap(Streams::stream)
        .collect(Collectors.toList());

    for (CombinedScanTask task : combinedScanTasks) {
      output.collect(new StreamRecord<>(task));
    }
  }

  @Override
  public void processElement(StreamRecord<WriteResult> element) {
    writeResultsOfCurrentCkpt.add(element.getValue());
  }

  @Override
  public void endInput() throws Exception {
    // TODO
  }

  @Override
  public void dispose() throws Exception {
    super.dispose();
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

  /**
   * Write all the complete data files to a newly created manifest file and return the manifest's avro serialized bytes.
   */
  private byte[] writeToManifest(long checkpointId) throws IOException {
    if (writeResultsOfCurrentCkpt.isEmpty()) {
      return EMPTY_MANIFEST_DATA;
    }

    WriteResult result = WriteResult.builder().addAll(writeResultsOfCurrentCkpt).build();
    DeltaManifests deltaManifests = FlinkManifestUtil.writeCompletedFiles(result,
        () -> manifestOutputFileFactory.create(checkpointId), table.spec());

    return SimpleVersionedSerialization.writeVersionAndSerialize(DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  /**
   * Returns a map that are grouped the {@link FileScanTask} by partition key.
   */
  private Map<StructLikeWrapper, Collection<FileScanTask>> groupTasksByPartition(Iterable<FileScanTask> tasks) {
    ListMultimap<StructLikeWrapper, FileScanTask> tasksGroupedByPartition = Multimaps.newListMultimap(
        Maps.newHashMap(), Lists::newArrayList);

    Types.StructType partitionSpecType = table.spec().partitionType();
    for (FileScanTask task : tasks) {
      StructLikeWrapper structLike = StructLikeWrapper
          .forType(partitionSpecType)
          .set(task.file().partition());

      tasksGroupedByPartition.put(structLike, task);
    }

    return tasksGroupedByPartition.asMap();
  }

  private static long parseTargetSizeInBytes(Map<String, String> props) {
    long splitSize = PropertyUtil.propertyAsLong(
        props,
        TableProperties.SPLIT_SIZE,
        TableProperties.SPLIT_SIZE_DEFAULT);

    long targetFileSize = PropertyUtil.propertyAsLong(
        props,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    return Math.min(splitSize, targetFileSize);
  }

  private static int parseSplitLookback(Map<String, String> props) {
    return PropertyUtil.propertyAsInt(
        props,
        TableProperties.SPLIT_LOOKBACK,
        TableProperties.SPLIT_LOOKBACK_DEFAULT);
  }

  private static long parseSplitOpenFileCost(Map<String, String> props) {
    return PropertyUtil.propertyAsLong(
        props,
        TableProperties.SPLIT_OPEN_FILE_COST,
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
  }

  private static DeltaManifests deserializeDataFiles(byte[] data, FileIO io) {
    if (data == null || Arrays.equals(EMPTY_MANIFEST_DATA, data)) {
      return null;
    }

    try {
      DeltaManifests deltaManifests = SimpleVersionedSerialization
          .readVersionAndDeSerialize(DeltaManifestsSerializer.INSTANCE, data);
      WriteResult result = deltaManifests.writeResult(io);

      Preconditions.checkState(result.deleteFiles().length == 0, "Not support auto-compact for format v2 now.");
      return deltaManifests;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void expireCompactedDataFiles(NavigableMap<Long, byte[]> compactedDataFiles, FileIO io) {
    List<DeltaManifests> deltaManifestsToExpire = compactedDataFiles.values()
        .stream()
        .map(data -> deserializeDataFiles(data, io))
        .filter(Predicates.notNull())
        .collect(Collectors.toList());

    for (DeltaManifests deltaManifests : deltaManifestsToExpire) {
      WriteResult result;
      try {
        result = deltaManifests.writeResult(io);
      } catch (Exception e) {
        // It's possible that the manifests have been deleted.
        LOG.warn("Failed to read data files and delete files from flink manifest: {}", deltaManifests, e);
        continue;
      }

      Preconditions.checkState(result.deleteFiles().length == 0, "Not support auto-compact for format v2 now.");

      // Delete all data files from warehouse location.
      for (DataFile dataFile : result.dataFiles()) {
        try {
          io.deleteFile(dataFile.path().toString());
        } catch (Exception e) {
          LOG.warn("Failed to delete the expired data file: {}", dataFile.path(), e);
        }
      }

      Preconditions.checkState(deltaManifests.deleteManifest() == null, "Not support auto-compact for format v2 now.");

      // Delete flink manifests from warehouse location.
      if (deltaManifests.dataManifest() != null) {
        try {
          io.deleteFile(deltaManifests.dataManifest().path());
        } catch (Exception e) {
          LOG.warn("Failed to delete the expired flink manifest file: {}", deltaManifests.dataManifest().path());
        }
      }
    }

    // Remove those files from local buffer.
    compactedDataFiles.clear();
  }

  private static ListStateDescriptor<String> buildJobIdDescriptor() {
    return new ListStateDescriptor<>(
        "Iceberg-compact-coordinator-jobId-state", BasicTypeInfo.STRING_TYPE_INFO
    );
  }

  private static ListStateDescriptor<SortedMap<Long, byte[]>> buildStateDescriptor() {
    // Construct a SortedMapTypeInfo.
    SortedMapTypeInfo<Long, byte[]> sortedMapTypeInfo = new SortedMapTypeInfo<>(
        BasicTypeInfo.LONG_TYPE_INFO,
        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
        Comparators.forType(Types.LongType.get())
    );
    return new ListStateDescriptor<>("Iceberg-compact-coordinator-state", sortedMapTypeInfo);
  }
}
