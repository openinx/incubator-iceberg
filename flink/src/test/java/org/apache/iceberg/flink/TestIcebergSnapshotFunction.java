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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.CollectingSourceContext;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.reader.RowReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestIcebergSnapshotFunction {

  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String tableLocation;
  private Table table;
  private FileFormat fileFormat = FileFormat.PARQUET;

  @Before
  public void before() throws IOException {
    tableLocation = tempFolder.newFolder().getAbsolutePath();
    table = WordCountData.createTable(tableLocation,
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name()),
        false);

    // Write few records and assert them.
    for (int i = 0; i < 2; i++) {
      List<Row> records = Arrays.asList(Row.of("hello", i + 1), Row.of("word", i + 1));
      DataFile file = TestUtility.writeRecords(records, WordCountData.SCHEMA,
          new Path(tableLocation, fileFormat.addExtension("file" + (i + 1))));
      table.newAppend().appendFile(file).commit();
    }

    TestUtility.checkIcebergTableRecords(tableLocation, Lists.newArrayList(
        WordCountData.createRecord("hello", 1),
        WordCountData.createRecord("word", 1),
        WordCountData.createRecord("hello", 2),
        WordCountData.createRecord("word", 2)),
        WordCountData.RECORD_COMPARATOR);
  }

  @Test
  public void testSource() throws Exception {
    IcebergSnapshotFunction source = CommonTestUtils.createCopySerializable(
        new IcebergSnapshotFunction(tableLocation, CONF, IcebergSource.NON_CONSUMED_SNAPSHOT_ID, 1000L, 2));
    Set<Row> expectRows = Sets.newHashSet(
        Row.of("hello", 1),
        Row.of("word", 1),
        Row.of("hello", 2),
        Row.of("word", 2)
    );
    List<CombinedScanTask> scanTasks = runRichSourceFunction(source);
    Set<Row> actualRows = Sets.newHashSet();
    for (CombinedScanTask scanTask : scanTasks) {
      actualRows.addAll(readRows(scanTask));
    }
    Assert.assertEquals(expectRows, actualRows);
  }

  private List<Row> readRows(CombinedScanTask scanTask) throws IOException {
    try (RowReader reader = new RowReader(scanTask, table.io(), table.schema(), table.encryption(), true)) {
      List<Row> rows = Lists.newArrayList();
      while (reader.hasNext()) {
        rows.add(reader.next());
      }
      return rows;
    }
  }

  private static List<CombinedScanTask> runRichSourceFunction(IcebergSnapshotFunction function)
      throws Exception {
    List<CombinedScanTask> outputs = Lists.newArrayList();
    try (MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("MockTask")
        .setInputSplitProvider(new MockInputSplitProvider())
        .setBufferSize(1024)
        .build()) {
      AbstractStreamOperator<?> operator = mock(AbstractStreamOperator.class);
      when(operator.getExecutionConfig()).thenReturn(new ExecutionConfig());
      when(operator.getOperatorID()).thenReturn(new OperatorID());

      RuntimeContext runtimeContext = new StreamingRuntimeContext(
          operator,
          environment,
          new HashMap<>());
      function.setRuntimeContext(runtimeContext);
      FunctionInitializationContext context = mock(FunctionInitializationContext.class);
      OperatorStateStore operatorState = mock(OperatorStateStore.class);
      ListState<Long> listState = mock(ListState.class);
      when(operatorState.getListState(IcebergSnapshotFunction.LAST_CONSUMED_SNAPSHOT_STATE)).thenReturn(listState);
      when(context.getOperatorStateStore()).thenReturn(operatorState);
      when(context.isRestored()).thenReturn(false);
      function.initializeState(context);
      function.open(new org.apache.flink.configuration.Configuration());
      try {
        SourceFunction.SourceContext<CombinedScanTask> ctx = new CollectingSourceContext<>(new Object(), outputs);
        function.run(ctx);
      } catch (Exception e) {
        throw new RuntimeException("Cannot invoke source.", e);
      }
      return outputs;
    }
  }
}
