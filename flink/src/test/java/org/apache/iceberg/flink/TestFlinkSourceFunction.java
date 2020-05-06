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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.CollectingSourceContext;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFlinkSourceFunction {

  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String tableLocation;
  private FileFormat fileFormat = FileFormat.PARQUET;

  @Before
  public void before() throws IOException {
    tableLocation = tempFolder.newFolder().getAbsolutePath();
    Table table = WordCountData.createTable(tableLocation,
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name()),
        false);

    // Write few records and assert them.
    List<Row> records1 = Arrays.asList(Row.of("hello", 1), Row.of("word", 1));
    List<Row> records2 = Arrays.asList(Row.of("hello", 2), Row.of("word", 2));

    DataFile file1 = writeRecords(records1, new Path(tableLocation, fileFormat.addExtension("file1")));
    table.newAppend().appendFile(file1).commit();

    DataFile file2 = writeRecords(records2, new Path(tableLocation, fileFormat.addExtension("file2")));
    table.newAppend().appendFile(file2).commit();

    TestUtility.checkIcebergTableRecords(tableLocation, Lists.newArrayList(
        WordCountData.createRecord("hello", 1),
        WordCountData.createRecord("word", 1),
        WordCountData.createRecord("hello", 2),
        WordCountData.createRecord("word", 2)),
        WordCountData.RECORD_COMPARATOR);
  }

  private DataFile writeRecords(Collection<Row> rows,
                                Path path) throws IOException {
    FileAppender<Row> parquetAppender = Parquet.write(fromPath(path, CONF))
        .schema(WordCountData.SCHEMA)
        .createWriterFunc(FlinkParquetWriters::buildWriter)
        .build();
    try {
      parquetAppender.addAll(rows);
    } finally {
      parquetAppender.close();
    }
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withInputFile(HadoopInputFile.fromPath(path, CONF))
        .withMetrics(parquetAppender.metrics())
        .build();
  }

  @Test
  public void testSource() throws Exception {
    IcebergSourceFunction source = CommonTestUtils.createCopySerializable(
        new IcebergSourceFunction(tableLocation, CONF));
    List<Row> expectList = ImmutableList.of(
        Row.of("hello", 1),
        Row.of("word", 1),
        Row.of("hello", 2),
        Row.of("word", 2)
    );
    Timer timer = new Timer(true);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        source.cancel();
      }
    }, 10000L);
    List<Row> actualList = runRichSourceFunction(source);
    Assert.assertEquals(expectList, actualList);
  }

  private static List<Row> runRichSourceFunction(IcebergSourceFunction sourceFunction)
      throws Exception {
    List<Row> outputs = Lists.newArrayList();
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
      sourceFunction.setRuntimeContext(runtimeContext);
      sourceFunction.initializeState(null);
      sourceFunction.open(new org.apache.flink.configuration.Configuration());
      try {
        SourceFunction.SourceContext<Row> ctx = new CollectingSourceContext<>(new Object(), outputs);
        sourceFunction.run(ctx);
      } catch (Exception e) {
        throw new RuntimeException("Cannot invoke source.", e);
      }
      return outputs;
    }
  }
}
