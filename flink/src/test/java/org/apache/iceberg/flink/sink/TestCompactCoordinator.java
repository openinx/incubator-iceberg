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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;

@RunWith(Parameterized.class)
public class TestCompactCoordinator extends TableTestBase {
  private static final Configuration CONF = new Configuration();

  private String tablePath;
  private File flinkManifestFolder;

  private final FileFormat format;

  @Parameterized.Parameters(name = "FileFormat = {0}, FormatVersion={1}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"avro", 1},
        new Object[] {"avro", 2},
        new Object[] {"parquet", 1},
        new Object[] {"parquet", 2},
        new Object[] {"orc", 1},
    };
  }

  public TestCompactCoordinator(String format, int formatVersion) {
    super(formatVersion);
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void setupTable() throws IOException {
    flinkManifestFolder = temp.newFolder();

    this.tableDir = temp.newFolder();
    this.metadataDir = new File(tableDir, "metadata");
    Assert.assertTrue(tableDir.delete());

    tablePath = tableDir.getAbsolutePath();

    // Construct the iceberg table.
    table = create(SimpleDataUtil.SCHEMA, PartitionSpec.unpartitioned());

    table.updateProperties()
        .set(DEFAULT_FILE_FORMAT, format.name())
        .set(FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath())
        .commit();
  }

  @Test
  public void testRestoreState() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;

    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, CombinedScanTask> harness = createOperator(jobID)) {
      harness.setup();
      harness.open();

      harness.processElement(of(null), ++timestamp);

      harness.notifyOfCompletedCheckpoint(checkpointId);
    }
  }

  @Test
  public void testPlanTasks() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;

    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, CombinedScanTask> harness = createOperator(jobID)) {
      harness.setup();
      harness.open();

      RowData row1 = SimpleDataUtil.createRowData(1, "AAAA");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      harness.processElement(of(dataFile1), ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);

      List<CombinedScanTask> tasks = harness.extractOutputValues();
      Assert.assertEquals(1, tasks.size());
      CombinedScanTask task = tasks.get(0);
      Assert.assertEquals(1, task.files().size());
      Assert.assertEquals(dataFile1.path(), task.files().stream().findFirst().get().file().path());
    }
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(table.schema(), table.spec(), CONF, tablePath, format.addExtension(filename), rows);
  }

  private static WriteResult of(DataFile dataFile) {
    return WriteResult.builder().addDataFiles(dataFile).build();
  }

  private OneInputStreamOperatorTestHarness<WriteResult, CombinedScanTask> createOperator(JobID jobID)
      throws Exception {
    TestOperatorFactory factory = TestOperatorFactory.of(tablePath);
    return new OneInputStreamOperatorTestHarness<>(factory, createEnvironment(jobID));
  }

  private static MockEnvironment createEnvironment(JobID jobID) {
    return new MockEnvironmentBuilder()
        .setTaskName("test task")
        .setManagedMemorySize(32 * 1024)
        .setInputSplitProvider(new MockInputSplitProvider())
        .setBufferSize(256)
        .setTaskConfiguration(new org.apache.flink.configuration.Configuration())
        .setExecutionConfig(new ExecutionConfig())
        .setMaxParallelism(16)
        .setJobID(jobID)
        .build();
  }

  private static class TestOperatorFactory extends AbstractStreamOperatorFactory<CombinedScanTask>
      implements OneInputStreamOperatorFactory<WriteResult, CombinedScanTask> {
    private final String tablePath;

    private TestOperatorFactory(String tablePath) {
      this.tablePath = tablePath;
    }

    private static TestOperatorFactory of(String tablePath) {
      return new TestOperatorFactory(tablePath);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<CombinedScanTask>> T createStreamOperator(
        StreamOperatorParameters<CombinedScanTask> param) {
      CompactCoordinator coordinator = new CompactCoordinator(TestTableLoader.of(tablePath));
      coordinator.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) coordinator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return CompactCoordinator.class;
    }
  }
}
