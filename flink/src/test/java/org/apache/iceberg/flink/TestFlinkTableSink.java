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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.flink.table.api.Expressions.$;

@RunWith(Parameterized.class)
public class TestFlinkTableSink extends AbstractTestBase {

  private static final String TABLE_NAME = "destinationTable";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String warehouse;
  private String tablePath;

  private final boolean useOldPlanner;
  private final int parallelism;

  @Parameterized.Parameters(name = "{index}: useOldPlanner={0}, parallelism={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[] {true, 1},
        new Object[] {false, 1},
        new Object[] {true, 2},
        new Object[] {false, 2}
    );
  }

  public TestFlinkTableSink(boolean useOldPlanner, int parallelism) {
    this.useOldPlanner = useOldPlanner;
    this.parallelism = parallelism;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    warehouse = folder.getAbsolutePath();

    tablePath = warehouse.concat(TABLE_NAME);
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdir());

    SimpleDataUtil.createTable(tablePath, ImmutableMap.of(), true);
  }

  @Test
  public void testSQL() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(400);
    env.setParallelism(parallelism);

    EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance().inStreamingMode();
    if (useOldPlanner) {
      settingsBuilder.useBlinkPlanner();
    }
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());

    String[] worlds = new String[] {"hello", "world", "foo", "bar", "apache", "foundation"};
    List<Row> rows = Lists.newArrayList();
    for (int i = 0; i < worlds.length; i++) {
      rows.add(Row.of(i + 1, worlds[i]));
    }
    DataStream<Row> dataStream = env.addSource(new FiniteTestSource<>(rows), SimpleDataUtil.FLINK_SCHEMA.toRowType());

    // Register the rows into a temporary table named 'sourceTable'.
    tEnv.createTemporaryView("sourceTable", tEnv.fromDataStream(dataStream, $("id"), $("data")));

    // Create another table named 'destinationTable'
    String ddl = String.format("CREATE TABLE %s(" +
            "id   int, " +
            "data string) " +
            "WITH (" +
            "'connector.type'='iceberg', " +
            "'connector.version'='1.10.0', " +
            "'connector.iceberg-table.name'='%s', " +
            "'connector.iceberg.catalog-type'='hadoop', " +
            "'connector.iceberg.hadoop-warehouse'='file://%s')",
        TABLE_NAME, TABLE_NAME, warehouse);
    tEnv.executeSql(ddl);

    // Redirect the records from 'words' table to 'destinationTable'
    tEnv.executeSql("INSERT INTO destinationTable SELECT id,data from sourceTable");

    // Submit the flink job.
    env.execute();

    // Assert the table records as expected.
    List<Record> expected = Lists.newArrayList();
    // There will be two checkpoints in FiniteTestSource, so it will produce a list with double records.
    for (int i = 0; i < 2; i++) {
      for (int k = 0; k < worlds.length; k++) {
        expected.add(SimpleDataUtil.createRecord(k + 1, "word"));
      }
    }
    SimpleDataUtil.assertTableRecords(tablePath, expected);
  }
}
