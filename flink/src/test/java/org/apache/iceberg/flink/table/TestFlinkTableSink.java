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

package org.apache.iceberg.flink.table;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestUtility;
import org.apache.iceberg.flink.WordCountData;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkTableSink extends AbstractTestBase {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String tableLocation;

  @Parameterized.Parameter
  public boolean useOldPlanner;

  @Parameterized.Parameters(name = "{index}: useOldPlanner={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[]{true}, new Object[]{false});
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    tableLocation = folder.getAbsolutePath();
    WordCountData.createTable(tableLocation, true);
  }

  private void testSQL(int parallelism, boolean useDDL) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(400);
    env.setParallelism(parallelism);
    StreamTableEnvironment tEnv;
    if (useOldPlanner) {
      tEnv = StreamTableEnvironment.create(env);
    } else {
      EnvironmentSettings settings = EnvironmentSettings.newInstance()
          .useBlinkPlanner()
          .inStreamingMode()
          .build();
      tEnv = StreamTableEnvironment.create(env, settings);
    }

    String[] worlds = new String[]{"hello", "world", "foo", "bar", "apache", "foundation"};
    List<Row> rows = Lists.newArrayList();
    for (int i = 0; i < worlds.length; i++) {
      rows.add(Row.of(worlds[i], i + 1));
    }
    DataStream<Row> dataStream = env.addSource(new FiniteTestSource<>(rows), WordCountData.FLINK_SCHEMA.toRowType());

    tEnv.createTemporaryView("words", tEnv.fromDataStream(dataStream, "word,num"));

    if (useDDL) {
      String ddl = String.format(
          "CREATE TABLE IcebergTable(" +
              "word string, " +
              "num int) " +
              "WITH (" +
              "'connector.type'='iceberg', " +
              "'connector.version'='0.8.0', " +
              "'connector.iceberg-table.identifier'='%s'," +
              "'update-mode'='upsert')", tableLocation);
      tEnv.sqlUpdate(ddl);
    } else {
      // Use connector descriptor to create the iceberg table.
      tEnv.connect(Iceberg.newInstance()
          .withVersion(IcebergValidator.CONNECTOR_VERSION_VALUE)
          .withTableIdentifier(tableLocation))
          .withSchema(new Schema().schema(WordCountData.FLINK_SCHEMA))
          .inUpsertMode()
          .createTemporaryTable("IcebergTable");
    }

    tEnv.sqlUpdate("INSERT INTO IcebergTable SELECT word, num from words");

    env.execute();

    // Assert the table records as expected.
    List<Record> expected = Lists.newArrayList();
    for (int i = 0; i < 2; i++) { // two checkpoints in the FiniteTestSource.
      for (int k = 0; k < worlds.length; k++) {
        expected.add(WordCountData.RECORD.copy(ImmutableMap.of("word", worlds[k], "num", k + 1)));
      }
    }
    TestUtility.checkIcebergTableRecords(tableLocation, expected, WordCountData.RECORD_COMPARATOR);
  }

  @Test
  public void testParallelismOneByDDL() throws Exception {
    testSQL(1, true);
  }

  @Test
  public void testParallelismOneByDescriptor() throws Exception {
    testSQL(1, false);
  }

  @Test
  public void testMultipleParallelismByDDL() throws Exception {
    testSQL(4, true);
  }

  @Test
  public void testMultipleParallelismByDescriptor() throws Exception {
    testSQL(4, false);
  }
}
