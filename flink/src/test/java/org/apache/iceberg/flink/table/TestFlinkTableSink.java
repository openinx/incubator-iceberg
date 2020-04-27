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
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestUtility;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkTableSink extends AbstractTestBase {

  private static final org.apache.iceberg.Schema SCHEMA = new org.apache.iceberg.Schema(
      Types.NestedField.optional(1, "word", Types.StringType.get()),
      Types.NestedField.optional(2, "num", Types.LongType.get())
  );
  private static final Record RECORD = GenericRecord.create(SCHEMA);

  private static final Comparator<Record> RECORD_COMPARATOR = (r1, r2) -> {
    int ret = StringUtils.compare((String) r1.getField("word"), (String) r2.getField("word"));
    if (ret != 0) {
      return ret;
    }
    return Long.compare((Long) r1.getField("num"), (Long) r2.getField("num"));
  };

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
    Assert.assertNotNull(createTestIcebergTable());
  }

  private org.apache.iceberg.Table createTestIcebergTable() {
    PartitionSpec spec = PartitionSpec
        .builderFor(SCHEMA)
        .identity("word")
        .build();
    return new HadoopTables().create(SCHEMA, spec, tableLocation);
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
    List<Tuple2<String, Long>> tuple2s = Lists.newArrayList();
    for (int i = 0; i < worlds.length; i++) {
      tuple2s.add(Tuple2.of(worlds[i], i + 1L));
    }
    TupleTypeInfo<Tuple2<String, Long>> tupleTypeInfo = new TupleTypeInfo<>(
        org.apache.flink.api.common.typeinfo.Types.STRING,
        org.apache.flink.api.common.typeinfo.Types.LONG);
    DataStream<Tuple2<String, Long>> dataStream = env.addSource(new FiniteTestSource<>(tuple2s), tupleTypeInfo);

    tEnv.createTemporaryView("words", tEnv.fromDataStream(dataStream, "word,num"));

    if (useDDL) {
      String ddl = String.format(
          "CREATE TABLE IcebergTable(" +
              "word string, " +
              "num bigint) " +
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
          .withSchema(new Schema().schema(
              TableSchema.builder()
                  .field("word", DataTypes.STRING())
                  .field("num", DataTypes.BIGINT())
                  .build()))
          .inUpsertMode()
          .createTemporaryTable("IcebergTable");
    }

    tEnv.sqlUpdate("INSERT INTO IcebergTable SELECT word, num from words");

    env.execute();

    // Assert the table records as expected.
    List<Record> expected = Lists.newArrayList();
    for (int i = 0; i < 2; i++) { // two checkpoints in the FiniteTestSource.
      for (int k = 0; k < worlds.length; k++) {
        expected.add(RECORD.copy(ImmutableMap.of("word", worlds[k], "num", k + 1L)));
      }
    }
    TestUtility.checkIcebergTableRecords(tableLocation, expected, RECORD_COMPARATOR);
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
