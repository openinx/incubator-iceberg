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
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkIcebergSink extends AbstractTestBase {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String tableLocation;
  private final Configuration conf = new Configuration();

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    tableLocation = folder.getAbsolutePath();
  }

  @Test
  public void testDataStreamParallelismOne() throws Exception {
    testDataStream(1, false);
  }

  @Test
  public void testDataStreamMultiParallelism() throws Exception {
    testDataStream(3, false);
  }

  @Test
  public void testDataStreamMultiParallelismInUnpartitionedTable() throws Exception {
    testDataStream(3, true);
  }

  private void testDataStream(int parallelism, boolean partitionTable) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Enable the checkpoint.
    env.enableCheckpointing(100);
    env.setParallelism(parallelism);

    List<Row> rows = Lists.newArrayList(
        Row.of("hello", 2),
        Row.of("world", 2),
        Row.of("word", 1)
    );

    DataStream<Row> dataStream = env.addSource(new FiniteTestSource<>(rows), WordCountData.FLINK_SCHEMA.toRowType());

    Table table = WordCountData.createTable(tableLocation, partitionTable);
    Assert.assertNotNull(table);

    // Output the data stream to stdout.
    dataStream.map(new WordCountData.Transformer())
        .addSink(IcebergSinkFunction
            .builder()
            .withTableLocation(tableLocation)
            .withConfiguration(conf)
            .build());

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    table.refresh();
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      for (Row row : rows) {
        records.add(WordCountData.RECORD.copy(ImmutableMap.of("word", row.getField(0), "num", row.getField(1))));
      }
    }
    TestUtility.checkIcebergTableRecords(tableLocation, Lists.newArrayList(records), WordCountData.RECORD_COMPARATOR);
  }
}
