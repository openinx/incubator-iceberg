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
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Tables;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkIcebergSink extends AbstractTestBase {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String tableLocation;

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    tableLocation = folder.getAbsolutePath();
  }

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "word", Types.StringType.get()),
      Types.NestedField.optional(2, "count", Types.IntegerType.get())
  );

  private Table createTestIcebergTable() {
    PartitionSpec spec = PartitionSpec
        .builderFor(SCHEMA)
        .identity("word")
        .build();
    Tables table = new HadoopTables();
    return table.create(SCHEMA, spec, tableLocation);
  }

  @Test
  public void testDataStreamParallelism() throws Exception {
    testDataStream(1);
  }

  @Test
  public void testDataStreamMultiParallelism() throws Exception {
    testDataStream(3);
  }

  private void testDataStream(int parallelism) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Enable the checkpoint.
    env.enableCheckpointing(100);
    env.setParallelism(parallelism);

    RowTypeInfo flinkSchema = new RowTypeInfo(
        org.apache.flink.api.common.typeinfo.Types.STRING,
        org.apache.flink.api.common.typeinfo.Types.INT
    );

    List<Row> rows = Lists.newArrayList(
        Row.of("hello", 2),
        Row.of("world", 2),
        Row.of("word", 1)
    );

    DataStream<Row> dataStream = env.addSource(new FiniteTestSource<>(rows), flinkSchema);

    Table table = createTestIcebergTable();
    Assert.assertNotNull(table);

    // Output the data stream to stdout.
    dataStream.addSink(new IcebergSinkFunction(tableLocation, flinkSchema));

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    table.refresh();
    Iterable<Record> results = IcebergGenerics.read(table).build();
    List<Record> records = Lists.newArrayList(results);
    // The stream will produce (hello,2),(world,2),(word,1),(hello,2),(world,2),(word,1) actually,
    // because the FiniteTestSource will produce double row list.
    Assert.assertEquals(6, records.size());

    // The hash set will remove the duplicated rows.
    Set<Record> real = Sets.newHashSet(records);
    Record record = GenericRecord.create(SCHEMA);
    Set<Record> expected = Sets.newHashSet(
        record.copy(ImmutableMap.of("word", "hello", "count", 2)),
        record.copy(ImmutableMap.of("word", "word", "count", 1)),
        record.copy(ImmutableMap.of("word", "world", "count", 2))
    );
    Assert.assertEquals("Should produce the expected record", expected, real);
  }
}
