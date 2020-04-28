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
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
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

  private static final Comparator<Record> RECORD_CMP = (r1, r2) -> {
    int ret = StringUtils.compare((String) r1.getField("word"), (String) r2.getField("word"));
    if (ret != 0) {
      return ret;
    }
    return Integer.compare((Integer) r1.getField("count"), (Integer) r2.getField("count"));
  };

  private Table createTestIcebergTable(boolean partition) {
    Tables table = new HadoopTables();
    if (partition) {
      PartitionSpec spec = PartitionSpec
          .builderFor(SCHEMA)
          .identity("word")
          .build();
      return table.create(SCHEMA, spec, tableLocation);
    } else {
      return table.create(SCHEMA, tableLocation);
    }
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

    RowTypeInfo flinkSchema = new RowTypeInfo(
        org.apache.flink.api.common.typeinfo.Types.STRING,
        org.apache.flink.api.common.typeinfo.Types.INT
    );
    TupleTypeInfo<Tuple2<Boolean, Row>> tupleTypeInfo = new TupleTypeInfo<>(
        org.apache.flink.api.common.typeinfo.Types.BOOLEAN, flinkSchema);

    List<Tuple2<Boolean, Row>> rows = Lists.newArrayList(
        Tuple2.of(true, Row.of("hello", 2)),
        Tuple2.of(true, Row.of("world", 2)),
        Tuple2.of(true, Row.of("word", 1))
    );

    DataStream<Tuple2<Boolean, Row>> dataStream = env.addSource(new FiniteTestSource<>(rows), tupleTypeInfo);

    Table table = createTestIcebergTable(partitionTable);
    Assert.assertNotNull(table);

    // Output the data stream to stdout.
    dataStream.addSink(new IcebergSinkFunction(tableLocation, flinkSchema));

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    table.refresh();
    Record record = GenericRecord.create(SCHEMA);
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      for (Tuple2<Boolean, Row> tuple2 : rows) {
        records.add(record.copy(ImmutableMap.of("word", tuple2.f1.getField(0),
            "count", tuple2.f1.getField(1))));
      }
    }
    TestUtility.checkIcebergTableRecords(tableLocation, Lists.newArrayList(records), RECORD_CMP);
  }
}
