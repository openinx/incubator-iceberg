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
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// TODO make it work on both parquet and avro format.
public class TestFlinkSinkFunction {

  private static final Schema SCHEMA = new Schema(
      org.apache.iceberg.types.Types.NestedField.optional(
          1, "word",
          org.apache.iceberg.types.Types.StringType.get()
      ),
      org.apache.iceberg.types.Types.NestedField.optional(
          2, "count",
          org.apache.iceberg.types.Types.IntegerType.get()
      )
  );

  private static final RowTypeInfo FLINK_SCHEMA = (RowTypeInfo) Types.ROW(Types.STRING, Types.INT);
  private static final Record RECORD = GenericRecord.create(SCHEMA);
  private static final Comparator<Record> RECORD_CMP = (r1, r2) -> {
    int ret = StringUtils.compare((String) r1.getField("word"), (String) r2.getField("word"));
    if (ret != 0) {
      return ret;
    }
    return ((Integer) r1.getField("count")) - ((Integer) r2.getField("count"));
  };

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String tableLocation = null;
  private Table table;

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    tableLocation = folder.getAbsolutePath();
    table = createTestIcebergTable();
    Assert.assertNotNull(table);
  }

  private Table createTestIcebergTable() {
    PartitionSpec spec = PartitionSpec
        .builderFor(SCHEMA)
        .identity("word")
        .build();
    return new HadoopTables().create(SCHEMA, spec, tableLocation);
  }

  private static void setFinalFieldWithValue(Field field, Object obj, Object newValue) throws Exception {
    field.setAccessible(true);

    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

    field.set(obj, newValue);
  }

  private OneInputStreamOperatorTestHarness<Row, Object> createSink() throws Exception {
    IcebergSinkFunction sink = new IcebergSinkFunction(tableLocation, FLINK_SCHEMA);
    OneInputStreamOperatorTestHarness<Row, Object> testHarness = new OneInputStreamOperatorTestHarness<>(
        new StreamSink<>(sink),
        1, 1, 0);
    MockEnvironment env = testHarness.getEnvironment();
    Field aggManagerField = MockEnvironment.class.getDeclaredField("aggregateManager");
    // The default TestGlobalAggregateManager won't accumulate the new committed value, which won't produce the
    // correct value. While it's a private final member in MockEnvironment, so we need the hack way to set it to
    // use the correct TestGlobalAggregateManager instance.
    setFinalFieldWithValue(aggManagerField, env, new TestGlobalAggregateManager());
    return testHarness;
  }

  @Test
  public void testClosingWithoutInput() throws Exception {
    try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink()) {
      testHarness.setup();
      testHarness.open();
    }
  }

  private long countDataFiles() throws IOException {
    return Files
        .walk(Paths.get(tableLocation, "data"))
        .filter(p -> p.getFileName().toString().endsWith(".parquet"))
        .count();
  }

  @Test
  public void testTableCommit() throws Exception {
    try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink()) {
      testHarness.setup();
      testHarness.open();

      testHarness.processElement(new StreamRecord<>(Row.of("hello", 1), 1L));
      testHarness.processElement(new StreamRecord<>(Row.of("world", 2), 1L));
      testHarness.processElement(new StreamRecord<>(Row.of("foo", 3), 1L));
      testHarness.processElement(new StreamRecord<>(Row.of("bar", 4), 1L));

      testHarness.snapshot(1L, 1L);
      Assert.assertEquals(4L, countDataFiles());

      testHarness.snapshot(2L, 1L);
      Assert.assertEquals(4L, countDataFiles());

      testHarness.notifyOfCompletedCheckpoint(2L);
      Assert.assertEquals(4L, countDataFiles());

      // Full scan the local table.

      checkTableRecords(Lists.newArrayList(
          RECORD.copy(ImmutableMap.of("word", "hello", "count", 1)),
          RECORD.copy(ImmutableMap.of("word", "world", "count", 2)),
          RECORD.copy(ImmutableMap.of("word", "foo", "count", 3)),
          RECORD.copy(ImmutableMap.of("word", "bar", "count", 4))
      ));
    }
  }

  @Test
  public void testRecovery() throws Exception {
    OperatorSubtaskState snapshot;
    try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink()) {
      testHarness.setup();
      testHarness.open();

      testHarness.processElement(new StreamRecord<>(Row.of("hello", 1), 1L));
      testHarness.processElement(new StreamRecord<>(Row.of("world", 2), 1L));
      testHarness.processElement(new StreamRecord<>(Row.of("foo", 3), 1L));
      testHarness.processElement(new StreamRecord<>(Row.of("bar", 4), 1L));

      snapshot = testHarness.snapshot(1L, 1L);
      Assert.assertEquals(4L, countDataFiles());
      Assert.assertEquals(-1L, GlobalTableCommitter.getMaxCommittedCheckpointId(tableLocation, new Configuration()));

      testHarness.processElement(new StreamRecord<>(Row.of("flink", 5), 1L));
      testHarness.processElement(new StreamRecord<>(Row.of("iceberg", 6), 1L));

      testHarness.notifyOfCompletedCheckpoint(1L);
      Assert.assertEquals(6L, countDataFiles());
      Assert.assertEquals(1L, GlobalTableCommitter.getMaxCommittedCheckpointId(tableLocation, new Configuration()));
      testHarness.snapshot(2L, 1L);

      // Full scan the local table.
      checkTableRecords(Lists.newArrayList(
          RECORD.copy(ImmutableMap.of("word", "hello", "count", 1)),
          RECORD.copy(ImmutableMap.of("word", "world", "count", 2)),
          RECORD.copy(ImmutableMap.of("word", "foo", "count", 3)),
          RECORD.copy(ImmutableMap.of("word", "bar", "count", 4))
      ));
    }
    try (OneInputStreamOperatorTestHarness<Row, Object> testHarness = createSink()) {
      testHarness.setup();
      testHarness.initializeState(snapshot);
      testHarness.open();

      testHarness.processElement(new StreamRecord<>(Row.of("tail", 5), 1L));
      testHarness.processElement(new StreamRecord<>(Row.of("head", 6), 1L));

      testHarness.snapshot(3L, 1L);
      Assert.assertEquals(8L, countDataFiles());
      testHarness.notifyOfCompletedCheckpoint(3L);
      Assert.assertEquals(3L, GlobalTableCommitter.getMaxCommittedCheckpointId(tableLocation, new Configuration()));

      // Full scan the local table.
      checkTableRecords(Lists.newArrayList(
          RECORD.copy(ImmutableMap.of("word", "hello", "count", 1)),
          RECORD.copy(ImmutableMap.of("word", "world", "count", 2)),
          RECORD.copy(ImmutableMap.of("word", "foo", "count", 3)),
          RECORD.copy(ImmutableMap.of("word", "bar", "count", 4)),
          RECORD.copy(ImmutableMap.of("word", "tail", "count", 5)),
          RECORD.copy(ImmutableMap.of("word", "head", "count", 6))
      ));
    }
  }

  private void checkTableRecords(List<Record> expected) {
    Table newTable = new HadoopTables().load(tableLocation);
    List<Record> results = Lists.newArrayList(IcebergGenerics.read(newTable).build());
    expected.sort(RECORD_CMP);
    results.sort(RECORD_CMP);
    Assert.assertEquals("Should produce the expected record", expected, results);
  }

  private static class TestGlobalAggregateManager implements GlobalAggregateManager {

    private Map<String, Object> accumulators = new HashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public <InT, AccT, OutT> OutT updateGlobalAggregate(
        String aggregateName,
        Object aggregand,
        AggregateFunction<InT, AccT, OutT> aggregateFunction) throws IOException {
      try {
        InstantiationUtil.clone((Serializable) aggregand);
        InstantiationUtil.clone(aggregateFunction);

        Object accumulator = accumulators.get(aggregateName);
        if (null == accumulator) {
          accumulator = aggregateFunction.createAccumulator();
        }
        accumulator = aggregateFunction.add((InT) aggregand, (AccT) accumulator);
        accumulators.put(aggregateName, accumulator);
        OutT result = aggregateFunction.getResult((AccT) accumulator);

        InstantiationUtil.clone((Serializable) result);
        return result;
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
  }
}
