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
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// TODO make it work on both parquet and avro format.
public class TestFlinkSinkFunction {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String tableLocation = null;
  private final Configuration conf = new Configuration();

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    tableLocation = folder.getAbsolutePath();
    WordCountData.createTable(tableLocation, true);
  }

  private static void setFinalFieldWithValue(Field field, Object obj, Object newValue) throws Exception {
    field.setAccessible(true);

    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

    field.set(obj, newValue);
  }

  private OneInputStreamOperatorTestHarness<Tuple2<Boolean, Row>, Object> createStreamSink() throws Exception {
    IcebergSinkFunction sink = IcebergSinkFunction.builder()
        .withTableLocation(tableLocation)
        .withConfiguration(conf)
        .withTableSchema(WordCountData.FLINK_SCHEMA)
        .build();
    OneInputStreamOperatorTestHarness<Tuple2<Boolean, Row>, Object> testHarness =
        new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 1, 1, 0);
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
    try (OneInputStreamOperatorTestHarness<Tuple2<Boolean, Row>, Object> testHarness = createStreamSink()) {
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

  private static StreamRecord<Tuple2<Boolean, Row>> createRecord(long timestamp, Object... values) {
    return new StreamRecord<>(Tuple2.of(true, Row.of(values)), timestamp);
  }

  @Test
  public void testTableCommit() throws Exception {
    try (OneInputStreamOperatorTestHarness<Tuple2<Boolean, Row>, Object> testHarness = createStreamSink()) {
      testHarness.setup();
      testHarness.open();

      testHarness.processElement(createRecord(1L, "hello", 1));
      testHarness.processElement(createRecord(1L, "world", 2));
      testHarness.processElement(createRecord(1L, "foo", 3));
      testHarness.processElement(createRecord(1L, "bar", 4));

      testHarness.snapshot(1L, 1L);
      Assert.assertEquals(4L, countDataFiles());

      testHarness.snapshot(2L, 1L);
      Assert.assertEquals(4L, countDataFiles());

      testHarness.notifyOfCompletedCheckpoint(2L);
      Assert.assertEquals(4L, countDataFiles());

      // Full scan the local table.

      TestUtility.checkIcebergTableRecords(tableLocation, Lists.newArrayList(
          WordCountData.RECORD.copy(ImmutableMap.of("word", "hello", "num", 1)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "world", "num", 2)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "foo", "num", 3)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "bar", "num", 4))
      ), WordCountData.RECORD_COMPARATOR);
    }
  }

  @Test
  public void testRecovery() throws Exception {
    OperatorSubtaskState snapshot;
    try (OneInputStreamOperatorTestHarness<Tuple2<Boolean, Row>, Object> testHarness = createStreamSink()) {
      testHarness.setup();
      testHarness.open();

      testHarness.processElement(createRecord(1L, "hello", 1));
      testHarness.processElement(createRecord(1L, "world", 2));
      testHarness.processElement(createRecord(1L, "foo", 3));
      testHarness.processElement(createRecord(1L, "bar", 4));

      snapshot = testHarness.snapshot(1L, 1L);
      Assert.assertEquals(4L, countDataFiles());
      Assert.assertEquals(-1L, GlobalTableCommitter.getMaxCommittedCheckpointId(tableLocation, new Configuration()));

      testHarness.processElement(createRecord(1L, "flink", 5));
      testHarness.processElement(createRecord(1L, "iceberg", 6));

      testHarness.notifyOfCompletedCheckpoint(1L);
      Assert.assertEquals(6L, countDataFiles());
      Assert.assertEquals(1L, GlobalTableCommitter.getMaxCommittedCheckpointId(tableLocation, new Configuration()));
      testHarness.snapshot(2L, 1L);

      // Full scan the local table.
      TestUtility.checkIcebergTableRecords(tableLocation, Lists.newArrayList(
          WordCountData.RECORD.copy(ImmutableMap.of("word", "hello", "num", 1)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "world", "num", 2)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "foo", "num", 3)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "bar", "num", 4))
      ), WordCountData.RECORD_COMPARATOR);
    }
    try (OneInputStreamOperatorTestHarness<Tuple2<Boolean, Row>, Object> testHarness = createStreamSink()) {
      testHarness.setup();
      testHarness.initializeState(snapshot);
      testHarness.open();

      testHarness.processElement(createRecord(1L, "tail", 5));
      testHarness.processElement(createRecord(1L, "head", 6));

      testHarness.snapshot(3L, 1L);
      Assert.assertEquals(8L, countDataFiles());
      testHarness.notifyOfCompletedCheckpoint(3L);
      Assert.assertEquals(3L, GlobalTableCommitter.getMaxCommittedCheckpointId(tableLocation, new Configuration()));

      // Full scan the local table.
      TestUtility.checkIcebergTableRecords(tableLocation, Lists.newArrayList(
          WordCountData.RECORD.copy(ImmutableMap.of("word", "hello", "num", 1)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "world", "num", 2)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "foo", "num", 3)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "bar", "num", 4)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "tail", "num", 5)),
          WordCountData.RECORD.copy(ImmutableMap.of("word", "head", "num", 6))
      ), WordCountData.RECORD_COMPARATOR);
    }
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
