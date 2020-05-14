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
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.data.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFlinkSource extends AbstractTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestFlinkSource.class);

  private static final String[] WORDS = new String[]{"hello", "world", "foo", "bar", "apache", "software"};
  private static final Configuration CONF = new Configuration();

  private String srcTableLocation;
  private String dstTableLocation;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    this.srcTableLocation = temp.newFolder().getAbsolutePath();
    this.dstTableLocation = temp.newFolder().getAbsolutePath();
    // Initialize the table schema.
    WordCountData.createTable(srcTableLocation, true);
    WordCountData.createTable(dstTableLocation, true);
  }

  @Test
  public void testExactlyOnce() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(500);
    env.setParallelism(1);

    IcebergSinkFunction icebergSink = IcebergSinkFunction.builder()
        .withTableSchema(WordCountData.FLINK_SCHEMA)
        .withTableLocation(srcTableLocation)
        .withConfiguration(CONF)
        .build();

    env.addSource(new TestRowSource(10))
        .setParallelism(1)
        .map(new WordCountData.Transformer())
        .addSink(icebergSink)
        .setParallelism(2);
    env.execute();

    List<Record> expectedRecords = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      for (String word : WORDS) {
        expectedRecords.add(WordCountData.RECORD.copy(ImmutableMap.of("word", word, "num", i)));
      }
    }
    // Assert that the source table has the expected records.
    TestUtility.checkIcebergTableRecords(srcTableLocation, expectedRecords, WordCountData.RECORD_COMPARATOR);

    IcebergSinkFunction dstSink = IcebergSinkFunction.builder()
        .withTableSchema(WordCountData.FLINK_SCHEMA)
        .withTableLocation(dstTableLocation)
        .withConfiguration(CONF)
        .build();
    IcebergSource.createSource(env, srcTableLocation, CONF, IcebergSource.NON_CONSUMED_SNAPSHOT_ID,
        1000L, 10L, WordCountData.FLINK_SCHEMA)
        .map(WordCountData.newTransformer())
        .addSink(dstSink)
        .setParallelism(1);
    env.execute();

    // Assert that the destination table has the expected records
    TestUtility.checkIcebergTableRecords(dstTableLocation, expectedRecords, WordCountData.RECORD_COMPARATOR);

    // Try to read parts of the table columns in a new streaming JOB.
    TableSchema wordColumn = TableSchema.builder().field("word", DataTypes.STRING()).build();
    String file = temp.newFile().toURI().toString();
    IcebergSource.createSource(env, srcTableLocation, CONF, IcebergSource.NON_CONSUMED_SNAPSHOT_ID,
        1000L, 10L, wordColumn)
        .writeAsText(file);
    env.execute();

    List<String> expectWords = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      expectWords.addAll(Arrays.asList(WORDS));
    }
    List<String> actualWords = Lists.newArrayList();
    readAllResultLines(actualWords, file);
    Collections.sort(expectWords);
    Collections.sort(actualWords);
    Assert.assertEquals(expectWords, actualWords);
  }

  static class TestRowSource implements SourceFunction<Row>, CheckpointListener {

    private final int expectedCheckpointNum;
    private transient int currentCheckpointsComplete = 0;
    private volatile boolean running = true;

    TestRowSource(int expectedCheckpointNum) {
      this.expectedCheckpointNum = expectedCheckpointNum;
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      for (int i = 0; i < expectedCheckpointNum; i++) {
        int checkpointToAwait;
        synchronized (ctx.getCheckpointLock()) {
          checkpointToAwait = i + 1;
          for (String word : WORDS) {
            ctx.collect(Row.of(word, i));
          }
        }

        synchronized (ctx.getCheckpointLock()) {
          while (running && currentCheckpointsComplete < checkpointToAwait) {
            ctx.getCheckpointLock().wait(100);
          }
        }
      }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
      currentCheckpointsComplete++;
      LOG.info("Checkpoint with checkpoint id {} has been completed.", checkpointId);
    }

    @Override
    public void cancel() {
      running = false;
    }
  }
}
