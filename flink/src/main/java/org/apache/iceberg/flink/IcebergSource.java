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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.SerializableConfiguration;

public class IcebergSource {

  public static final long NON_CONSUMED_SNAPSHOT_ID = -1L;

  private IcebergSource() {
  }

  public static DataStream<Row> createSource(StreamExecutionEnvironment env,
                                             String tableLocation,
                                             Configuration conf,
                                             long fromSnapshotId,
                                             long intervalMillis,
                                             TableSchema readSchema) {
    return createSource(env, tableLocation, conf, fromSnapshotId, intervalMillis, Long.MAX_VALUE, readSchema);
  }

  /**
   * Mainly used for unit test.
   */
  public static DataStream<Row> createSource(StreamExecutionEnvironment env,
                                             String tableLocation,
                                             Configuration conf,
                                             long fromSnapshotId,
                                             long intervalMillis,
                                             long remainingSnapshot,
                                             TableSchema readSchema) {
    IcebergSnapshotFunction snapshotFunc = new IcebergSnapshotFunction(tableLocation, conf, fromSnapshotId,
        intervalMillis, remainingSnapshot);
    // The TableSchema is not serializable so need to convert to Iceberg schema firstly here.
    Schema icebergReadSchema = FlinkSchemaUtil.convert(readSchema);
    IcebergTaskOperatorFactory factory = new IcebergTaskOperatorFactory(tableLocation, conf, icebergReadSchema);
    return env.addSource(snapshotFunc, "IcebergSnapshotFunction")
        .forceNonParallel()
        .transform("IcebergTaskOperator", readSchema.toRowType(), factory);
  }

  private static class IcebergTaskOperatorFactory implements OneInputStreamOperatorFactory<CombinedScanTask, Row>,
      YieldingOperatorFactory<Row>, StreamOperatorFactory<Row> {

    private final String tableLocation;
    private final SerializableConfiguration conf;
    private final Schema readSchema;
    private MailboxExecutor mailboxExecutor;
    private ChainingStrategy strategy = ChainingStrategy.ALWAYS;

    IcebergTaskOperatorFactory(String tableLocation, Configuration conf, Schema readSchema) {
      this.tableLocation = tableLocation;
      this.conf = new SerializableConfiguration(conf);
      this.readSchema = readSchema;
    }

    @Override
    public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
      this.mailboxExecutor = mailboxExecutor;
    }

    @Override
    public <T extends StreamOperator<Row>> T createStreamOperator(StreamTask<?, ?> containingTask,
                                                                  StreamConfig config,
                                                                  Output<StreamRecord<Row>> output) {
      IcebergTaskOperator operator = new IcebergTaskOperator(tableLocation, conf, readSchema, mailboxExecutor);
      operator.setup(containingTask, config, output);
      return (T) operator;
    }

    @Override
    public void setChainingStrategy(ChainingStrategy newStrategy) {
      this.strategy = newStrategy;
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
      return this.strategy;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergTaskOperator.class;
    }
  }
}
