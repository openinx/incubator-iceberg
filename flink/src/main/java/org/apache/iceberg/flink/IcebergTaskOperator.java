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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Deque;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.reader.RowReader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTaskOperator extends AbstractStreamOperator<Row>
    implements OneInputStreamOperator<CombinedScanTask, Row> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTaskOperator.class);

  /* Serializable fields */
  private final String tableLocation;
  private final SerializableConfiguration conf;
  private Schema readSchema;

  /* Non-serializable fields */
  private transient Table table;
  private transient ListState<byte[]> scanTasksState;
  private transient SourceFunction.SourceContext<Row> sourceContext;
  private transient MailboxExecutor executor;
  // Now we use the queue to buffer the unhandled scan task, while actually there's possible that the upstream will
  // reproduce the duplicated scan task, so here it would need a MAP/SET to remove the duplicated task. (TODO).
  // By the way, we should handle the task by the order of sequence number DESC in future update/delete implementation.
  private transient Deque<ScanTaskSplit> scanTaskSplits;
  private transient ScanTaskSplitsSerializer serializer;

  public IcebergTaskOperator(String tableLocation, SerializableConfiguration conf,
                             Schema readSchema, MailboxExecutor executor) {
    this.tableLocation = tableLocation;
    this.conf = conf;
    this.readSchema = readSchema;
    this.executor = executor;
  }

  @Override
  public void initializeState(StateInitializationContext ctx) throws Exception {
    super.initializeState(ctx);

    this.table = new HadoopTables(conf.get()).load(tableLocation);
    if (this.readSchema != null) {
      // Reassign ids to match the base schema.
      readSchema = TypeUtil.reassignIds(readSchema, table.schema());
    }
    int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
    this.serializer = new ScanTaskSplitsSerializer();

    this.scanTaskSplits = new LinkedList<>();
    this.scanTasksState = ctx.getOperatorStateStore().getListState(
        new ListStateDescriptor<>("iceberg-scan-split-task-states", BytePrimitiveArraySerializer.INSTANCE));
    if (ctx.isRestored()) {
      LOG.info("Restoring state from the {} (task-index={})", getClass().getSimpleName(), taskIdx);
      byte[] data = scanTasksState.get().iterator().next();
      this.scanTaskSplits = this.serializer.deserialize(ScanTaskSplitsSerializer.SERIALIZATION_VERSION, data);
    }
  }

  @Override
  public void open() throws Exception {
    super.open();

    this.sourceContext = StreamSourceContexts.getSourceContext(
        getOperatorConfig().getTimeCharacteristic(),
        getProcessingTimeService(),
        new Object(), // no actual locking needed
        getContainingTask().getStreamStatusMaintainer(),
        output,
        getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval(),
        -1);

    if (!scanTaskSplits.isEmpty()) {
      // Schedule to read and collect the latest row.
      readRowAndCollect(scanTaskSplits);
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);

    byte[] data = this.serializer.serialize(this.scanTaskSplits);
    this.scanTasksState.clear();
    this.scanTasksState.add(data);
  }

  @Override
  public void processElement(StreamRecord<CombinedScanTask> element) throws Exception {
    this.scanTaskSplits.addLast(new ScanTaskSplit(element.getValue(), 0));
    // Schedule to read and collect the latest row.
    readRowAndCollect(scanTaskSplits);
  }

  @Override
  public void dispose() throws Exception {
    super.dispose();
  }

  @Override
  public void close() throws Exception {
    super.close();

    // Emit the max watermark to end the stream.
    try {
      sourceContext.emitWatermark(Watermark.MAX_WATERMARK);
    } catch (Exception e) {
      LOG.warn("Unable to emit watermark while closing", e);
    }

    cleanUp();
  }

  private void readRowAndCollect(Deque<ScanTaskSplit> queue) {
    this.executor.execute(() -> {
      if (!queue.isEmpty()) {
        ScanTaskSplit split = queue.getFirst();
        if (split.hasNext()) {
          Row row = split.next();
          this.sourceContext.collect(row);
        } else {
          split.close();
          queue.removeFirst();
        }
        // Re-enqueue the mail box again to coordinate with the checkpoint.
        if (!queue.isEmpty()) {
          readRowAndCollect(queue);
        }
      }
    }, "IcebergTaskOperator-ReadRowAndCollect");
  }

  private void cleanUp() {
    this.scanTaskSplits.clear();
    this.sourceContext.close();
    this.output.close();
  }

  class ScanTaskSplit implements CloseableIterator<Row> {
    /* Serializable fields */
    private final CombinedScanTask scanTask;
    private int consumedOffset;

    /* Non-serializable fields */
    private transient RowReader reader;

    ScanTaskSplit(CombinedScanTask scanTask, int consumedOffset) {
      this.scanTask = scanTask;
      this.consumedOffset = consumedOffset;
      initReader();
    }

    private void initReader() {
      this.reader = new RowReader(this.scanTask, table.io(), readSchema, table.encryption(), true);
      // Drop all the consumed rows firstly.
      for (int i = 0; i < consumedOffset; i++) {
        if (reader.hasNext()) {
          // Skip the consumed rows
          reader.next();
        } else {
          throw new NoSuchElementException();
        }
      }
    }

    @Override
    public void close() throws IOException {
      this.reader.close();
    }

    @Override
    public boolean hasNext() {
      return this.reader.hasNext();
    }

    @Override
    public Row next() {
      Row row = this.reader.next();
      consumedOffset++;
      return row;
    }
  }

  class ScanTaskSplitsSerializer implements SimpleVersionedSerializer<Deque<ScanTaskSplit>> {
    private static final int SERIALIZATION_VERSION = 1;
    private static final int MAGIC = 0xffa223f2;

    @Override
    public int getVersion() {
      return SERIALIZATION_VERSION;
    }

    @Override
    public byte[] serialize(Deque<ScanTaskSplit> states) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeInt(MAGIC);
        oos.writeInt(states.size());
        for (ScanTaskSplit state : states) {
          oos.writeObject(state.scanTask);
          oos.writeInt(state.consumedOffset);
        }
      }
      return baos.toByteArray();
    }

    @Override
    public Deque<ScanTaskSplit> deserialize(int version, byte[] serialized) throws IOException {
      ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
      try (ObjectInputStream ois = new ObjectInputStream(bais)) {
        // check the serialization version.
        assert version == getVersion() : "version mismatched because the deserialize version is " +
            version + ", while the serialize version is " + getVersion();
        // check the magic number.
        int magic = ois.readInt();
        assert magic == MAGIC : "magic number mismatch the expected magic number.";

        // read the task one by one.
        int size = ois.readInt();
        Deque<ScanTaskSplit> states = new LinkedList<>();
        for (int i = 0; i < size; i++) {
          CombinedScanTask task = (CombinedScanTask) ois.readObject();
          int consumedOffset = ois.readInt();
          states.addLast(new ScanTaskSplit(task, consumedOffset));
        }
        return states;
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
  }
}
