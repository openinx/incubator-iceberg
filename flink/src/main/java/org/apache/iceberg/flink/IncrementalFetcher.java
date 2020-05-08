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

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import org.apache.flink.types.Row;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.reader.RowReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;

public class IncrementalFetcher {
  private final Table table;
  private final Consumer<Row> rowConsumer;
  private long lastConsumedSnapshotId;

  public IncrementalFetcher(Table table, long lastConsumedSnapshotId, Consumer<Row> rowConsumer) {
    this.table = table;
    this.rowConsumer = rowConsumer;
    this.lastConsumedSnapshotId = lastConsumedSnapshotId;
  }

  public long getLastConsumedSnapshotId() {
    return this.lastConsumedSnapshotId;
  }

  public void consumeNextSnap() throws IOException {
    table.refresh();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    int index = snapshotIds.indexOf(lastConsumedSnapshotId);
    if (index >= 0) {
      snapshotIds = snapshotIds.subList(0, index);
    }
    if (!snapshotIds.isEmpty()) {
      CloseableIterable<CombinedScanTask> scanTasks;
      long snapshotId = snapshotIds.get(snapshotIds.size() - 1);
      if (lastConsumedSnapshotId == -1) {
        // The IncrementalScan could only read the second snapshot's incremental data, so the first snapshot MUST use
        // the normal table scan.
        scanTasks = table.newScan().useSnapshot(snapshotId).planTasks();
      } else {
        scanTasks = table.newScan().appendsBetween(lastConsumedSnapshotId, snapshotId).planTasks();
      }
      try {
        for (CombinedScanTask scanTask : scanTasks) {
          try (RowReader reader = new RowReader(scanTask,
              table.io(), table.schema() /*TODO should be read schema */,
              table.encryption(),
              true)) {
            while (reader.next()) {
              rowConsumer.accept(reader.get());
            }
          }
        }
      } finally {
        scanTasks.close();
      }
      this.lastConsumedSnapshotId = snapshotId;
    }
  }
}
