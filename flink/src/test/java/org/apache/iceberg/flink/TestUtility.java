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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.junit.Assert;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;

public class TestUtility {

  private TestUtility() {

  }

  public static DataFile writeRecords(Collection<Row> rows, Schema schema, Path path) throws IOException {
    Configuration conf = new Configuration();
    FileAppender<Row> parquetAppender = Parquet.write(fromPath(path, conf))
        .schema(schema)
        .createWriterFunc(FlinkParquetWriters::buildWriter)
        .build();
    try {
      parquetAppender.addAll(rows);
    } finally {
      parquetAppender.close();
    }
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withInputFile(HadoopInputFile.fromPath(path, conf))
        .withMetrics(parquetAppender.metrics())
        .build();
  }

  /**
   * Read the Iceberg table's records from the given table location, and assert that the records should be matched to
   * the given record collections.
   *
   * @param tableLocation to read the iceberg records.
   * @param expected      records to check.
   * @param comparator    to sort the records in order.
   */
  public static void checkIcebergTableRecords(String tableLocation,
                                              List<Record> expected,
                                              Comparator<Record> comparator) {
    Preconditions.checkArgument(expected != null, "expected records shouldn't be null");
    Table newTable = new HadoopTables().load(tableLocation);
    List<Record> results = Lists.newArrayList(IcebergGenerics.read(newTable).build());

    expected.sort(comparator);
    results.sort(comparator);
    Assert.assertEquals("Should produce the expected record", expected, results);
  }

  public static void checkTableSameRecords(String srcTableLocation,
                                           String dstTableLocation,
                                           Comparator<Record> comparator) {
    Table srcTable = new HadoopTables().load(srcTableLocation);
    Table dstTable = new HadoopTables().load(dstTableLocation);
    List<Record> srcRecords = Lists.newArrayList(IcebergGenerics.read(srcTable).build());
    List<Record> dstRecords = Lists.newArrayList(IcebergGenerics.read(dstTable).build());
    srcRecords.sort(comparator);
    dstRecords.sort(comparator);
    Assert.assertEquals("Should produce the expected record", srcRecords, dstRecords);
  }
}
