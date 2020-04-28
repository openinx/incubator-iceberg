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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;

public class TestIcebergSourceFunction extends AbstractTestBase {

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema();

  private String srcTableLocation;
  private String dstTableLocation;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();


  @Before
  public void before() throws IOException {
    this.srcTableLocation = temp.newFolder().getAbsolutePath();
    this.dstTableLocation = temp.newFolder().getAbsolutePath();
  }

  @Test
  public void testExactlyOnce() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(500);
    env.setParallelism(2);

    RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.STRING, Types.INT);
    IcebergSourceFunction icebergSource = new IcebergSourceFunction(srcTableLocation);
    IcebergSinkFunction icebergSink = new IcebergSinkFunction(dstTableLocation, rowTypeInfo);

    env.addSource(icebergSource)
        .map(row -> Tuple2.of(true, row))
        .addSink(icebergSink)
        .setParallelism(2);
    env.execute();

    // Write few records into the source table.
    List<Record> records = Lists.newArrayList();
    Table table = new HadoopTables().load(srcTableLocation);
    AppendFiles appendFiles = table.newAppend();
    DataFile file1 = writeRecords(records, new Path(srcTableLocation, "data1.parquet"));
    DataFile file2 = writeRecords(records, new Path(srcTableLocation, "data2.parquet"));
    appendFiles.appendFile(file1)
        .appendFile(file2);
    // Commit the iceberg table.
    appendFiles.commit();

    // Check that the streaming reader have got the data records.
    // TODO
  }


  private DataFile writeRecords(Collection<Record> records,
                                Path path) throws IOException {
    try (FileAppender<Record> parquetAppender = Parquet.write(fromPath(path, CONF))
        .schema(SCHEMA)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build()) {
      parquetAppender.addAll(records);
      return DataFiles.builder(PartitionSpec.unpartitioned())
          .withInputFile(HadoopInputFile.fromPath(path, CONF))
          .withMetrics(parquetAppender.metrics())
          .build();
    }
  }
}
