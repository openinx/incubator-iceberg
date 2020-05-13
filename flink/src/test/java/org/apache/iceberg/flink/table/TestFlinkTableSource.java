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

package org.apache.iceberg.flink.table;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TestUtility;
import org.apache.iceberg.flink.WordCountData;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkTableSource {

  private FileFormat fileFormat = FileFormat.PARQUET;
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String srcTableLocation;
  private String dstTableLocation;

  @Before
  public void before() throws IOException {
    this.srcTableLocation = tempFolder.newFolder().getAbsolutePath();
    this.dstTableLocation = tempFolder.newFolder().getAbsolutePath();
    Table srcTable = WordCountData.createTable(srcTableLocation, false);
    WordCountData.createTable(dstTableLocation, false);

    // Write few records to the source table.
    writeFewRecords(srcTable, srcTableLocation);
  }

  private void writeFewRecords(Table table, String tableLocation) throws IOException {
    for (int i = 0; i < 10; i++) {
      List<Row> records = Arrays.asList(Row.of("hello", i), Row.of("word", i));
      DataFile file = TestUtility.writeRecords(records, WordCountData.SCHEMA,
          new Path(tableLocation, fileFormat.addExtension("file" + i)));
      table.newAppend().appendFile(file).commit();
    }
  }

  @Test
  public void testSource() throws Exception {
    Configuration conf = new Configuration();
    conf.setString(DeploymentOptions.TARGET, "local");
    conf.setBoolean(DeploymentOptions.ATTACHED, true);
    conf.setBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED, true);
    StreamExecutionEnvironment env = new StreamExecutionEnvironment(conf);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(400).setParallelism(1);
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
    String ddlFormat = "CREATE TABLE %s(" +
        "  word string, " +
        "  num int) " +
        "WITH (" +
        "  'connector.type'='iceberg', " +
        "  'connector.version'='0.8.0', " +
        "  'connector.iceberg-table.identifier'='%s'," +
        "  'update-mode'='upsert')";
    tEnv.sqlUpdate(String.format(ddlFormat, "srcTable", srcTableLocation));
    tEnv.sqlUpdate(String.format(ddlFormat, "dstTable", dstTableLocation));
    tEnv.sqlUpdate("INSERT INTO dstTable SELECT word, num FROM srcTable");

    // Submit the job and will cancel the job after 60 seconds.
    JobClient jobCli = env.executeAsync();
    Thread.sleep(60000L);
    jobCli.cancel();

    TestUtility.checkTableSameRecords(srcTableLocation, dstTableLocation, WordCountData.RECORD_COMPARATOR);
  }
}
