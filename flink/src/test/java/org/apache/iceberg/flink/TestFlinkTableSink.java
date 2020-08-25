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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.flink.table.api.Expressions.$;

@RunWith(Parameterized.class)
public class TestFlinkTableSink extends AbstractTestBase {
  private static final Configuration CONF = new Configuration();
  private static final DataFormatConverters.RowConverter CONVERTER = new DataFormatConverters.RowConverter(
      SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

  private static final String TABLE_NAME = "flink_table";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String warehouse;
  private String tablePath;
  private Runnable catalogTableCreator;

  private final FileFormat format;
  private final boolean useOldPlanner;
  private final int parallelism;

  @Parameterized.Parameters(name = "{index}: format={0}, useOldPlanner={1}, parallelism={2}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[] {"avro", true, 1},
        new Object[] {"avro", false, 1},
        new Object[] {"avro", true, 2},
        new Object[] {"avro", false, 2}
    );
  }

  public TestFlinkTableSink(String format, boolean useOldPlanner, int parallelism) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.useOldPlanner = useOldPlanner;
    this.parallelism = parallelism;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    warehouse = folder.getAbsolutePath();

    tablePath = warehouse.concat("/default/").concat(TABLE_NAME);
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdirs());

    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    HadoopCatalog catalog = new HadoopCatalog(CONF, warehouse);
    catalogTableCreator = () -> catalog
        .createTable(TableIdentifier.parse("default." + TABLE_NAME),
            SimpleDataUtil.SCHEMA,
            PartitionSpec.unpartitioned(),
            props);
  }

  @Test
  public void testSQL() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(400);
    env.setParallelism(parallelism);

    EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance().inStreamingMode();
    if (useOldPlanner) {
      settingsBuilder.useBlinkPlanner();
    }
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
    tEnv.executeSql(String.format("create catalog iceberg_catalog with (" +
        "'type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')", warehouse));
    tEnv.executeSql("use catalog iceberg_catalog");
    tEnv.getConfig().getConfiguration().set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);

    // Create the table.
    catalogTableCreator.run();

    String[] worlds = new String[] {"hello", "world", "foo", "bar", "apache", "foundation"};
    List<Row> rows = Lists.newArrayList();
    for (int i = 0; i < worlds.length; i++) {
      rows.add(Row.of(i + 1, worlds[i]));
    }
    TypeInformation<Row> typeInformation = new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());
    DataStream<RowData> stream = env.addSource(new FiniteTestSource<>(rows), typeInformation)
        .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));

    // Register the rows into a temporary table named 'sourceTable'.
    tEnv.createTemporaryView("sourceTable", tEnv.fromDataStream(stream, $("id"), $("data")));

    // Redirect the records from source table to destination table.
    String insertSQL = String.format("INSERT INTO %s SELECT id,data from sourceTable",
        TABLE_NAME);
    TableResult result = tEnv.executeSql(insertSQL);
    waitComplete(result);

    // Assert the table records as expected.
    List<Record> expected = Lists.newArrayList();
    // There will be two checkpoints in FiniteTestSource, so it will produce a list with double records.
    for (int i = 0; i < 2; i++) {
      for (int k = 0; k < worlds.length; k++) {
        expected.add(SimpleDataUtil.createRecord(k + 1, worlds[k]));
      }
    }
    SimpleDataUtil.assertTableRecords(tablePath, expected);
  }

  private static void waitComplete(TableResult result) {
    result.getJobClient().ifPresent(jobClient -> {
      try {
        jobClient.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
