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

package org.apache.iceberg.flink.source;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.flink.types.Row;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.junit.Assert;
import org.junit.Test;

public class TestBoundedTableFactory extends ChangeLogTableTestBase {

  @Test
  public void testEmptyDataSet() {
    String table = name.getMethodName();
    List<List<Row>> emptyDataSet = ImmutableList.of();

    String dataId = BoundedTableFactory.registerDataSet(emptyDataSet);
    sql("CREATE TABLE %s(id INT, data STRING) WITH ('connector'='BoundedSource', 'data-id'='%s')", table, dataId);

    Assert.assertEquals("Should have caught empty change log set.", ImmutableList.of(),
        sql("SELECT * FROM %s", table));
  }

  @Test
  public void testBoundedTableFactory() {
    String table = name.getMethodName();
    List<List<Row>> dataSet = ImmutableList.of(
        ImmutableList.of(
            row("+I", 1, "aaa"),
            row("-D", 1, "aaa"),
            row("+I", 1, "bbb"),
            row("+I", 2, "aaa"),
            row("-D", 2, "aaa"),
            row("+I", 2, "bbb")
        ),
        ImmutableList.of(
            row("-U", 2, "bbb"),
            row("+U", 2, "ccc"),
            row("-D", 2, "ccc"),
            row("+I", 2, "ddd")
        ),
        ImmutableList.of(
            row("-D", 1, "bbb"),
            row("+I", 1, "ccc"),
            row("-D", 1, "ccc"),
            row("+I", 1, "ddd")
        )
    );

    String dataId = BoundedTableFactory.registerDataSet(dataSet);
    sql("CREATE TABLE %s(id INT, data STRING) WITH ('connector'='BoundedSource', 'data-id'='%s')", table, dataId);

    List<Row> rowSet = dataSet.stream().flatMap(Streams::stream).collect(Collectors.toList());
    Assert.assertEquals("Should have the expected change log events.", rowSet, sql("SELECT * FROM %s", table));

    Assert.assertEquals("Should have the expected change log events",
        rowSet.stream().filter(r -> Objects.equals(r.getField(1), "aaa")).collect(Collectors.toList()),
        sql("SELECT * FROM %s WHERE data='aaa'", table));
  }
}
