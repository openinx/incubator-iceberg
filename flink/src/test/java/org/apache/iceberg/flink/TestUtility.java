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
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.Assert;

public class TestUtility {

  private TestUtility() {

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
}
