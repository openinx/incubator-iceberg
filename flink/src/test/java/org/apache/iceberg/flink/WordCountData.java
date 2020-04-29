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

import java.util.Comparator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

public class WordCountData {

  private WordCountData() {
  }

  public static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "word", Types.StringType.get()),
      Types.NestedField.optional(2, "num", Types.IntegerType.get())
  );

  public static final TableSchema FLINK_SCHEMA = TableSchema.builder()
      .field("word", DataTypes.STRING())
      .field("num", DataTypes.INT())
      .build();

  public static final Record RECORD = GenericRecord.create(SCHEMA);

  public static final Comparator<Record> RECORD_COMPARATOR = (r1, r2) -> {
    int ret = StringUtils.compare((String) r1.getField("word"), (String) r2.getField("word"));
    if (ret != 0) {
      return ret;
    }
    return Integer.compare((Integer) r1.getField("num"), (Integer) r2.getField("num"));
  };
}
