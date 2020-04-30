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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

public class WordCountData {

  private WordCountData() {
  }

  public static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(0, "word", Types.StringType.get()),
      Types.NestedField.optional(1, "num", Types.IntegerType.get())
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

  public static Table createTable(String tableIdentifier, boolean partitioned) {
    if (partitioned) {
      PartitionSpec spec = PartitionSpec
          .builderFor(SCHEMA)
          .identity("word")
          .build();
      return new HadoopTables().create(SCHEMA, spec, tableIdentifier);
    } else {
      return new HadoopTables().create(SCHEMA, tableIdentifier);
    }
  }

  public static class Transformer implements MapFunction<Row, Tuple2<Boolean, Row>> {

    @Override
    public Tuple2<Boolean, Row> map(Row value) {
      return Tuple2.of(true, value);
    }
  }
}
