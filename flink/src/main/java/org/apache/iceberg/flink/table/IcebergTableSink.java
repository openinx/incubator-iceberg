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

import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.IcebergSinkFunction;

public class IcebergTableSink implements UpsertStreamTableSink<Row> {

  private final boolean isAppendOnly;
  private final String tableIdentifier;
  private final TableSchema tableSchema;

  public IcebergTableSink(boolean isAppendOnly, String tableIdentifier, TableSchema tableSchema) {
    this.isAppendOnly = isAppendOnly;
    this.tableIdentifier = tableIdentifier;
    this.tableSchema = tableSchema;
  }

  @Override
  public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
    consumeDataStream(dataStream);
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
    Configuration conf = new Configuration();
    IcebergSinkFunction sinkFunction = IcebergSinkFunction.builder()
        .withConfiguration(conf)
        .withTableLocation(tableIdentifier)
        .withTableSchema(tableSchema)
        .build();
    return dataStream
        .addSink(sinkFunction)
        .setParallelism(dataStream.getParallelism())
        .name(TableConnectorUtils.generateRuntimeName(this.getClass(), tableSchema.getFieldNames()));
  }

  @Override
  public TableSchema getTableSchema() {
    return this.tableSchema;
  }

  @Override
  public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
    if (!Arrays.equals(tableSchema.getFieldNames(), fieldNames)) {
      String expectedFieldNames = Arrays.toString(tableSchema.getFieldNames());
      String actualFieldNames = Arrays.toString(fieldNames);
      throw new ValidationException("The field names is mismatched. Expected: " +
          expectedFieldNames + "But was: " + actualFieldNames);
    }
    if (!Arrays.equals(tableSchema.getFieldTypes(), fieldTypes)) {
      String expectedFieldTypes = Arrays.toString(tableSchema.getFieldTypes());
      String actualFieldTypes = Arrays.toString(fieldNames);
      throw new ValidationException("Field types are mismatched. Expected: " +
          expectedFieldTypes + " But was: " + actualFieldTypes);
    }
    return this;
  }

  @Override
  public void setKeyFields(String[] keys) {
    // do nothing here.
  }

  @Override
  public void setIsAppendOnly(Boolean isAppendOnly) {
    if (this.isAppendOnly && !isAppendOnly) {
      throw new ValidationException("The given query is not supported by this sink because the sink is configured " +
          "to operate in append mode only. Thus, it only support insertions (no queries with updating results).");
    }
  }

  @Override
  public TypeInformation<Row> getRecordType() {
    return this.tableSchema.toRowType();
  }

  @Override
  public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
    return Types.TUPLE(Types.BOOLEAN, getRecordType());
  }
}
