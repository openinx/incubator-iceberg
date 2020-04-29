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

import java.util.List;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.CheckCompatibility;
import org.apache.iceberg.types.Type;

public class FlinkSchemaUtil {

  private FlinkSchemaUtil() {
  }

  public static Schema convert(TableSchema flinkSchema) {
    RowTypeInfo rowTypeInfo = new RowTypeInfo(flinkSchema.getFieldTypes(), flinkSchema.getFieldNames());
    Type converted = FlinkTypeVisitor.visit(rowTypeInfo, new FlinkTypeToType(rowTypeInfo));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  static void validate(Schema sinkSchema, Schema sourceSchema, boolean checkNullability, boolean checkOrdering) {
    List<String> errors;
    if (checkNullability) {
      errors = CheckCompatibility.writeCompatibilityErrors(sinkSchema, sourceSchema, checkOrdering);
    } else {
      errors = CheckCompatibility.typeCompatibilityErrors(sinkSchema, sourceSchema, checkOrdering);
    }
    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Cannot write incompatible dataset to table with schema:\n")
          .append(sinkSchema)
          .append("\nProblems:");
      for (String error : errors) {
        sb.append("\n* ").append(error);
      }
      throw new IllegalArgumentException(sb.toString());
    }
  }
}
