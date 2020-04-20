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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class TypeToFlinkType extends TypeUtil.SchemaVisitor<TypeInformation<?>> {

  TypeToFlinkType() {
  }

  @Override
  public TypeInformation<?> schema(Schema schema, TypeInformation<?> typeInfo) {
    return typeInfo;
  }

  @Override
  public TypeInformation<?> struct(Types.StructType struct, List<TypeInformation<?>> fieldResults) {
    return new RowTypeInfo(fieldResults.toArray(new TypeInformation[0]));
  }

  @Override
  public TypeInformation<?> field(Types.NestedField field, TypeInformation<?> fieldResult) {
    return fieldResult;
  }

  @Override
  public TypeInformation<?> list(Types.ListType list, TypeInformation<?> elementResult) {
    return new ListTypeInfo(elementResult);
  }

  @Override
  public TypeInformation<?> map(Types.MapType map, TypeInformation<?> keyResult, TypeInformation<?> valueResult) {
    return new MapTypeInfo(keyResult, valueResult);
  }

  @Override
  public TypeInformation<?> primitive(Type.PrimitiveType primitive) {
    switch (primitive.typeId()) {
      case BOOLEAN:
        return BasicTypeInfo.BOOLEAN_TYPE_INFO;
      case INTEGER:
        return BasicTypeInfo.INT_TYPE_INFO;
      case LONG:
        return BasicTypeInfo.LONG_TYPE_INFO;
      case FLOAT:
        return BasicTypeInfo.FLOAT_TYPE_INFO;
      case DOUBLE:
        return BasicTypeInfo.DOUBLE_TYPE_INFO;
      case DATE:
        return BasicTypeInfo.DATE_TYPE_INFO;
      case TIME:
        throw new UnsupportedOperationException(
            "Flink does not support time fields");
      case TIMESTAMP:
        // TODO check whether Flink support zone or non-zoone ?
        throw new UnsupportedOperationException(
            "Flink does not support timestamp without time zone fields");
      case STRING:
        return BasicTypeInfo.STRING_TYPE_INFO;
      case UUID:
        // use String
        return BasicTypeInfo.STRING_TYPE_INFO;
      case FIXED:
        return BasicTypeInfo.CHAR_TYPE_INFO;
      case BINARY:
        return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
      case DECIMAL:
        return BasicTypeInfo.BIG_DEC_TYPE_INFO;
      default:
        throw new UnsupportedOperationException(
            "Cannot convert unknown type to Spark: " + primitive);
    }
  }
}
