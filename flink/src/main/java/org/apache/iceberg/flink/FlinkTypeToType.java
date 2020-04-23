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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class FlinkTypeToType extends FlinkTypeVisitor<Type> {
  private final RowTypeInfo root;
  private int nextId = 0;

  FlinkTypeToType(RowTypeInfo root) {
    this.root = root;
    // the root struct's fields use the first ids
    this.nextId = root.getTotalFields();
  }

  @Override
  public Type row(RowTypeInfo typeInfo, List<Type> types) {
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(types.size());
    boolean isRoot = root == typeInfo;
    String[] fieldNames = typeInfo.getFieldNames();
    Preconditions.checkArgument(fieldNames.length == types.size());
    for (int i = 0; i < types.size(); i += 1) {
      Type type = types.get(i);
      int id;
      if (isRoot) {
        id = i;
      } else {
        id = getNextId();
      }
      newFields.add(Types.NestedField.optional(id, fieldNames[i], type, null));
    }
    return Types.StructType.of(newFields);
  }

  @Override
  public Type basicArray(BasicArrayTypeInfo type, Type elementType) {
    return Types.ListType.ofOptional(getNextId(), elementType);
  }

  @Override
  public Type objectArray(ObjectArrayTypeInfo type, Type elementType) {
    return Types.ListType.ofOptional(getNextId(), elementType);
  }

  @Override
  public Type primitiveArray(PrimitiveArrayTypeInfo type, Type elementType) {
    return Types.ListType.ofOptional(getNextId(), elementType);
  }

  @Override
  public Type map(MapTypeInfo type, Type keyType, Type valueType) {
    return Types.MapType.ofOptional(getNextId(), getNextId(), keyType, valueType);
  }

  @Override
  public Type primitive(TypeInformation type) {
    Type.PrimitiveType primitiveType = TYPES.get(type);
    if (primitiveType == null) {
      throw new UnsupportedOperationException("Not a supported type: " + type.toString());
    }
    return primitiveType;
  }

  private int getNextId() {
    int next = nextId;
    nextId += 1;
    return next;
  }

  private static final Map<TypeInformation, Type.PrimitiveType> TYPES = new HashMap<>();

  static {
    TYPES.put(BasicTypeInfo.STRING_TYPE_INFO, Types.StringType.get());
    TYPES.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, Types.BooleanType.get());
    TYPES.put(BasicTypeInfo.BYTE_TYPE_INFO, Types.IntegerType.get());
    TYPES.put(BasicTypeInfo.SHORT_TYPE_INFO, Types.IntegerType.get());
    TYPES.put(BasicTypeInfo.INT_TYPE_INFO, Types.IntegerType.get());
    TYPES.put(BasicTypeInfo.LONG_TYPE_INFO, Types.LongType.get());
    TYPES.put(BasicTypeInfo.FLOAT_TYPE_INFO, Types.FloatType.get());
    TYPES.put(BasicTypeInfo.DOUBLE_TYPE_INFO, Types.DoubleType.get());
    TYPES.put(BasicTypeInfo.CHAR_TYPE_INFO, Types.StringType.get());
    TYPES.put(BasicTypeInfo.DATE_TYPE_INFO, Types.DateType.get());
    TYPES.put(BasicTypeInfo.BIG_DEC_TYPE_INFO, Types.BinaryType.get());
    TYPES.put(BasicTypeInfo.BIG_INT_TYPE_INFO, Types.BinaryType.get());
    TYPES.put(SqlTimeTypeInfo.TIME, Types.TimeType.get());
    TYPES.put(SqlTimeTypeInfo.TIMESTAMP, Types.TimestampType.withZone());
    TYPES.put(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Types.BinaryType.get());
  }
}
