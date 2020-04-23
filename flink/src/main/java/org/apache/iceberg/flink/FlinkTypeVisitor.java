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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

public class FlinkTypeVisitor<T> {

  static <T> T visit(TypeInformation<?> type, FlinkTypeVisitor<T> visitor) {
    if (type instanceof RowTypeInfo) {
      RowTypeInfo rowTypeInfo = (RowTypeInfo) type;
      TypeInformation<?>[] types = rowTypeInfo.getFieldTypes();

      List<T> fieldResults = Lists.newArrayListWithExpectedSize(types.length);
      for (int i = 0; i < types.length; i++) {
        fieldResults.add(visit(types[i], visitor));
      }
      return visitor.row(rowTypeInfo, fieldResults);
    } else if (type instanceof BasicArrayTypeInfo) {
      BasicArrayTypeInfo basicArrayTypeInfo = (BasicArrayTypeInfo) type;
      return visitor.basicArray(basicArrayTypeInfo,
          visit(basicArrayTypeInfo.getComponentInfo(), visitor));
    } else if (type instanceof ObjectArrayTypeInfo) {
      ObjectArrayTypeInfo objectArrayTypeInfo = (ObjectArrayTypeInfo) type;
      return visitor.objectArray(objectArrayTypeInfo,
          visit(objectArrayTypeInfo.getComponentInfo(), visitor));
    } else if (type instanceof PrimitiveArrayTypeInfo) {
      PrimitiveArrayTypeInfo primitiveArrayTypeInfo = (PrimitiveArrayTypeInfo) type;
      return visitor.primitiveArray(primitiveArrayTypeInfo,
          visit(primitiveArrayTypeInfo.getComponentType(), visitor));
    } else if (type instanceof MapTypeInfo) {
      MapTypeInfo mapTypeInfo = (MapTypeInfo) type;
      return visitor.map(mapTypeInfo,
          visit(mapTypeInfo.getKeyTypeInfo(), visitor),
          visit(mapTypeInfo.getValueTypeInfo(), visitor));
    } else {
      return visitor.primitive(type);
    }
  }

  public T row(RowTypeInfo type, List<T> fieldResults) {
    return null;
  }

  public T basicArray(BasicArrayTypeInfo type, T elementResult) {
    return null;
  }

  public T objectArray(ObjectArrayTypeInfo type, T elementResult) {
    return null;
  }

  public T primitiveArray(PrimitiveArrayTypeInfo type, T elementResult) {
    return null;
  }

  public T map(MapTypeInfo type, T keyResult, T valueResult) {
    return null;
  }

  public T primitive(TypeInformation type) {
    return null;
  }
}
