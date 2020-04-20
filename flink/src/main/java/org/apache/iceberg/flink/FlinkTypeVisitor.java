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
import org.apache.flink.api.common.typeinfo.TypeInformation;


public class FlinkTypeVisitor<T> {

  static <T> T visit(TypeInformation<?> type, FlinkTypeVisitor<T> visitor) {
    throw new UnsupportedOperationException("TODO: implement this method");
  }

  public T struct(TypeInformation<?> type, List<T> fieldResults) {
    return null;
  }

  public T field(TypeInformation<?> type, T typeResult) {
    return null;
  }

  public T array(TypeInformation<?> type, T elementResult) {
    return null;
  }

  public T map(TypeInformation<?> type, T keyResult, T valueResult) {
    return null;
  }

  public T atomic(TypeInformation<?> type) {
    return null;
  }
}
