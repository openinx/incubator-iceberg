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

import java.util.Arrays;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.transforms.Transform;

public class PartitionKey implements StructLike {

  private final Object[] partitionTuple;

  private PartitionKey(Object[] partitionTuple) {
    this.partitionTuple = partitionTuple;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionKey)) {
      return false;
    }

    PartitionKey that = (PartitionKey) o;
    return Arrays.equals(partitionTuple, that.partitionTuple);
  }

  public Object[] getPartitionTuple() {
    return this.partitionTuple;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(partitionTuple);
  }

  @Override
  public int size() {
    return partitionTuple.length;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(partitionTuple[pos]);
  }

  @Override
  public <T> void set(int pos, T value) {
    partitionTuple[pos] = value;
  }

  public static class Builder {
    private final int size;
    private final Transform[] transforms;

    @SuppressWarnings("unchecked")
    Builder(PartitionSpec spec) {
      List<PartitionField> fields = spec.fields();
      this.size = fields.size();
      this.transforms = new Transform[size];

      for (int i = 0; i < size; i += 1) {
        PartitionField field = fields.get(i);
        this.transforms[i] = field.transform();
      }
    }

    public PartitionKey build(Row row) {
      Object[] partitionTuple = new Object[size];
      for (int i = 0; i < partitionTuple.length; i += 1) {
        Transform<Object, Object> transform = transforms[i];
        partitionTuple[i] = transform.apply(row.getField(i));
      }
      return new PartitionKey(partitionTuple);
    }
  }
}
