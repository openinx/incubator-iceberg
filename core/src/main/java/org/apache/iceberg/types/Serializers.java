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

package org.apache.iceberg.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ByteBuffers;

public class Serializers {

  private Serializers() {
  }

  public static Serializer<StructLike> forType(Types.StructType struct) {
    return new StructLikeSerializer(struct);
  }

  public static <T> Serializer<List<T>> forType(Types.ListType list) {
    return new ListSerializer<>(list);
  }

  public static <T> Serializer<T> forType(Type.PrimitiveType type) {
    return new PrimitiveSerializer<>(type);
  }

  @SuppressWarnings("unchecked")
  private static <T> Serializer<T> internal(Type type) {
    if (type.isPrimitiveType()) {
      return forType(type.asPrimitiveType());
    } else if (type.isStructType()) {
      return (Serializer<T>) forType(type.asStructType());
    } else if (type.isListType()) {
      return (Serializer<T>) forType(type.asListType());
    }
    throw new UnsupportedOperationException("Cannot determine serializer for type: " + type);
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> internalClass(Type type) {
    if (type.isPrimitiveType()) {
      return (Class<T>) type.typeId().javaClass();
    } else if (type.isStructType()) {
      return (Class<T>) StructLike.class;
    } else if (type.isListType()) {
      return (Class<T>) List.class;
    } else if (type.isMapType()) {
      return (Class<T>) Map.class;
    }

    throw new UnsupportedOperationException("Cannot determine expected class for type: " + type);
  }

  private static class StructLikeSerializer implements Serializer<StructLike> {
    private final Types.StructType struct;
    private final Serializer<Object>[] serializers;
    private final Class<?>[] classes;

    private StructLikeSerializer(Types.StructType struct) {
      this.struct = struct;
      this.serializers = struct.fields().stream()
          .map(field -> internal(field.type()))
          .toArray((IntFunction<Serializer<Object>[]>) Serializer[]::new);
      this.classes = struct.fields().stream()
          .map(field -> internalClass(field.type()))
          .toArray(Class<?>[]::new);
    }

    @Override
    public byte[] serialize(StructLike object) {
      if (object == null) {
        return new byte[0];
      }

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      for (int i = 0; i < serializers.length; i += 1) {
        Class<?> valueClass = classes[i];

        byte[] fieldData = serializers[i].serialize(object.get(i, valueClass));
        out.write(fieldData.length);

        try {
          out.write(fieldData);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      return out.toByteArray();
    }

    @Override
    public StructLike deserialize(byte[] data) {
      if (data == null || data.length == 0) {
        return null;
      }

      GenericRecord record = GenericRecord.create(struct);
      ByteArrayInputStream in = new ByteArrayInputStream(data);

      for (int i = 0; i < serializers.length; i += 1) {
        int length = in.read();
        byte[] fieldData = new byte[length];

        record.set(i, serializers[i].deserialize(fieldData));
      }

      return record;
    }
  }

  private static class ListSerializer<T> implements Serializer<List<T>> {
    private final Serializer<T> elementSerializer;

    private ListSerializer(Types.ListType list) {
      this.elementSerializer = internal(list.elementType());
    }

    @Override
    public byte[] serialize(List<T> object) {
      if (object == null) {
        return new byte[0];
      }

      try (ByteArrayOutputStream out = new ByteArrayOutputStream();
           DataOutputStream dos = new DataOutputStream(out)) {

        dos.writeInt(object.size());
        for (T elem : object) {
          byte[] data = elementSerializer.serialize(elem);
          dos.writeInt(data.length);
          dos.write(data);
        }

        return out.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public List<T> deserialize(byte[] data) {
      if (data == null || data.length == 0) {
        return null;
      }

      try (ByteArrayInputStream in = new ByteArrayInputStream(data);
           DataInputStream dis = new DataInputStream(in)) {

        int size = dis.readInt();
        List<T> result = Lists.newArrayListWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
          byte[] fieldData = new byte[dis.readInt()];
          Preconditions.checkState(fieldData.length == dis.read(fieldData));
          result.add(elementSerializer.deserialize(fieldData));
        }

        return result;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class PrimitiveSerializer<T> implements Serializer<T> {

    private final Type.PrimitiveType type;

    private PrimitiveSerializer(Type.PrimitiveType type) {
      this.type = type;
    }

    @Override
    public byte[] serialize(Object object) {
      return ByteBuffers.toByteArray(Conversions.toByteBuffer(type, object));
    }

    @Override
    public T deserialize(byte[] data) {
      return Conversions.fromByteBuffer(type, ByteBuffer.wrap(data));
    }
  }
}
