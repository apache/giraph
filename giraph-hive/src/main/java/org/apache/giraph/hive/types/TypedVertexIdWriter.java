/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.hive.types;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.types.WritableUnwrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

import static org.apache.giraph.hive.common.HiveUtils.columnIndexOrThrow;
import static org.apache.giraph.types.WritableUnwrappers.lookup;

/**
 * Writer for Vertex IDs from Hive with known types
 *
 * @param <I> Vertex ID
 */
public class TypedVertexIdWriter<I extends WritableComparable>
    implements HiveVertexIdWriter<I> {
  /** Hive column index */
  private final int columnIndex;
  /** {@link WritableUnwrapper} for Hive column to Giraph Writable */
  private final WritableUnwrapper<I, Object> writableUnwrapper;

  /**
   * Constructor
   *
   * @param columnIndex column index
   * @param writableUnwrapper {@link WritableUnwrapper}
   */
  public TypedVertexIdWriter(int columnIndex,
      WritableUnwrapper<I, Object> writableUnwrapper) {
    this.columnIndex = columnIndex;
    this.writableUnwrapper = writableUnwrapper;
  }

  /**
   * Create from Configuration with column name and Schema
   *
   * @param conf {@link org.apache.hadoop.conf.Configuration}
   * @param columnOption {@link StrConfOption} for column name
   * @param schema {@link HiveTableSchema}
   * @param <I> Vertex ID
   * @return {@link TypedVertexIdWriter}
   */
  public static <I extends WritableComparable> HiveVertexIdWriter<I>
  createIdWriter(
      ImmutableClassesGiraphConfiguration conf, StrConfOption columnOption,
      HiveTableSchema schema) {
    Class<I> vertexIdClass = conf.getVertexIdClass();
    if (NullWritable.class.isAssignableFrom(vertexIdClass)) {
      return HiveVertexIdWriter.Null.get();
    }
    int columnIndex = columnIndexOrThrow(schema, conf, columnOption);
    Class hiveClass = schema.columnType(columnIndex).javaClass();
    WritableUnwrapper unwrapper = lookup(vertexIdClass, hiveClass);
    return new TypedVertexIdWriter<I>(columnIndex, unwrapper);
  }

  @Override
  public void writeId(I value, HiveWritableRecord record) {
    Object object = writableUnwrapper.unwrap(value);
    record.set(columnIndex, object);
  }
}
