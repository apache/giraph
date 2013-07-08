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
import org.apache.hadoop.io.Writable;

import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

import static org.apache.giraph.hive.common.HiveUtils.columnIndexOrThrow;
import static org.apache.giraph.types.WritableUnwrappers.lookup;

/**
 * Writer for Vertex/Edge Values from Hive with known types
 *
 * @param <W> Vertex/Edge Value
 */
public class TypedValueWriter<W extends Writable>
    implements HiveValueWriter<W> {
  /** Hive column index */
  private final int columnIndex;
  /** {@link WritableUnwrapper} for Hive column to Giraph Writable */
  private final WritableUnwrapper<W, Object> writableUnwrapper;

  /**
   * Constructor
   *
   * @param columnIndex column index
   * @param writableUnwrapper JavaWritableConverter
   */
  public TypedValueWriter(int columnIndex,
      WritableUnwrapper<W, Object> writableUnwrapper) {
    this.columnIndex = columnIndex;
    this.writableUnwrapper = writableUnwrapper;
  }

  /**
   * Create from Configuration with column name and Schema
   *
   * @param conf Configuration
   * @param columnOption StrConfOption for column name
   * @param schema HiveTableSchema
   * @param <V> Vertex/Edge Value
   * @return TypedVertexValueReader
   */
  public static <V extends Writable> HiveValueWriter<V>
  createValueWriter(
        ImmutableClassesGiraphConfiguration conf, StrConfOption columnOption,
        HiveTableSchema schema) {
    Class<V> vertexValueClass = conf.getVertexValueClass();
    if (NullWritable.class.isAssignableFrom(vertexValueClass)) {
      return HiveValueWriter.Null.get();
    }
    int columnIndex = columnIndexOrThrow(schema, conf, columnOption);
    Class hiveClass = schema.columnType(columnIndex).javaClass();
    WritableUnwrapper unwrapper = lookup(vertexValueClass, hiveClass);
    return new TypedValueWriter(columnIndex, unwrapper);
  }

  @Override
  public void writeValue(W value, HiveWritableRecord record) {
    Object object = writableUnwrapper.unwrap(value);
    record.set(columnIndex, object);
  }
}
