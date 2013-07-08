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
import org.apache.giraph.types.WritableWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

import static org.apache.giraph.hive.common.HiveUtils.columnIndexOrThrow;
import static org.apache.giraph.types.WritableWrappers.lookup;

/**
 * Reader for Vertex IDs from Hive with known types
 *
 * @param <I> Vertex ID
 */
public class TypedVertexIdReader<I extends WritableComparable>
    implements HiveVertexIdReader<I> {
  /** Hive column index */
  private final int columnIndex;
  /** {@link WritableWrapper} for Hive column to Giraph Writable */
  private final WritableWrapper<I, Object> writableWrapper;

  /**
   * Constructor
   *
   * @param columnIndex column index
   * @param writableWrapper {@link WritableWrapper}
   */
  public TypedVertexIdReader(int columnIndex,
      WritableWrapper<I, Object> writableWrapper) {
    this.columnIndex = columnIndex;
    this.writableWrapper = writableWrapper;
  }

  /**
   * Create from Configuration with column name and Schema
   *
   * @param conf {@link org.apache.hadoop.conf.Configuration}
   * @param columnOption {@link StrConfOption} for column name
   * @param schema {@link HiveTableSchema}
   * @param <I> Vertex ID
   * @return {@link TypedVertexIdReader}
   */
  public static <I extends WritableComparable> HiveVertexIdReader<I>
  createIdReader(
      ImmutableClassesGiraphConfiguration conf, StrConfOption columnOption,
      HiveTableSchema schema) {
    Class<I> vertexIdClass = conf.getVertexIdClass();
    if (NullWritable.class.isAssignableFrom(vertexIdClass)) {
      return HiveVertexIdReader.Null.get();
    }
    int columnIndex = columnIndexOrThrow(schema, conf, columnOption);
    Class hiveClass = schema.columnType(columnIndex).javaClass();
    WritableWrapper wrapper = lookup(vertexIdClass, hiveClass);
    return new TypedVertexIdReader<I>(columnIndex, wrapper);
  }

  @Override
  public I readId(HiveReadableRecord record) {
    Object object = record.get(columnIndex);
    return writableWrapper.wrap(object);
  }

  public int getColumnIndex() {
    return columnIndex;
  }

  public WritableWrapper<I, Object> getWritableWrapper() {
    return writableWrapper;
  }
}

