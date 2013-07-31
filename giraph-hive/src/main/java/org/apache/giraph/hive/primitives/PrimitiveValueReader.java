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
package org.apache.giraph.hive.primitives;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.graph.GraphType;
import org.apache.giraph.hive.values.HiveValueReader;
import org.apache.giraph.types.WritableWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Preconditions;

import static org.apache.giraph.hive.common.HiveUtils.columnIndexOrThrow;
import static org.apache.giraph.types.WritableWrappers.lookup;

/**
 * Reader for Vertex/Edge Values from Hive with known types
 *
 * @param <W> Vertex/Edge Value
 */
public class PrimitiveValueReader<W extends Writable>
    implements HiveValueReader<W> {
  /** Hive column index */
  private final int columnIndex;
  /** {@link WritableWrapper} for Hive column to Giraph Writable */
  private final WritableWrapper<W, Object> writableWrapper;

  /**
   * Constructor
   *
   * @param columnIndex column index
   * @param writableWrapper {@link WritableWrapper}
   */
  public PrimitiveValueReader(int columnIndex,
      WritableWrapper<W, Object> writableWrapper) {
    Preconditions.checkNotNull(writableWrapper);
    this.columnIndex = columnIndex;
    this.writableWrapper = writableWrapper;
  }

  /**
   * Create from Configuration with column name and Schema
   *
   * @param conf Configuration
   * @param graphType GraphType
   * @param columnOption StrConfOption for column name
   * @param schema HiveTableSchema
   * @param <W> Graph type
   * @return TypedVertexValueReader
   */
  public static <W extends Writable> HiveValueReader<W> create(
      ImmutableClassesGiraphConfiguration conf, GraphType graphType,
      StrConfOption columnOption, HiveTableSchema schema) {
    Class<W> valueClass = graphType.get(conf);
    if (NullWritable.class.isAssignableFrom(valueClass)) {
      return HiveValueReader.Null.get();
    }
    int columnIndex = columnIndexOrThrow(schema, conf, columnOption);
    Class hiveClass = schema.columnType(columnIndex).javaClass();
    WritableWrapper wrapper = lookup(valueClass, hiveClass);
    return new PrimitiveValueReader(columnIndex, wrapper);
  }

  @Override
  public void readFields(W value, HiveReadableRecord record) {
    Object object = record.get(columnIndex);
    writableWrapper.wrap(object, value);
  }
}
