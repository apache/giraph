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
package org.apache.giraph.hive.output;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.hive.types.HiveValueWriter;
import org.apache.giraph.hive.types.HiveVertexIdWriter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

import static org.apache.giraph.hive.types.TypedValueWriter.createValueWriter;
import static org.apache.giraph.hive.types.TypedVertexIdWriter.createIdWriter;

/**
 * A {@link VertexToHive} using
 * {@link org.apache.giraph.types.WritableUnwrapper}s.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public class TypedVertexToHive<I extends WritableComparable, V extends Writable,
    E extends Writable> extends SimpleVertexToHive<I, V, E> {
  /** Source ID column name in Hive */
  public static final StrConfOption VERTEX_ID_COLUMN =
      new StrConfOption("hive.output.vertex.id.column", null,
          "Source Vertex ID column");
  /** Target ID column name in Hive */
  public static final StrConfOption VERTEX_VALUE_COLUMN =
      new StrConfOption("hive.output.vertex.value.column", null,
          "Target Vertex ID column");

  /** Vertex ID writer */
  private HiveVertexIdWriter<I> vertexIdWriter;
  /** Vertex Value writer */
  private HiveValueWriter<V> vertexValueWriter;

  @Override
  public void checkOutput(HiveOutputDescription outputDesc,
      HiveTableSchema schema, HiveWritableRecord emptyRecord) { }

  @Override
  public void initialize() {
    HiveTableSchema schema = getTableSchema();
    ImmutableClassesGiraphConfiguration conf = getConf();

    vertexIdWriter = createIdWriter(conf, VERTEX_ID_COLUMN, schema);
    vertexValueWriter = createValueWriter(conf, VERTEX_VALUE_COLUMN, schema);
  }

  @Override
  public void fillRecord(Vertex<I, V, E> vertex, HiveWritableRecord record) {
    vertexIdWriter.writeId(vertex.getId(), record);
    vertexValueWriter.writeValue(vertex.getValue(), record);
  }
}
