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
package org.apache.giraph.hive.input.vertex;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.hive.types.HiveValueReader;
import org.apache.giraph.hive.types.HiveVertexIdReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;

import static org.apache.giraph.hive.types.TypedValueReader.createValueReader;
import static org.apache.giraph.hive.types.TypedVertexIdReader.createIdReader;

/**
 * A {@link HiveToVertex} using {@link org.apache.giraph.types.WritableWrapper}
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public class TypedHiveToVertex<I extends WritableComparable, V extends Writable,
    E extends Writable> extends SimpleHiveToVertex<I, V, E> {
  /** Source ID column name in Hive */
  public static final StrConfOption VERTEX_ID_COLUMN =
      new StrConfOption("hive.input.vertex.id.column", null,
          "Vertex ID column");
  /** Target ID column name in Hive */
  public static final StrConfOption VERTEX_VALUE_COLUMN =
      new StrConfOption("hive.input.vertex.value.column", null,
          "Vertex Value column");
  /** Edge Value column name in Hive */
  public static final StrConfOption EDGES_COLUMN =
      new StrConfOption("hive.input.vertex.edges.column", null,
          "Edges column");

  /** Vertex ID reader */
  private HiveVertexIdReader<I> vertexIdReader;
  /** Vertex Value reader */
  private HiveValueReader<V> vertexValueReader;

  @Override
  public void checkInput(HiveInputDescription inputDesc,
      HiveTableSchema schema) { }

  @Override
  public void initializeRecords(Iterator<HiveReadableRecord> records) {
    super.initializeRecords(records);

    HiveTableSchema schema = getTableSchema();
    ImmutableClassesGiraphConfiguration conf = getConf();

    vertexIdReader = createIdReader(conf, VERTEX_ID_COLUMN, schema);
    vertexValueReader = createValueReader(conf, VERTEX_VALUE_COLUMN, schema);
  }

  @Override
  public Iterable<Edge<I, E>> getEdges(HiveReadableRecord record) {
    return ImmutableList.of();
  }

  @Override
  public I getVertexId(HiveReadableRecord record) {
    return vertexIdReader.readId(record);
  }

  @Override
  public V getVertexValue(HiveReadableRecord record) {
    return vertexValueReader.readValue(record);
  }
}
