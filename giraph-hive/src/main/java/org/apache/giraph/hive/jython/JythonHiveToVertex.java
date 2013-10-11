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
package org.apache.giraph.hive.jython;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.GraphType;
import org.apache.giraph.hive.common.GiraphHiveConstants;
import org.apache.giraph.hive.input.vertex.SimpleHiveToVertex;
import org.apache.giraph.hive.values.HiveValueReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;

/**
 * A {@link org.apache.giraph.hive.input.vertex.HiveToVertex} that writes each
 * part (vertex ID, vertex value, edges) using separate writers.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public class JythonHiveToVertex<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends SimpleHiveToVertex<I, V, E> {
  /** Source ID column name in Hive */
  public static final StrConfOption VERTEX_ID_COLUMN =
      new StrConfOption("hive.input.vertex.id.column", null,
          "Vertex ID column");
  /** Target ID column name in Hive */
  public static final StrConfOption VERTEX_VALUE_COLUMN =
      new StrConfOption("hive.input.vertex.value.column", null,
          "Vertex Value column");

  /** Vertex ID reader */
  private HiveValueReader<I> vertexIdReader;
  /** Vertex Value reader */
  private HiveValueReader<V> vertexValueReader;

  @Override
  public void checkInput(HiveInputDescription inputDesc,
      HiveTableSchema schema) { }

  @Override
  public void initializeRecords(Iterator<HiveReadableRecord> records) {
    super.initializeRecords(records);

    HiveTableSchema schema = getTableSchema();
    ImmutableClassesGiraphConfiguration<I, V, E> conf = getConf();

    vertexIdReader = HiveJythonUtils.newValueReader(schema, VERTEX_ID_COLUMN,
        conf, GraphType.VERTEX_ID,
        GiraphHiveConstants.VERTEX_ID_READER_JYTHON_NAME);
    vertexValueReader = HiveJythonUtils.newValueReader(schema,
        VERTEX_VALUE_COLUMN, conf, GraphType.VERTEX_VALUE,
        GiraphHiveConstants.VERTEX_VALUE_READER_JYTHON_NAME);
  }

  @Override
  public Iterable<Edge<I, E>> getEdges(HiveReadableRecord record) {
    return ImmutableList.of();
  }

  @Override
  public I getVertexId(HiveReadableRecord record) {
    I vertexId = getReusableVertexId();
    vertexIdReader.readFields(vertexId, record);
    return vertexId;
  }

  @Override
  public V getVertexValue(HiveReadableRecord record) {
    V vertexValue = getReusableVertexValue();
    vertexValueReader.readFields(vertexValue, record);
    return vertexValue;
  }
}
