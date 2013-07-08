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
package org.apache.giraph.hive.input.edge;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.hive.types.HiveValueReader;
import org.apache.giraph.hive.types.HiveVertexIdReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

import java.util.Iterator;

import static org.apache.giraph.hive.types.TypedValueReader.createValueReader;
import static org.apache.giraph.hive.types.TypedVertexIdReader.createIdReader;

/**
 * A {@link HiveToEdge} using {@link org.apache.giraph.types.WritableWrapper}s
 *
 * @param <I> Vertex ID
 * @param <E> Edge Value
 */
public class TypedHiveToEdge<I extends WritableComparable, E extends Writable>
    extends SimpleHiveToEdge<I, E> {
  /** Source ID column name in Hive */
  public static final StrConfOption EDGE_SOURCE_ID_COLUMN =
      new StrConfOption("hive.input.edge.source.id.column", null,
          "Source Vertex ID column");
  /** Target ID column name in Hive */
  public static final StrConfOption EDGE_TARGET_ID_COLUMN =
      new StrConfOption("hive.input.edge.target.id.column", null,
          "Target Vertex ID column");
  /** Edge Value column name in Hive */
  public static final StrConfOption EDGE_VALUE_COLUMN =
      new StrConfOption("hive.input.edge.value.column", null,
          "Edge Value column");

  /** Source ID reader */
  private HiveVertexIdReader<I> sourceIdReader;
  /** Target ID reader */
  private HiveVertexIdReader<I> targetIdReader;
  /** Edge Value reader */
  private HiveValueReader<E> vertexValueReader;

  @Override
  public void checkInput(HiveInputDescription inputDesc,
      HiveTableSchema schema) { }

  @Override
  public void initializeRecords(Iterator<HiveReadableRecord> records) {
    super.initializeRecords(records);

    HiveTableSchema schema = getTableSchema();
    ImmutableClassesGiraphConfiguration conf = getConf();

    sourceIdReader = createIdReader(conf, EDGE_SOURCE_ID_COLUMN, schema);
    targetIdReader = createIdReader(conf, EDGE_TARGET_ID_COLUMN, schema);
    vertexValueReader = createValueReader(conf, EDGE_VALUE_COLUMN, schema);
  }

  @Override
  public E getEdgeValue(HiveReadableRecord record) {
    return vertexValueReader.readValue(record);
  }

  @Override
  public I getSourceVertexId(
      HiveReadableRecord record) {
    return sourceIdReader.readId(record);
  }

  @Override
  public I getTargetVertexId(
      HiveReadableRecord record) {
    return targetIdReader.readId(record);
  }
}

