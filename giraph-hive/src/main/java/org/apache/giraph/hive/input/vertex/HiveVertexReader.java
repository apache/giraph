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
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.facebook.giraph.hive.HiveRecord;
import com.facebook.giraph.hive.HiveTableSchema;
import com.facebook.giraph.hive.HiveTableSchemaAware;
import com.facebook.giraph.hive.HiveTableSchemas;
import com.facebook.giraph.hive.impl.input.HiveApiRecordReader;

import java.io.IOException;

/**
 * VertexReader using Hive
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 * @param <M> Message Value
 */
public class HiveVertexReader<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements VertexReader<I, V, E, M>, HiveTableSchemaAware {
  /** Configuration key for {@link HiveToVertexValue} class */
  public static final String HIVE_TO_VERTEX_KEY =
      "giraph.hive.to.vertex.value.class";
  /** Configuration key for {@link HiveToVertexEdges} class */
  public static final String HIVE_TO_VERTEX_EDGES_KEY =
      "giraph.hive.to.vertex.edges.class";
  /** Configuration key for whether to reuse vertex */
  public static final String REUSE_VERTEX_KEY = "giraph.hive.reuse.vertex";

  /** Underlying Hive RecordReader used */
  private HiveApiRecordReader hiveRecordReader;
  /** Schema for table in Hive */
  private HiveTableSchema tableSchema;

  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, V, E, M> conf;

  /** User class to create vertices from a HiveRecord */
  private HiveToVertexValue<I, V> hiveToVertexValue;
  /** User class to create vertex edges from HiveRecord - optional */
  private HiveToVertexEdges<I, E> hiveToVertexEdges;

  /**
   * If we are reusing vertices this will be the single vertex to read into.
   * Otherwise if it's null we will create a new vertex each time.
   */
  private Vertex<I, V, E, M> vertexToReuse = null;

  /**
   * Get underlying Hive record reader used.
   *
   * @return RecordReader from Hive.
   */
  public HiveApiRecordReader getHiveRecordReader() {
    return hiveRecordReader;
  }

  /**
   * Set underlying Hive record reader used.
   *
   * @param hiveRecordReader RecordReader to read from Hive.
   */
  public void setHiveRecordReader(HiveApiRecordReader hiveRecordReader) {
    this.hiveRecordReader = hiveRecordReader;
  }

  @Override
  public HiveTableSchema getTableSchema() {
    return tableSchema;
  }

  @Override
  public void setTableSchema(HiveTableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  /**
   * Get our Configuration.
   *
   * @return ImmutableClassesGiraphConfiguration
   */
  public ImmutableClassesGiraphConfiguration<I, V, E, M> getConf() {
    return conf;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
    hiveRecordReader.initialize(inputSplit, context);
    conf = new ImmutableClassesGiraphConfiguration(context.getConfiguration());
    instantiateHiveToVertexValueFromConf();
    instantiateHiveToVertexEdgesFromConf();
    if (conf.getBoolean(REUSE_VERTEX_KEY, false)) {
      vertexToReuse = conf.createVertex();
    }
  }

  /**
   * Retrieve the user's HiveToVertex from our configuration.
   *
   * @throws IOException if anything goes wrong reading from Configuration.
   */
  private void instantiateHiveToVertexValueFromConf() throws IOException {
    Class<? extends HiveToVertexValue> klass = conf.getClass(HIVE_TO_VERTEX_KEY,
        null, HiveToVertexValue.class);
    if (klass == null) {
      throw new IOException(HIVE_TO_VERTEX_KEY + " not set in conf");
    }
    hiveToVertexValue = ReflectionUtils.newInstance(klass, conf);
    HiveTableSchemas.configure(hiveToVertexValue, tableSchema);
  }

  /**
   * Retrieve the user's HiveToVertexEdges from our configuration. This class
   * is optional. If not specified will just use HiveToVertexEdges.Empty.
   */
  private void instantiateHiveToVertexEdgesFromConf() {
    Class<? extends HiveToVertexEdges> klass = conf.getClass(
        HIVE_TO_VERTEX_EDGES_KEY, null, HiveToVertexEdges.class);
    if (klass == null) {
      hiveToVertexEdges = HiveToVertexEdges.Empty.get();
    } else {
      hiveToVertexEdges = ReflectionUtils.newInstance(klass, conf);
    }
    HiveTableSchemas.configure(hiveToVertexEdges, tableSchema);
  }

  @Override
  public boolean nextVertex() throws IOException, InterruptedException {
    return hiveRecordReader.nextKeyValue();
  }

  @Override
  public void close() throws IOException {
    hiveRecordReader.close();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return hiveRecordReader.getProgress();
  }

  @Override
  public final Vertex<I, V, E, M> getCurrentVertex()
    throws IOException, InterruptedException {
    HiveRecord hiveRecord = hiveRecordReader.getCurrentValue();
    Vertex<I, V, E, M> vertex = vertexToReuse;
    if (vertex == null) {
      vertex = conf.createVertex();
    }
    I id = hiveToVertexValue.getVertexId(hiveRecord);
    V value = hiveToVertexValue.getVertexValue(hiveRecord);
    Iterable<Edge<I, E>> edges = hiveToVertexEdges.getEdges(hiveRecord);
    vertex.initialize(id, value, edges);
    return vertex;
  }
}
