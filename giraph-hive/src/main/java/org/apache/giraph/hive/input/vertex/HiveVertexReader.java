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
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.facebook.giraph.hive.HiveRecord;
import com.facebook.giraph.hive.HiveTableSchema;
import com.facebook.giraph.hive.HiveTableSchemaAware;

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
  /** Key in Configuration for HiveToVertex class */
  public static final String HIVE_TO_VERTEX_KEY = "giraph.hive.to.vertex.class";

  /** Underlying Hive RecordReader used */
  private RecordReader<WritableComparable, HiveRecord> hiveRecordReader;
  /** Schema for table in Hive */
  private HiveTableSchema tableSchema;

  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, V, E, M> conf;

  /** User class to create vertices from a HiveRecord */
  private HiveToVertex<I, V, E> hiveToVertex;

  /**
   * Get underlying Hive record reader used.
   *
   * @return RecordReader from Hive.
   */
  public RecordReader<WritableComparable, HiveRecord> getHiveRecordReader() {
    return hiveRecordReader;
  }

  /**
   * Set underlying Hive record reader used.
   *
   * @param hiveRecordReader RecordReader to read from Hive.
   */
  public void setHiveRecordReader(
      RecordReader<WritableComparable, HiveRecord> hiveRecordReader) {
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
    instantiateHiveToVertexFromConf();
  }

  /**
   * Retrieve the user's HiveVertexCreator from our configuration.
   *
   * @throws IOException if anything goes wrong reading from Configuration.
   */
  private void instantiateHiveToVertexFromConf() throws IOException {
    Class<? extends HiveToVertex> klass = conf.getClass(HIVE_TO_VERTEX_KEY,
        null, HiveToVertex.class);
    if (klass == null) {
      throw new IOException(HIVE_TO_VERTEX_KEY + " not set in conf");
    }
    hiveToVertex = ReflectionUtils.newInstance(klass, conf);
    hiveToVertex.setTableSchema(tableSchema);
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
    Vertex vertex = conf.createVertex();
    hiveToVertex.fillVertex(hiveRecord, vertex);
    return vertex;
  }
}
