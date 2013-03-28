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
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.hive.input.RecordReaderWrapper;
import org.apache.giraph.io.iterables.GiraphReader;
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
    implements GiraphReader<Vertex<I, V, E, M>>, HiveTableSchemaAware {
  /** Configuration key for {@link HiveToVertex} class */
  public static final String HIVE_TO_VERTEX_KEY =
      "giraph.hive.to.vertex.class";
  /** Underlying Hive RecordReader used */
  private HiveApiRecordReader hiveRecordReader;
  /** Schema for table in Hive */
  private HiveTableSchema tableSchema;

  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, V, E, M> conf;

  /**
   * {@link HiveToVertex} chosen by user,
   * or {@link SimpleHiveToVertex} if none specified
   */
  private HiveToVertex<I, V, E, M> hiveToVertex;

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
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    hiveRecordReader.initialize(inputSplit, context);
    conf = new ImmutableClassesGiraphConfiguration<I, V, E,
        M>(context.getConfiguration());
    Class<? extends HiveToVertex> klass = conf.getClass(HIVE_TO_VERTEX_KEY,
        SimpleHiveToVertex.class, HiveToVertex.class);
    hiveToVertex = ReflectionUtils.newInstance(klass, conf);
    HiveTableSchemas.configure(hiveToVertex, tableSchema);
    hiveToVertex.initializeRecords(
        new RecordReaderWrapper<HiveRecord>(hiveRecordReader));
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
  public boolean hasNext() {
    return hiveToVertex.hasNext();
  }

  @Override
  public Vertex<I, V, E, M> next() {
    return hiveToVertex.next();
  }

  @Override
  public void remove() {
    hiveToVertex.remove();
  }
}
