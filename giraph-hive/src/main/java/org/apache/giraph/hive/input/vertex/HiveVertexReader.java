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
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.facebook.giraph.hive.record.HiveReadableRecord;
import com.facebook.giraph.hive.schema.HiveTableSchema;
import com.facebook.giraph.hive.schema.HiveTableSchemaAware;
import com.facebook.giraph.hive.schema.HiveTableSchemas;

import java.io.IOException;

import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_TO_VERTEX_CLASS;

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
  /** Underlying Hive RecordReader used */
  private RecordReader<WritableComparable, HiveReadableRecord> hiveRecordReader;
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
  public RecordReader<WritableComparable, HiveReadableRecord>
  getHiveRecordReader() {
    return hiveRecordReader;
  }

  /**
   * Set underlying Hive record reader used.
   *
   * @param hiveRecordReader RecordReader to read from Hive.
   */
  public void setHiveRecordReader(
      RecordReader<WritableComparable, HiveReadableRecord> hiveRecordReader) {
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
    conf = new ImmutableClassesGiraphConfiguration<I, V, E, M>(
        context.getConfiguration());
    Class<? extends HiveToVertex> klass = HIVE_TO_VERTEX_CLASS.get(conf);
    hiveToVertex = ReflectionUtils.newInstance(klass, conf);
    HiveTableSchemas.configure(hiveToVertex, tableSchema);
    hiveToVertex.initializeRecords(
        new RecordReaderWrapper<HiveReadableRecord>(hiveRecordReader));
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
