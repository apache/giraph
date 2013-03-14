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
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.facebook.giraph.hive.HiveRecord;
import com.facebook.giraph.hive.HiveTableSchema;
import com.facebook.giraph.hive.HiveTableSchemas;

import java.io.IOException;

/**
 * A reader for reading edges from Hive.
 *
 * @param <I> Vertex ID
 * @param <E> Edge Value
 */
public class HiveEdgeReader<I extends WritableComparable, E extends Writable>
    implements EdgeReader<I, E> {
  /** Configuration key for edge creator class */
  public static final String HIVE_TO_EDGE_KEY = "giraph.hive.to.edge.class";
  /** Configuration key for whether to reuse edge */
  public static final String REUSE_EDGE_KEY = "giraph.hive.reuse.edge";

  /** Underlying Hive RecordReader used */
  private RecordReader<WritableComparable, HiveRecord> hiveRecordReader;
  /** Schema for table in Hive */
  private HiveTableSchema tableSchema;

  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, ?, E, ?> conf;

  /** User class to create edges from a HiveRecord */
  private HiveToEdge<I, E> hiveToEdge;
  /**
   * If we are reusing edges this will be the single edge to read into.
   * Otherwise if it's null we will create a new edge each time.
   */
  private ReusableEdge<I, E> edgeToReuse = null;

  /**
   * Get underlying Hive record reader used.
   *
   * @return RecordReader from Hive
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

  /**
   * Get Hive table schema for table being read from.
   *
   * @return Hive table schema for table
   */
  public HiveTableSchema getTableSchema() {
    return tableSchema;
  }

  /**
   * Set Hive schema for table being read from.
   *
   * @param tableSchema Hive table schema
   */
  public void setTableSchema(HiveTableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  /**
   * Get our Configuration.
   *
   * @return ImmutableClassesGiraphConfiguration
   */
  public ImmutableClassesGiraphConfiguration<I, ?, E, ?> getConf() {
    return conf;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
    hiveRecordReader.initialize(inputSplit, context);
    conf = new ImmutableClassesGiraphConfiguration(context.getConfiguration());
    instantiateHiveToEdgeFromConf();
    if (conf.getBoolean(REUSE_EDGE_KEY, false)) {
      edgeToReuse = conf.createReusableEdge();
    }
  }

  /**
   * Retrieve the user's HiveEdgeCreator from the Configuration.
   *
   * @throws IOException if anything goes wrong reading from Configuration
   */
  private void instantiateHiveToEdgeFromConf() throws IOException {
    Class<? extends HiveToEdge> klass = conf.getClass(HIVE_TO_EDGE_KEY,
        null, HiveToEdge.class);
    if (klass == null) {
      throw new IOException(HIVE_TO_EDGE_KEY + " not set in conf");
    }
    hiveToEdge = ReflectionUtils.newInstance(klass, conf);
    HiveTableSchemas.configure(hiveToEdge, tableSchema);
  }

  @Override
  public boolean nextEdge() throws IOException, InterruptedException {
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
  public I getCurrentSourceId() throws IOException, InterruptedException {
    return hiveToEdge.getSourceVertexId(hiveRecordReader.getCurrentValue());
  }

  @Override
  public Edge<I, E> getCurrentEdge() throws IOException,
      InterruptedException {
    HiveRecord record = hiveRecordReader.getCurrentValue();
    ReusableEdge<I, E> edge = edgeToReuse;
    if (edge == null) {
      edge = conf.createReusableEdge();
    }
    edge.setValue(hiveToEdge.getEdgeValue(record));
    edge.setTargetVertexId(hiveToEdge.getTargetVertexId(record));
    return edge;
  }
}
