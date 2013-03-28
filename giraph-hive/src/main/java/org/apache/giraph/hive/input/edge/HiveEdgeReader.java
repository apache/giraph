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
import org.apache.giraph.hive.input.RecordReaderWrapper;
import org.apache.giraph.io.iterables.EdgeWithSource;
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

import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_TO_EDGE_CLASS;

/**
 * A reader for reading edges from Hive.
 *
 * @param <I> Vertex ID
 * @param <E> Edge Value
 */
public class HiveEdgeReader<I extends WritableComparable, E extends Writable>
    implements GiraphReader<EdgeWithSource<I, E>>, HiveTableSchemaAware {
  /** Configuration key for edge creator class */
  public static final String HIVE_TO_EDGE_KEY = "giraph.hive.to.edge.class";

  /** Underlying Hive RecordReader used */
  private RecordReader<WritableComparable, HiveReadableRecord> hiveRecordReader;
  /** Schema for table in Hive */
  private HiveTableSchema tableSchema;

  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, ?, E, ?> conf;

  /** User class to create edges from a HiveRecord */
  private HiveToEdge<I, E> hiveToEdge;

  /**
   * Get underlying Hive record reader used.
   *
   * @return RecordReader from Hive
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
  public ImmutableClassesGiraphConfiguration<I, ?, E, ?> getConf() {
    return conf;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
    throws IOException, InterruptedException {
    hiveRecordReader.initialize(inputSplit, context);
    conf = new ImmutableClassesGiraphConfiguration(context.getConfiguration());
    instantiateHiveToEdgeFromConf();
    hiveToEdge.initializeRecords(
        new RecordReaderWrapper<HiveReadableRecord>(hiveRecordReader));
  }

  /**
   * Retrieve the user's {@link HiveToEdge} from the Configuration.
   *
   * @throws IOException if anything goes wrong reading from Configuration
   */
  private void instantiateHiveToEdgeFromConf() throws IOException {
    Class<? extends HiveToEdge> klass = HIVE_TO_EDGE_CLASS.get(conf);
    if (klass == null) {
      throw new IOException(HIVE_TO_EDGE_CLASS.getKey() + " not set in conf");
    }
    hiveToEdge = ReflectionUtils.newInstance(klass, conf);
    HiveTableSchemas.configure(hiveToEdge, tableSchema);
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
    return hiveToEdge.hasNext();
  }

  @Override
  public EdgeWithSource<I, E> next() {
    return hiveToEdge.next();
  }

  @Override
  public void remove() {
    hiveToEdge.remove();
  }
}
