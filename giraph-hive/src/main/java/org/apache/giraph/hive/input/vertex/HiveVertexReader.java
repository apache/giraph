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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.hive.common.DefaultConfigurableAndTableSchemaAware;
import org.apache.giraph.hive.common.HiveUtils;
import org.apache.giraph.hive.input.RecordReaderWrapper;
import org.apache.giraph.io.iterables.GiraphReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.facebook.hiveio.record.HiveReadableRecord;

import java.io.IOException;

/**
 * VertexReader using Hive
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public class HiveVertexReader<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends DefaultConfigurableAndTableSchemaAware<I, V, E>
    implements GiraphReader<Vertex<I, V, E>> {
  /** Underlying Hive RecordReader used */
  private RecordReader<WritableComparable, HiveReadableRecord> hiveRecordReader;

  /**
   * {@link HiveToVertex} chosen by user,
   * or {@link SimpleHiveToVertex} if none specified
   */
  private HiveToVertex<I, V, E> hiveToVertex;

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
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    hiveRecordReader.initialize(inputSplit, context);
    hiveToVertex = HiveUtils.newHiveToVertex(getConf(), getTableSchema());
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
  public Vertex<I, V, E> next() {
    return hiveToVertex.next();
  }

  @Override
  public void remove() {
    hiveToVertex.remove();
  }
}
