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

import org.apache.giraph.io.iterables.EdgeWithSource;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.record.HiveReadableRecord;

import java.util.Iterator;

/**
 * Simple implementation of {@link HiveToEdge} when each edge is in the one
 * row of the input.
 *
 * @param <I> Vertex id
 * @param <E> Edge data
 */
public abstract class SimpleHiveToEdge<I extends WritableComparable,
    E extends Writable> extends AbstractHiveToEdge<I, E> {
  /** Iterator over input records */
  private Iterator<HiveReadableRecord> records;
  /** Reusable {@link EdgeWithSource} object */
  private EdgeWithSource<I, E> reusableEdge = new EdgeWithSource<I, E>();

  /**
   * Read source vertex ID from Hive record
   *
   * @param hiveRecord HiveRecord to read from
   * @return source vertex ID
   */
  public abstract I getSourceVertexId(HiveReadableRecord hiveRecord);

  /**
   * Read target vertex ID from Hive record
   *
   * @param hiveRecord HiveRecord to read from
   * @return target vertex ID
   */
  public abstract I getTargetVertexId(HiveReadableRecord hiveRecord);

  /**
   * Read edge value from the Hive record.
   *
   * @param hiveRecord HiveRecord to read from
   * @return Edge value
   */
  public abstract E getEdgeValue(HiveReadableRecord hiveRecord);

  @Override
  public void initializeRecords(Iterator<HiveReadableRecord> records) {
    this.records = records;
    reusableEdge.setSourceVertexId(getConf().createVertexId());
    reusableEdge.setEdge(getConf().createReusableEdge());
  }

  @Override
  public boolean hasNext() {
    return records.hasNext();
  }

  @Override
  public EdgeWithSource<I, E> next() {
    HiveReadableRecord record = records.next();
    reusableEdge.setSourceVertexId(getSourceVertexId(record));
    reusableEdge.setTargetVertexId(getTargetVertexId(record));
    reusableEdge.setEdgeValue(getEdgeValue(record));
    return reusableEdge;
  }

  protected I getReusableSourceVertexId() {
    return reusableEdge.getSourceVertexId();
  }

  protected I getReusableTargetVertexId() {
    return reusableEdge.getTargetVertexId();
  }

  protected E getReusableEdgeValue() {
    return reusableEdge.getEdgeValue();
  }
}

