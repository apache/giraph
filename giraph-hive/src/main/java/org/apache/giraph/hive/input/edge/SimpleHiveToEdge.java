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
import org.apache.giraph.io.iterables.EdgeWithSource;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.giraph.hive.HiveReadableRecord;
import com.facebook.giraph.hive.HiveRecord;

import java.util.Iterator;

/**
 * Simple implementation of {@link HiveToEdge} when each edge is in the one
 * row of the input.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public abstract class SimpleHiveToEdge<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends AbstractHiveToEdge<I, V, E, M> {
  /** Iterator over input records */
  private Iterator<HiveRecord> records;
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
  public void setConf(ImmutableClassesGiraphConfiguration<I, V, E, M> conf) {
    super.setConf(conf);
    reusableEdge.setEdge(getConf().createReusableEdge());
  }

  @Override
  public final void initializeRecords(Iterator<HiveRecord> records) {
    this.records = records;
  }

  @Override
  public boolean hasNext() {
    return records.hasNext();
  }

  @Override
  public EdgeWithSource<I, E> next() {
    HiveRecord record = records.next();
    reusableEdge.setSourceVertexId(getSourceVertexId(record));
    reusableEdge.setTargetVertexId(getTargetVertexId(record));
    reusableEdge.setEdgeValue(getEdgeValue(record));
    return reusableEdge;
  }
}
