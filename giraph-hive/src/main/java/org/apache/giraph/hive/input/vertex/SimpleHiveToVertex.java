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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.facebook.hiveio.record.HiveReadableRecord;

import java.util.Iterator;

/**
 * Simple implementation of {@link HiveToVertex} when each vertex is in the one
 * row of the input.
 *
 * @param <I> Vertex ID
 * @param <V> Vertex Value
 * @param <E> Edge Value
 */
public abstract class SimpleHiveToVertex<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends AbstractHiveToVertex<I, V, E> {
  /** Hive records which we are reading from */
  private Iterator<HiveReadableRecord> records;

  /** Reusable vertex object */
  private Vertex<I, V, E> reusableVertex;

  /** Reusable vertex id */
  private I reusableVertexId;
  /** Reusable vertex value */
  private V reusableVertexValue;
  /** Reusable edges */
  private OutEdges<I, E> reusableOutEdges;

  /**
   * Read the Vertex's ID from the HiveRecord given.
   *
   * @param record HiveRecord to read from.
   * @return Vertex ID
   */
  public abstract I getVertexId(HiveReadableRecord record);

  /**
   * Read the Vertex's Value from the HiveRecord given.
   *
   * @param record HiveRecord to read from.
   * @return Vertex Value
   */
  public abstract V getVertexValue(HiveReadableRecord record);

  /**
   * Read Vertex's edges from the HiveRecord given.
   *
   * @param record HiveRecord to read from.
   * @return iterable of edges
   */
  public abstract Iterable<Edge<I, E>> getEdges(HiveReadableRecord record);

  @Override
  public void initializeRecords(Iterator<HiveReadableRecord> records) {
    this.records = records;
    reusableVertex = getConf().createVertex();
    reusableVertexId = getConf().createVertexId();
    reusableVertexValue = getConf().createVertexValue();
    reusableOutEdges = getConf().createOutEdges();
  }

  @Override
  public boolean hasNext() {
    return records.hasNext();
  }

  @Override
  public Vertex<I, V, E> next() {
    HiveReadableRecord record = records.next();
    I id = getVertexId(record);
    V value = getVertexValue(record);
    Iterable<Edge<I, E>> edges = getEdges(record);
    reusableVertex.initialize(id, value, edges);
    return reusableVertex;
  }

  protected I getReusableVertexId() {
    return reusableVertexId;
  }

  protected V getReusableVertexValue() {
    return reusableVertexValue;
  }

  /**
   * Get reusable OutEdges object
   *
   * @param <OE> Type of OutEdges
   * @return Reusable OutEdges object
   */
  protected <OE extends OutEdges<I, E>> OE getReusableOutEdges() {
    return (OE) reusableOutEdges;
  }
}
