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

package org.apache.giraph.edge;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.VertexIdEdgeIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

import com.google.common.collect.MapMaker;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple in memory edge store which supports any type of ids.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class SimpleEdgeStore<I extends WritableComparable,
  V extends Writable, E extends Writable>
  extends AbstractEdgeStore<I, V, E, I,
  Map.Entry<I, OutEdges<I, E>>> {

  /**
   * Constructor.
   *
   * @param service Service worker
   * @param configuration Configuration
   * @param progressable Progressable
   */
  public SimpleEdgeStore(
    CentralizedServiceWorker<I, V, E> service,
    ImmutableClassesGiraphConfiguration<I, V, E> configuration,
    Progressable progressable) {
    super(service, configuration, progressable);
  }

  @Override
  protected I getVertexId(Map.Entry<I, OutEdges<I, E>> entry,
    I representativeVertexId) {
    return entry.getKey();
  }

  @Override
  protected I createVertexId(Map.Entry<I, OutEdges<I, E>> entry) {
    return entry.getKey();
  }

  @Override
  protected ConcurrentMap<I, OutEdges<I, E>> getPartitionEdges(
    int partitionId) {
    ConcurrentMap<I, OutEdges<I, E>> partitionEdges =
        (ConcurrentMap<I, OutEdges<I, E>>) transientEdges.get(partitionId);
    if (partitionEdges == null) {
      ConcurrentMap<I, OutEdges<I, E>> newPartitionEdges =
          new MapMaker().concurrencyLevel(
              configuration.getNettyServerExecutionConcurrency()).makeMap();
      partitionEdges = (ConcurrentMap<I, OutEdges<I, E>>)
          transientEdges.putIfAbsent(partitionId, newPartitionEdges);
      if (partitionEdges == null) {
        partitionEdges = newPartitionEdges;
      }
    }
    return partitionEdges;
  }

  @Override
  protected OutEdges<I, E> getPartitionEdges(
    Map.Entry<I, OutEdges<I, E>> entry) {
    return entry.getValue();
  }

  @Override
  protected void writeVertexKey(I key, DataOutput output) throws IOException {
    key.write(output);
  }

  @Override
  protected I readVertexKey(DataInput input) throws IOException {
    I id = configuration.createVertexId();
    id.readFields(input);
    return id;
  }

  @Override
  protected Iterator<Map.Entry<I, OutEdges<I, E>>>
  getPartitionEdgesIterator(Map<I, OutEdges<I, E>> partitionEdges) {
    return partitionEdges.entrySet().iterator();
  }

  @Override
  protected OutEdges<I, E> getVertexOutEdges(
      VertexIdEdgeIterator<I, E> vertexIdEdgeIterator,
      Map<I, OutEdges<I, E>> partitionEdgesIn) {
    ConcurrentMap<I, OutEdges<I, E>> partitionEdges =
        (ConcurrentMap<I, OutEdges<I, E>>) partitionEdgesIn;
    I vertexId = vertexIdEdgeIterator.getCurrentVertexId();
    OutEdges<I, E> outEdges = partitionEdges.get(vertexId);
    if (outEdges == null) {
      OutEdges<I, E> newOutEdges =
          configuration.createAndInitializeInputOutEdges();
      outEdges = partitionEdges.putIfAbsent(vertexId, newOutEdges);
      if (outEdges == null) {
        outEdges = newOutEdges;
        // Since we had to use the vertex id as a new key in the map,
        // we need to release the object.
        vertexIdEdgeIterator.releaseCurrentVertexId();
      }
    }
    return outEdges;
  }
}
