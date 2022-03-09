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

package org.apache.giraph.edge.primitives;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.AbstractEdgeStore;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.utils.VertexIdEdgeIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Special edge store to be used when ids are IntWritable.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class IntEdgeStore<V extends Writable, E extends Writable>
  extends AbstractEdgeStore<IntWritable, V, E, Integer,
  Int2ObjectMap.Entry<OutEdges<IntWritable, E>>> {

  /**
   * Constructor.
   *
   * @param service       Service worker
   * @param configuration Configuration
   * @param progressable  Progressable
   */
  public IntEdgeStore(
      CentralizedServiceWorker<IntWritable, V, E> service,
      ImmutableClassesGiraphConfiguration<IntWritable, V, E> configuration,
      Progressable progressable) {
    super(service, configuration, progressable);
  }

  @Override
  protected IntWritable getVertexId(
    Int2ObjectMap.Entry<OutEdges<IntWritable, E>> entry,
    IntWritable representativeVertexId) {
    representativeVertexId.set(entry.getIntKey());
    return representativeVertexId;
  }

  @Override
  protected IntWritable createVertexId(
    Int2ObjectMap.Entry<OutEdges<IntWritable, E>> entry) {
    return new IntWritable(entry.getIntKey());
  }

  @Override
  protected OutEdges<IntWritable, E> getPartitionEdges(
    Int2ObjectMap.Entry<OutEdges<IntWritable, E>> entry) {
    return entry.getValue();
  }

  @Override
  protected void writeVertexKey(Integer key, DataOutput output)
      throws IOException {
    output.writeInt(key);
  }

  @Override
  protected Integer readVertexKey(DataInput input)
      throws IOException {
    return input.readInt();
  }

  @Override
  protected Iterator<Int2ObjectMap.Entry<OutEdges<IntWritable, E>>>
  getPartitionEdgesIterator(
    Map<Integer, OutEdges<IntWritable, E>> partitionEdges) {
    return  ((Int2ObjectMap<OutEdges<IntWritable, E>>) partitionEdges)
        .int2ObjectEntrySet()
        .iterator();
  }

  @Override
  protected Int2ObjectMap<OutEdges<IntWritable, E>> getPartitionEdges(
      int partitionId) {
    Int2ObjectMap<OutEdges<IntWritable, E>> partitionEdges =
        (Int2ObjectMap<OutEdges<IntWritable, E>>)
            transientEdges.get(partitionId);
    if (partitionEdges == null) {
      Int2ObjectMap<OutEdges<IntWritable, E>> newPartitionEdges =
          Int2ObjectMaps.synchronize(
              new Int2ObjectOpenHashMap<OutEdges<IntWritable, E>>());
      partitionEdges = (Int2ObjectMap<OutEdges<IntWritable, E>>)
          transientEdges.putIfAbsent(partitionId,
              newPartitionEdges);
      if (partitionEdges == null) {
        partitionEdges = newPartitionEdges;
      }
    }
    return partitionEdges;
  }

  @Override
  protected OutEdges<IntWritable, E> getVertexOutEdges(
      VertexIdEdgeIterator<IntWritable, E> vertexIdEdgeIterator,
      Map<Integer, OutEdges<IntWritable, E>> partitionEdgesIn) {
    Int2ObjectMap<OutEdges<IntWritable, E>> partitionEdges =
        (Int2ObjectMap<OutEdges<IntWritable, E>>) partitionEdgesIn;
    IntWritable vertexId = vertexIdEdgeIterator.getCurrentVertexId();
    OutEdges<IntWritable, E> outEdges = partitionEdges.get(vertexId.get());
    if (outEdges == null) {
      synchronized (partitionEdges) {
        outEdges = partitionEdges.get(vertexId.get());
        if (outEdges == null) {
          outEdges = configuration.createAndInitializeInputOutEdges();
          partitionEdges.put(vertexId.get(), outEdges);
        }
      }
    }
    return outEdges;
  }
}
