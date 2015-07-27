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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Special edge store to be used when ids are LongWritable.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class LongEdgeStore<V extends Writable, E extends Writable>
  extends AbstractEdgeStore<LongWritable, V, E, Long,
  Long2ObjectMap.Entry<OutEdges<LongWritable, E>>> {

  /**
   * Constructor.
   *
   * @param service Service worker
   * @param configuration Configuration
   * @param progressable Progressable
   */
  public LongEdgeStore(
    CentralizedServiceWorker<LongWritable, V, E> service,
    ImmutableClassesGiraphConfiguration<LongWritable, V, E> configuration,
    Progressable progressable) {
    super(service, configuration, progressable);
  }

  @Override
  protected LongWritable getVertexId(
    Long2ObjectMap.Entry<OutEdges<LongWritable, E>> entry,
    LongWritable representativeVertexId) {
    representativeVertexId.set(entry.getLongKey());
    return representativeVertexId;
  }

  @Override
  protected LongWritable createVertexId(
    Long2ObjectMap.Entry<OutEdges<LongWritable, E>> entry) {
    return new LongWritable(entry.getLongKey());
  }


  @Override
  protected OutEdges<LongWritable, E> getPartitionEdges(
    Long2ObjectMap.Entry<OutEdges<LongWritable, E>> entry) {
    return entry.getValue();
  }

  @Override
  protected void writeVertexKey(Long key, DataOutput output)
      throws IOException {
    output.writeLong(key);
  }

  @Override
  protected Long readVertexKey(DataInput input) throws IOException {
    return input.readLong();
  }

  @Override
  protected Iterator<Long2ObjectMap.Entry<OutEdges<LongWritable, E>>>
  getPartitionEdgesIterator(
      Map<Long, OutEdges<LongWritable, E>> partitionEdges) {
    return ((Long2ObjectMap<OutEdges<LongWritable, E>>) partitionEdges)
        .long2ObjectEntrySet()
        .iterator();
  }

  @Override
  protected Long2ObjectMap<OutEdges<LongWritable, E>> getPartitionEdges(
    int partitionId) {
    Long2ObjectMap<OutEdges<LongWritable, E>> partitionEdges =
      (Long2ObjectMap<OutEdges<LongWritable, E>>)
        transientEdges.get(partitionId);
    if (partitionEdges == null) {
      Long2ObjectMap<OutEdges<LongWritable, E>> newPartitionEdges =
          Long2ObjectMaps.synchronize(
              new Long2ObjectOpenHashMap<OutEdges<LongWritable, E>>());
      partitionEdges = (Long2ObjectMap<OutEdges<LongWritable, E>>)
          transientEdges.putIfAbsent(partitionId,
          newPartitionEdges);
      if (partitionEdges == null) {
        partitionEdges = newPartitionEdges;
      }
    }
    return partitionEdges;
  }

  @Override
  protected OutEdges<LongWritable, E> getVertexOutEdges(
    VertexIdEdgeIterator<LongWritable, E> vertexIdEdgeIterator,
    Map<Long, OutEdges<LongWritable, E>> partitionEdgesIn) {
    Long2ObjectMap<OutEdges<LongWritable, E>> partitionEdges =
        (Long2ObjectMap<OutEdges<LongWritable, E>>) partitionEdgesIn;
    LongWritable vertexId = vertexIdEdgeIterator.getCurrentVertexId();
    OutEdges<LongWritable, E> outEdges = partitionEdges.get(vertexId.get());
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
