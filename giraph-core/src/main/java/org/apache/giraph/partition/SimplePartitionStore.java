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

package org.apache.giraph.partition;

import com.google.common.collect.Maps;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.EdgeStore;
import org.apache.giraph.edge.EdgeStoreFactory;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.VertexIdEdges;
import org.apache.giraph.utils.VertexIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple in-memory partition store.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class SimplePartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends PartitionStore<I, V, E> {
  /** Map of stored partitions. */
  private final ConcurrentMap<Integer, Partition<I, V, E>> partitions =
      Maps.newConcurrentMap();
  /** Edge store for this worker. */
  private final EdgeStore<I, V, E> edgeStore;
  /** Configuration. */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Context used to report progress */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Queue of partitions to be precessed in a superstep */
  private BlockingQueue<Partition<I, V, E>> partitionQueue;

  /**
   * Constructor.
   * @param conf Configuration
   * @param context Mapper context
   * @param serviceWorker Service worker
   */
  public SimplePartitionStore(ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Mapper<?, ?, ?, ?>.Context context,
      CentralizedServiceWorker<I, V, E> serviceWorker) {
    this.conf = conf;
    this.context = context;
    EdgeStoreFactory<I, V, E> edgeStoreFactory = conf.createEdgeStoreFactory();
    edgeStoreFactory.initialize(serviceWorker, conf, context);
    edgeStore = edgeStoreFactory.newStore();
  }

  @Override
  public boolean addPartition(Partition<I, V, E> partition) {
    return partitions.putIfAbsent(partition.getId(), partition) == null;
  }

  @Override
  public Partition<I, V, E> removePartition(Integer partitionId) {
    return partitions.remove(partitionId);
  }

  @Override
  public boolean hasPartition(Integer partitionId) {
    return partitions.containsKey(partitionId);
  }

  @Override
  public Iterable<Integer> getPartitionIds() {
    return partitions.keySet();
  }

  @Override
  public int getNumPartitions() {
    return partitions.size();
  }

  @Override
  public long getPartitionVertexCount(Integer partitionId) {
    Partition partition = partitions.get(partitionId);
    if (partition == null) {
      return 0;
    } else {
      return partition.getVertexCount();
    }
  }

  @Override
  public long getPartitionEdgeCount(Integer partitionId) {
    Partition partition = partitions.get(partitionId);
    if (partition == null) {
      return 0;
    } else {
      return partition.getEdgeCount();
    }
  }

  @Override
  public void startIteration() {
    if (partitionQueue != null && !partitionQueue.isEmpty()) {
      throw new IllegalStateException("startIteration: It seems that some of " +
          "of the partitions from previous iteration over partition store are" +
          " not yet processed.");
    }
    partitionQueue =
        new ArrayBlockingQueue<Partition<I, V, E>>(getNumPartitions());
    for (Partition<I, V, E> partition : partitions.values()) {
      partitionQueue.add(partition);
    }
  }

  @Override
  public Partition<I, V, E> getNextPartition() {
    return partitionQueue.poll();
  }

  @Override
  public void putPartition(Partition<I, V, E> partition) { }

  /**
   * Get or create a partition.
   * @param partitionId Partition Id
   * @return The requested partition (never null)
   */
  private Partition<I, V, E> getOrCreatePartition(Integer partitionId) {
    Partition<I, V, E> oldPartition = partitions.get(partitionId);
    if (oldPartition == null) {
      Partition<I, V, E> newPartition =
          conf.createPartition(partitionId, context);
      oldPartition = partitions.putIfAbsent(partitionId, newPartition);
      if (oldPartition == null) {
        return newPartition;
      }
    }
    return oldPartition;
  }

  @Override
  public void addPartitionVertices(Integer partitionId,
      ExtendedDataOutput extendedDataOutput) {
    VertexIterator<I, V, E> vertexIterator =
        new VertexIterator<I, V, E>(extendedDataOutput, conf);

    Partition<I, V, E> partition = getOrCreatePartition(partitionId);
    partition.addPartitionVertices(vertexIterator);
    putPartition(partition);
  }

  @Override
  public void addPartitionEdges(Integer partitionId,
      VertexIdEdges<I, E> edges) {
    edgeStore.addPartitionEdges(partitionId, edges);
  }

  @Override
  public void moveEdgesToVertices() {
    edgeStore.moveEdgesToVertices();
  }
}
