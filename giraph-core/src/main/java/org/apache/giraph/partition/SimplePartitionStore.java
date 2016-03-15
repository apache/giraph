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
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.VertexIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;

/**
 * A simple in-memory partition store.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class SimplePartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements PartitionStore<I, V, E> {
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Job context (for progress) */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Map of stored partitions. */
  private final ConcurrentMap<Integer, Partition<I, V, E>> partitions =
      Maps.newConcurrentMap();
  /** Queue of partitions to be precessed in a superstep */
  private BlockingQueue<Partition<I, V, E>> partitionQueue;

  /**
   * Constructor.
   * @param conf Configuration
   * @param context Mapper context
   */
  public SimplePartitionStore(ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Mapper<?, ?, ?, ?>.Context context) {
    this.conf = conf;
    this.context = context;
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
  public boolean isEmpty() {
    return partitions.size() == 0;
  }

  @Override
  public void startIteration() {
    checkState(partitionQueue == null || partitionQueue.isEmpty(),
        "startIteration: It seems that some of " +
          "of the partitions from previous iteration over partition store are" +
          " not yet processed.");

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
  public void shutdown() { }

  @Override
  public void initialize() { }
}
