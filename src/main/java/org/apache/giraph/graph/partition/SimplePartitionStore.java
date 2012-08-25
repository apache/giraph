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

package org.apache.giraph.graph.partition;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple in-memory partition store.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public class SimplePartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends PartitionStore<I, V, E, M> {
  /** Map of stored partitions. */
  private final ConcurrentMap<Integer, Partition<I, V, E, M>> partitions =
      Maps.newConcurrentMap();
  /** Configuration. */
  private final Configuration conf;

  /**
   * Constructor.
   *
   * @param conf Configuration
   */
  public SimplePartitionStore(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void addPartition(Partition<I, V, E, M> partition) {
    if (partitions.putIfAbsent(partition.getId(), partition) != null) {
      throw new IllegalStateException("addPartition: partition " +
          partition.getId() + " already exists");
    }
  }

  @Override
  public void addPartitionVertices(Integer partitionId,
                                   Collection<Vertex<I, V, E, M>> vertices) {
    Partition<I, V, E, M> partition = partitions.get(partitionId);
    if (partition == null) {
      Partition<I, V, E, M> newPartition = new Partition<I, V, E, M>(conf,
          partitionId);
      partition = partitions.putIfAbsent(partitionId, newPartition);
      if (partition == null) {
        partition = newPartition;
      }
    }
    partition.putVertices(vertices);
  }

  @Override
  public Partition<I, V, E, M> getPartition(Integer partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  public Partition<I, V, E, M> removePartition(Integer partitionId) {
    return partitions.remove(partitionId);
  }

  @Override
  public void deletePartition(Integer partitionId) {
    partitions.remove(partitionId);
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


}
