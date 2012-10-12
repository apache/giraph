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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import java.util.Collection;

/**
 * Structure that stores partitions for a worker.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public abstract class PartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> {

  /**
   * Add a new partition to the store.
   *
   * @param partition Partition
   */
  public abstract void addPartition(Partition<I, V, E, M> partition);

  /**
   * Add some vertices to a (possibly existing) partition.
   *
   * @param partitionId Id of the destination partition
   * @param vertices Vertices
   */
  public abstract void addPartitionVertices(
      Integer partitionId, Collection<Vertex<I, V, E, M>> vertices);

  /**
   * Get a partition.
   *
   * @param partitionId Partition id
   * @return The requested partition
   */
  public abstract Partition<I, V, E, M> getPartition(Integer partitionId);

  /**
   * Remove a partition and return it.
   *
   * @param partitionId Partition id
   * @return The removed partition
   */
  public abstract Partition<I, V, E, M> removePartition(Integer partitionId);

  /**
   * Just delete a partition
   * (more efficient than {@link #removePartition(Integer partitionID)} if the
   * partition is out of core).
   *
   * @param partitionId Partition id
   */
  public abstract void deletePartition(Integer partitionId);

  /**
   * Whether a specific partition is present in the store.
   *
   * @param partitionId Partition id
   * @return True iff the partition is present
   */
  public abstract boolean hasPartition(Integer partitionId);

  /**
   * Return the ids of all the stored partitions as an Iterable.
   *
   * @return The partition ids
   */
  public abstract Iterable<Integer> getPartitionIds();

  /**
   * Return the number of stored partitions.
   *
   * @return The number of partitions
   */
  public abstract int getNumPartitions();

  /**
   * Whether the partition store is empty.
   *
   * @return True iff there are no partitions in the store
   */
  public boolean isEmpty() {
    return getNumPartitions() == 0;
  }

  /**
   * Return all the stored partitions as an Iterable.
   *
   * @return The partition objects
   */
  public Iterable<Partition<I, V, E, M>> getPartitions() {
    return Iterables.transform(getPartitionIds(),
        new Function<Integer, Partition<I, V, E, M>>() {
          @Override
          public Partition<I, V, E, M> apply(Integer partitionId) {
            return getPartition(partitionId);
          }
        });
  }
}
