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

import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Structure that stores partitions for a worker. PartitionStore does not allow
 * random accesses to partitions except upon removal.
 * This structure is thread-safe, unless otherwise specified.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public interface PartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /**
   * Add a *new* partition to the store. If the partition is already existed,
   * it does not add the partition and returns false.
   * Note: this method is not thread-safe and should be called by a single
   * thread.
   *
   * @param partition Partition to add
   * @return Whether the addition made any change in the partition store
   */
  boolean addPartition(Partition<I, V, E> partition);

  /**
   * Remove a partition and return it. Called from a single thread, *not* from
   * within a scheduling cycle. This method should *not* be called in
   * INPUT_SUPERSTEP.
   *
   * @param partitionId Partition id
   * @return The removed partition
   */
  Partition<I, V, E> removePartition(Integer partitionId);

  /**
   * Whether a specific partition is present in the store.
   *
   * @param partitionId Partition id
   * @return True iff the partition is present
   */
  boolean hasPartition(Integer partitionId);

  /**
   * Return the ids of all the stored partitions as an Iterable.
   *
   * @return The partition ids
   */
  Iterable<Integer> getPartitionIds();

  /**
   * Return the number of stored partitions.
   *
   * @return The number of partitions
   */
  int getNumPartitions();

  /**
   * Return the number of vertices in a partition.
   *
   * @param partitionId Partition id
   * @return The number of vertices in the specified partition
   */
  long getPartitionVertexCount(Integer partitionId);

  /**
   * Return the number of edges in a partition.
   *
   * @param partitionId Partition id
   * @return The number of edges in the specified partition
   */
  long getPartitionEdgeCount(Integer partitionId);

  /**
   * Whether the partition store is empty.
   *
   * @return True iff there are no partitions in the store
   */
  boolean isEmpty();

  /**
   * Add vertices to a given partition from a given DataOutput instance. This
   * method is called right after receipt of vertex request in INPUT_SUPERSTEP.
   *
   * @param partitionId Partition id
   * @param extendedDataOutput Output containing serialized vertex data
   */
  void addPartitionVertices(Integer partitionId,
                            ExtendedDataOutput extendedDataOutput);

  /**
   * Called at the end of the computation. Called from a single thread.
   */
  void shutdown();

  /**
   * Called at the beginning of the computation. Called from a single thread.
   */
  void initialize();

  /**
   * Start the iteration cycle to iterate over partitions. Note that each
   * iteration cycle *must* iterate over *all* partitions. Usually an iteration
   * cycle is necessary for
   *   1) moving edges (from edge store) to vertices after edge input splits are
   *      loaded in INPUT_SUPERSTEP,
   *   2) computing partitions in each superstep (called once per superstep),
   *   3) saving vertices/edges in the output superstep.
   *   4) any sort of populating a data-structure based on the partitions in
   *      this store.
   *
   * After an iteration is started, multiple threads can access the partition
   * store using {@link #getNextPartition()} to iterate over the partitions.
   * Each time {@link #getNextPartition()} is called an unprocessed partition in
   * the current iteration is returned. After processing of the partition is
   * done, partition should be put back in the store using
   * {@link #putPartition(Partition)}. Here is an example of the entire
   * workflow:
   *
   * In the main thread:
   *   partitionStore.startIteration();
   *
   * In multiple threads iterating over the partitions:
   *   Partition partition = partitionStore.getNextPartition();
   *   ... do stuff with partition ...
   *   partitionStore.putPartition(partition);
   *
   * Called from a single thread.
   */
  void startIteration();

  /**
   * Return the next partition in iteration for the current superstep.
   * Note: user has to put back the partition to the store through
   * {@link #putPartition(Partition)} after use. Look at comments on
   * {@link #startIteration()} for more detail.
   *
   * @return The next partition to process
   */
  Partition<I, V, E> getNextPartition();

  /**
   * Put a partition back to the store. Use this method to put a partition
   * back after it has been retrieved through {@link #getNextPartition()}.
   * Look at comments on {@link #startIteration()} for more detail.
   *
   * @param partition Partition
   */
  void putPartition(Partition<I, V, E> partition);
}
