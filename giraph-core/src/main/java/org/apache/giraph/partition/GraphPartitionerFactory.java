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

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.worker.LocalData;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Defines the partitioning framework for this application.
 *
 * Abstracts and implements all GraphPartitionerFactoryInterface logic
 * on top of two functions which define partitioning scheme:
 * - which partition vertex should be in, and
 * - which partition should belong to which worker
 *
 * @param <I> Vertex id value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class GraphPartitionerFactory<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends DefaultImmutableClassesGiraphConfigurable<I, V, E>
    implements GraphPartitionerFactoryInterface<I, V, E>  {
  @Override
  public void initialize(LocalData<I, V, E, ? extends Writable> localData) {
  }

  @Override
  public final MasterGraphPartitioner<I, V, E> createMasterGraphPartitioner() {
    return new MasterGraphPartitionerImpl<I, V, E>(getConf()) {
      @Override
      protected int getWorkerIndex(int partition, int partitionCount,
          int workerCount) {
        return GraphPartitionerFactory.this.getWorker(
            partition, partitionCount, workerCount);
      }
    };
  }

  @Override
  public final WorkerGraphPartitioner<I, V, E> createWorkerGraphPartitioner() {
    return new WorkerGraphPartitionerImpl<I, V, E>() {
      @Override
      protected int getPartitionIndex(I id, int partitionCount,
        int workerCount) {
        return GraphPartitionerFactory.this.getPartition(id,
            partitionCount, workerCount);
      }
    };
  }

  /**
   * Calculates in which partition current vertex belongs to,
   * from interval [0, partitionCount).
   *
   * @param id Vertex id
   * @param partitionCount Number of partitions
   * @param workerCount Number of workers
   * @return partition
   */
  public abstract int getPartition(I id, int partitionCount,
    int workerCount);

  /**
   * Calculates worker that should be responsible for passed partition.
   *
   * @param partition Current partition
   * @param partitionCount Number of partitions
   * @param workerCount Number of workers
   * @return index of worker responsible for current partition
   */
  public abstract int getWorker(
      int partition, int partitionCount, int workerCount);

  /**
   * Utility function for calculating in which partition value
   * from interval [0, max) should belong to.
   *
   * @param value Value for which partition is requested
   * @param max Maximum possible value
   * @param partitions Number of partitions, equally sized.
   * @return Index of partition where value belongs to.
   */
  public static int getPartitionInRange(int value, int max, int partitions) {
    double keyRange = ((double) max) / partitions;
    int part = (int) ((value % max) / keyRange);
    return Math.max(0, Math.min(partitions - 1, part));
  }

  /**
   * Utility function for calculating in which partition value
   * from interval [0, max) should belong to.
   *
   * @param value Value for which partition is requested
   * @param max Maximum possible value
   * @param partitions Number of partitions, equally sized.
   * @return Index of partition where value belongs to.
   */
  public static int getPartitionInRange(long value, long max, int partitions) {
    double keyRange = ((double) max) / partitions;
    int part = (int) ((value % max) / keyRange);
    return Math.max(0, Math.min(partitions - 1, part));
  }
}
