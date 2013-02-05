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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.collect.Maps;

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
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> conf;
  /** Context used to report progress */
  private final Mapper<?, ?, ?, ?>.Context context;

  /**
   * Constructor.
   *
   * @param conf Configuration
   * @param context Mapper context
   */
  public SimplePartitionStore(
      ImmutableClassesGiraphConfiguration<I, V, E, M> conf,
      Mapper<?, ?, ?, ?>.Context context) {
    this.conf = conf;
    this.context = context;
  }

  @Override
  public void addPartition(Partition<I, V, E, M> partition) {
    Partition<I, V, E, M> oldPartition = partitions.get(partition.getId());
    if (oldPartition == null) {
      oldPartition = partitions.putIfAbsent(partition.getId(), partition);
      if (oldPartition == null) {
        return;
      }
    }
    oldPartition.addPartition(partition);
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

  @Override
  public void putPartition(Partition<I, V, E, M> partition) { }
}
