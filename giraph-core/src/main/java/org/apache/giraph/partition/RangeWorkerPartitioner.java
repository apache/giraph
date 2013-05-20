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

import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Range partitioning will split the vertices by a key range based on a generic
 * type.  This allows vertices that have some locality with the vertex ids
 * to reduce the amount of messages sent.  The tradeoffs are that
 * range partitioning is more susceptible to hot spots if the keys
 * are not randomly distributed.  Another negative is the user must implement
 * some of the functionality around how to split the key range.
 *
 * Note:  This implementation is incomplete, the developer must implement the
 * various methods based on their index type.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class RangeWorkerPartitioner<I extends WritableComparable,
    V extends Writable, E extends Writable> implements
    WorkerGraphPartitioner<I, V, E> {
  /** Mapping of the vertex ids to the {@link PartitionOwner} */
  protected NavigableMap<I, RangePartitionOwner<I>> vertexRangeMap =
      new TreeMap<I, RangePartitionOwner<I>>();

  @Override
  public PartitionOwner createPartitionOwner() {
    return new RangePartitionOwner<I>();
  }

  @Override
  public PartitionOwner getPartitionOwner(I vertexId) {
    // Find the partition owner based on the maximum partition id.
    // If the vertex id exceeds any of the maximum partition ids, give
    // it to the last one
    if (vertexId == null) {
      throw new IllegalArgumentException(
          "getPartitionOwner: Illegal null vertex id");
    }
    I maxVertexIndex = vertexRangeMap.ceilingKey(vertexId);
    if (maxVertexIndex == null) {
      return vertexRangeMap.lastEntry().getValue();
    } else {
      return vertexRangeMap.get(vertexId);
    }
  }

  @Override
  public Collection<? extends PartitionOwner> getPartitionOwners() {
    return vertexRangeMap.values();
  }
}
