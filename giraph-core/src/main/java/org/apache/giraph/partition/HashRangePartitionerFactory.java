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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.primitives.UnsignedInts;

/**
 * Divides the vertices into partitions by their hash code using ranges of the
 * hash space.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class HashRangePartitionerFactory<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends GraphPartitionerFactory<I, V, E> {

  /** A transformed hashCode() must be strictly smaller than this. */
  private static final long HASH_LIMIT = 2L * Integer.MAX_VALUE + 2L;

  @Override
  public int getPartition(I id, int partitionCount, int workerCount) {
    long unsignedHashCode = UnsignedInts.toLong(id.hashCode());
    // The reader can verify that unsignedHashCode of HASH_LIMIT - 1 yields
    // index of size - 1, and unsignedHashCode of 0 yields index of 0.
    return (int) ((unsignedHashCode * partitionCount) / HASH_LIMIT);
  }

  @Override
  public int getWorker(int partition, int partitionCount, int workerCount) {
    return partition % workerCount;
  }
}
