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

/**
 * Divides the vertices into partitions by their hash code using a simple
 * round-robin hash for great balancing if given a random hash code.
 *
 * @param <I> Vertex id value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class HashPartitionerFactory<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends GraphPartitionerFactory<I, V, E> {

  @Override
  public int getPartition(I id, int partitionCount, int workerCount) {
    return Math.abs(id.hashCode() % partitionCount);
  }

  @Override
  public int getWorker(int partition, int partitionCount, int workerCount) {
    return partition % workerCount;
  }
}
