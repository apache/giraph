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
 * Divides the vertices into partitions by their hash code using a simple
 * round-robin hash for great balancing if given a random hash code.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class HashPartitionerFactory<I extends WritableComparable,
  V extends Writable, E extends Writable>
  extends DefaultImmutableClassesGiraphConfigurable<I, V, E>
  implements GraphPartitionerFactory<I, V, E>  {

  @Override
  public void initialize(LocalData<I, V, E, ? extends Writable> localData) {
  }

  @Override
  public MasterGraphPartitioner<I, V, E> createMasterGraphPartitioner() {
    return new HashMasterPartitioner<I, V, E>(getConf());
  }

  @Override
  public WorkerGraphPartitioner<I, V, E> createWorkerGraphPartitioner() {
    return new HashWorkerPartitioner<I, V, E>();
  }
}
