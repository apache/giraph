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

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.worker.LocalData;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Defines the partitioning framework for this application.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public interface GraphPartitionerFactoryInterface<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    ImmutableClassesGiraphConfigurable<I, V, E> {

  /**
   * Use some local data present in the worker
   *
   * @param localData localData present in the worker
   */
  void initialize(LocalData<I, V, E, ? extends Writable> localData);
  /**
   * Create the {@link MasterGraphPartitioner} used by the master.
   * Instantiated once by the master and reused.
   *
   * @return Instantiated master graph partitioner
   */
  MasterGraphPartitioner<I, V, E> createMasterGraphPartitioner();

  /**
   * Create the {@link WorkerGraphPartitioner} used by the worker.
   * Instantiated once by every worker and reused.
   *
   * @return Instantiated worker graph partitioner
   */
  WorkerGraphPartitioner<I, V, E> createWorkerGraphPartitioner();
}
