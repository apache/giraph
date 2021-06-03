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

package org.apache.giraph.mapping;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Interface of operations that can be done on mapping store
 * once it is fully loaded
 *
 * @param <I> vertex id type
 * @param <B> mapping target type
 */
public interface MappingStoreOps<I extends WritableComparable,
  B extends Writable> {

  /**
   * Must be called before anything else can be done
   * on this instance
   * @param mappingStore mapping store instance to operate on
   */
  void initialize(MappingStore<I, B> mappingStore);

  /**
   * True if MappingStoreOps is based on embedding info
   *
   * @return true if worker info is embedded into vertex ids
   */
  boolean hasEmbedding();

  /**
   * Embed target information into vertexId
   *
   * @param id vertexId
   */
  void embedTargetInfo(I id);

  /**
   * Remove target information from vertexId
   *
   * @param id vertexId
   */
  void removeTargetInfo(I id);


  /**
   * Get partition id for a vertex id
   *
   * @param id vertexId
   * @param partitionCount partitionCount
   * @param workerCount workerCount
   * @return partition of vertex id
   */
  int getPartition(I id, int partitionCount, int workerCount);
}
