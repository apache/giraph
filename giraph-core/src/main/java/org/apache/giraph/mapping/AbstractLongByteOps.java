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

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Implementation of basic methods in MappingStoreOps
 */
@SuppressWarnings("unchecked, rawtypes")
public abstract class AbstractLongByteOps
  implements MappingStoreOps<LongWritable, ByteWritable> {
  /** Mapping store instance to operate on */
  protected LongByteMappingStore mappingStore;

  @Override
  public void initialize(MappingStore<LongWritable,
      ByteWritable> mappingStore) {
    this.mappingStore = (LongByteMappingStore) mappingStore;
  }

  /**
   * Compute partition given id, partitionCount, workerCount &amp; target
   * @param id vertex id
   * @param partitionCount number of partitions
   * @param workerCount number of workers
   * @param target target worker
   * @return partition number
   */
  protected int computePartition(LongWritable id, int partitionCount,
    int workerCount, byte target) {
    int numRows = partitionCount / workerCount;
    numRows = (numRows * workerCount == partitionCount) ? numRows : numRows + 1;
    if (target == -1) {
      // default to hash based partitioning
      return Math.abs(id.hashCode() % partitionCount);
    } else {
      int targetWorker = target & 0xFF;
      // assume zero based indexing of partition & worker [also consecutive]
      return numRows * targetWorker + Math.abs(id.hashCode() % numRows);
    }
  }
}
