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

import org.apache.hadoop.io.LongWritable;

/**
 * MappingStoreOps implementation used to embed target information into
 * vertex id. Stores information in the higher order bits of the long id
 */
public class DefaultEmbeddedLongByteOps extends AbstractLongByteOps {
  /** Bit mask for first 9 bits in a long */
  private static final long MASK = ((long) 0x1FF) << 55;
  /** Inverse of MASK */
  private static final long IMASK = ~ MASK;

  /**
   * Default constructor (do not use)
   */
  public DefaultEmbeddedLongByteOps() {
  }

  @Override
  public boolean hasEmbedding() {
    return true;
  }

  @Override
  public void embedTargetInfo(LongWritable id) {
    if ((id.get() & MASK) != 0) {
      throw new IllegalStateException("Expected first 9 bits of long " +
          " to be empty");
    }
    byte target = mappingStore.getByteTarget(id);
    // first bit = 0 & rest 8 bits set to target
    // add 1 to distinguish between not set and assignment to worker-0
    // (prefix bits = 0 can mean one of two things :
    // no entry in the mapping, in which case target = -1, so -1 + 1 = 0
    // vertex is created later during computation, so prefix bits are 0 anyway)
    long maskValue = ((1L + target) & 0xFF) << 55;
    id.set(id.get() | maskValue);
  }

  @Override
  public void removeTargetInfo(LongWritable id) {
    id.set(id.get() & IMASK);
  }

  @Override
  public int getPartition(LongWritable id, int partitionCount,
    int workerCount) {
    // extract last 8 bits
    // subtract 1 since added 1 during embedInfo (unset = -1)
    byte target = (byte) (((id.get() >>> 55) & 0xFF) - 1);
    return computePartition(id, partitionCount, workerCount, target);
  }
}
