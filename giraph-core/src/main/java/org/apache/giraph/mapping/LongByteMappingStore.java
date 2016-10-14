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

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.google.common.collect.MapMaker;

/**
 *
 * An implementation of MappingStore&lt;LongWritable, ByteWritable&gt;
 *
 * Methods implemented here are thread safe by default because it is guaranteed
 * that each entry is written to only once.
 * It can represent up to a maximum of 254 workers
 * any byte passed is treated as unsigned
 */
@ThreadSafe
public class LongByteMappingStore
  extends DefaultImmutableClassesGiraphConfigurable<LongWritable, Writable,
  Writable> implements MappingStore<LongWritable, ByteWritable> {
  /** Logger instance */
  private static final Logger LOG = Logger.getLogger(
    LongByteMappingStore.class);

  /** Counts number of entries added */
  private final AtomicLong numEntries = new AtomicLong(0);

  /** Id prefix to bytesArray index mapping */
  private ConcurrentMap<Long, byte[]> concurrentIdToBytes;
  /** Primitive idToBytes for faster querying */
  private Long2ObjectOpenHashMap<byte[]> idToBytes;
  /** Number of lower order bits */
  private int lower;
  /** Number of distinct prefixes */
  private int upper;
  /** Bit mask for lowerOrder suffix bits */
  private int lowerBitMask;
  /** LowerOrder bits count */
  private int lowerOrder;

  @Override
  public void initialize() {
    upper = GiraphConstants.LB_MAPPINGSTORE_UPPER.get(getConf());
    lower = GiraphConstants.LB_MAPPINGSTORE_LOWER.get(getConf());

    if ((lower & (lower - 1)) != 0) {
      throw new IllegalStateException("lower not a power of two");
    }

    lowerBitMask = lower - 1;
    lowerOrder = Integer.numberOfTrailingZeros(lower); // log_2_(lower)
    concurrentIdToBytes = new MapMaker()
        .initialCapacity(upper)
        .concurrencyLevel(getConf().getNumInputSplitsThreads())
        .makeMap();
    idToBytes = new Long2ObjectOpenHashMap<>(upper);
  }

  /**
   * Auxiliary method to be used by getTarget
   *
   * @param vertexId vertexId
   * @return return byte value of target
   */
  public byte getByteTarget(LongWritable vertexId) {
    long key = vertexId.get() >>> lowerOrder;
    int suffix = (int) (vertexId.get() & lowerBitMask);
    if (!idToBytes.containsKey(key)) {
      return -1;
    }
    return idToBytes.get(key)[suffix];
  }

  @Override
  public void addEntry(LongWritable vertexId, ByteWritable target) {
    long key = vertexId.get() >>> lowerOrder;
    byte[] bytes = concurrentIdToBytes.get(key);
    if (bytes == null) {
      byte[] newBytes = new byte[lower];
      Arrays.fill(newBytes, (byte) -1);
      bytes = concurrentIdToBytes.putIfAbsent(key, newBytes);
      if (bytes == null) {
        bytes = newBytes;
      }
    }
    bytes[(int) (vertexId.get() & lowerBitMask)] = target.get();
    numEntries.getAndIncrement(); // increment count
  }

  @Override
  public ByteWritable getTarget(LongWritable vertexId,
    ByteWritable target) {
    byte bval = getByteTarget(vertexId);
    if (bval == -1) { // worker not assigned by mapping
      return null;
    }
    target.set(bval);
    return target;
  }

  @Override
  public void postFilling() {
    // not thread-safe
    for (Long id : concurrentIdToBytes.keySet()) {
      idToBytes.put(id, concurrentIdToBytes.get(id));
    }
    concurrentIdToBytes.clear();
    concurrentIdToBytes = null;
  }

  @Override
  public long getStats() {
    return numEntries.longValue();
  }
}
