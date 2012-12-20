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

package org.apache.giraph.utils;

import java.util.BitSet;

/**
 * Bit set optimized for increasing longs to save storage space.
 * The general idea is that even though some keys will be added out-of-order,
 * there is a base key that keeps increasing so that the bit set doesn't get
 * very big.  When there are enough set bits, the bit set gets compacted.
 * Thread-safe.
 */
public class IncreasingBitSet {
  /** Minimum number of bits to shift */
  public static final int MIN_BITS_TO_SHIFT = 64 * 1024;
  /** Bit set used */
  private BitSet bitSet = new BitSet();
  /** Last base key (all keys < this have been accepted */
  private long lastBaseKey = 0;

  /**
   * Add a key if it is possible.
   *
   * @param key Key to add
   * @return True if the key was added, false otherwise
   */
  public synchronized boolean add(long key) {
    long remainder = key - lastBaseKey;
    checkLegalKey(remainder);

    if (remainder < 0) {
      return false;
    }
    if (bitSet.get((int) remainder)) {
      return false;
    }
    bitSet.set((int) remainder);
    int nextClearBit = bitSet.nextClearBit(0);
    if (nextClearBit >= MIN_BITS_TO_SHIFT) {
      bitSet = bitSet.get(nextClearBit,
          Math.max(nextClearBit, bitSet.length()));
      lastBaseKey += nextClearBit;
    }
    return true;
  }

  /**
   * Get the number of set bits
   *
   * @return Number of set bits
   */
  public synchronized long cardinality() {
    long size = bitSet.cardinality();
    return size + lastBaseKey;
  }

  /**
   * Get the size of the bit set
   *
   * @return Size of the bit set
   */
  public synchronized int size() {
    return bitSet.size();
  }

  /**
   * Check for existence of a key
   *
   * @param key Key to check for
   * @return True if the key exists, false otherwise
   */
  public synchronized boolean has(long key) {
    long remainder = key - lastBaseKey;
    checkLegalKey(remainder);

    if (remainder < 0) {
      return true;
    }
    return bitSet.get((int) remainder);
  }

  /**
   * Get the last base key (mainly for debugging).
   *
   * @return Last base key
   */
  public synchronized long getLastBaseKey() {
    return lastBaseKey;
  }

  /**
   * Check the remainder for validity
   *
   * @param remainder Remainder to check
   */
  private void checkLegalKey(long remainder) {
    if (remainder > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "checkLegalKey: Impossible that to add key " +
          (remainder + lastBaseKey) + " with base " +
          lastBaseKey + " since the " +
          "spread is too large (> " + Integer.MAX_VALUE);
    }
  }
}
