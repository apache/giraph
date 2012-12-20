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

package org.apache.giraph.comm;

import org.apache.giraph.utils.IncreasingBitSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test IncreasingBitSetTest
 */
public class IncreasingBitSetTest {
  @Test
  public void add256kIntegers() {
    IncreasingBitSet IncreasingBitSet = new IncreasingBitSet();
    for (int i = 0; i < 256 * 1024; ++i) {
      assertFalse(IncreasingBitSet.has(i));
      assertTrue(IncreasingBitSet.add(i));
      assertTrue(IncreasingBitSet.has(i));
      assertTrue(IncreasingBitSet.size() <=
          IncreasingBitSet.MIN_BITS_TO_SHIFT);
    }
    assertEquals(256 * 1024L, IncreasingBitSet.getLastBaseKey());
  }

  @Test
  public void add256kIntegersAlternate() {
    IncreasingBitSet IncreasingBitSet = new IncreasingBitSet();
    for (int i = 0; i < 256 * 1024; i += 2) {
      assertFalse(IncreasingBitSet.has(i));
      assertTrue(IncreasingBitSet.add(i));
      assertTrue(IncreasingBitSet.has(i));
      assertFalse(IncreasingBitSet.has(i + 1));
      assertTrue(IncreasingBitSet.size() <= 256 * 1024);
    }
    assertEquals(128 * 1024L, IncreasingBitSet.cardinality());
    for (int i = 1; i < 256 * 1024; i += 2) {
      assertFalse(IncreasingBitSet.has(i));
      assertTrue(IncreasingBitSet.add(i));
      assertTrue(IncreasingBitSet.has(i));
      assertTrue(IncreasingBitSet.has(i - 1));
      assertTrue(IncreasingBitSet.size() <= 256 * 1024);
    }
    assertEquals(256 * 1024L, IncreasingBitSet.cardinality());
  }

  @Test
  public void add256kIntegersOutOfOrder() {
    IncreasingBitSet IncreasingBitSet = new IncreasingBitSet();
    for (int i = 128 * 1024; i < 256 * 1024; ++i) {
      assertFalse(IncreasingBitSet.has(i));
      assertTrue(IncreasingBitSet.add(i));
      assertTrue(IncreasingBitSet.has(i));
      assertTrue(IncreasingBitSet.size() <= 512 * 1024);
    }
    assertEquals(128 * 1024L, IncreasingBitSet.cardinality());
    for (int i = 0; i < 128 * 1024; ++i) {
      assertFalse(IncreasingBitSet.has(i));
      assertTrue(IncreasingBitSet.add(i));
      assertTrue(IncreasingBitSet.has(i));
      assertTrue(IncreasingBitSet.size() <= 512 * 1024);
    }
    assertEquals(256 * 1024L, IncreasingBitSet.cardinality());
    assertEquals(256 * 1024L, IncreasingBitSet.getLastBaseKey());
  }
}
