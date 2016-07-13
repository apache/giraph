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
package org.apache.giraph.types;

import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.collections.BasicSet;
import org.apache.giraph.types.ops.collections.array.WArrayList;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test basic collections
 */
public class TestCollections {
  @Test
  public void testBasicSet() {
    BasicSet<LongWritable> longSet = LongTypeOps.INSTANCE.createOpenHashSet();
    long count = 13;
    for (long i = 1, j = 0; j < count; i *= 10, j++) {
      longSet.add(new LongWritable(i));
    }
    Assert.assertEquals(count, longSet.size());

    longSet.clear();
    Assert.assertEquals(0, longSet.size());
  }

  @Test
  @Ignore("this test requires 32G to run")
  public void testLargeBasicSet() {
    long capacity = 1234567890;
    BasicSet<LongWritable> longSet = LongTypeOps.INSTANCE.createOpenHashSet(capacity);
    longSet.add(new LongWritable(capacity));
    longSet.add(new LongWritable(capacity));
    Assert.assertEquals(1, longSet.size());
  }

  @Test
  @Ignore("this test requires 1G to run")
  public void testLargeBasicList() {
    int capacity = 123456789;
    WArrayList<LongWritable> longSet = LongTypeOps.INSTANCE.createArrayList(capacity);
    longSet.addW(new LongWritable(capacity));
    longSet.addW(new LongWritable(capacity));
    Assert.assertEquals(2, longSet.size());
  }
}
