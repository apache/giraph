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

import io.netty.util.internal.ThreadLocalRandom;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.BasicCollectionsUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test BasicSets and Basic2ObjectMaps
 */
public class TestBasicCollections {
  private void testLongWritable2Object(Map<Long, String> input) {
    Basic2ObjectMap<LongWritable, String> map = BasicCollectionsUtils.create2ObjectMap(LongWritable.class);

    LongWritable longW = new LongWritable();
    // adding
    long keySum = 0;
    for (Long key : input.keySet()) {
      longW.set(key.longValue());
      Assert.assertNull(map.put(longW, input.get(key)));
      keySum += key.longValue();
    }
    Assert.assertEquals(input.size(), map.size());
    // iterator
    long sum = 0;
    Iterator<LongWritable> iterator = map.fastKeyIterator();
    while (iterator.hasNext()) {
      sum += iterator.next().get();
    }
    Assert.assertEquals(keySum, sum);
    // removal
    for (Long key : input.keySet()) {
      longW.set(key.longValue());
      Assert.assertEquals(input.get(key), map.get(longW));
      map.remove(longW);
    }
    Assert.assertEquals(0, map.size());
  }

  private void testFloatWritable2Object(Map<Float, String> input) {
    Basic2ObjectMap<FloatWritable, String> map = BasicCollectionsUtils.create2ObjectMap(FloatWritable.class);

    FloatWritable floatW = new FloatWritable();
    // adding
    float keySum = 0;
    for (Float key : input.keySet()) {
      floatW.set(key.longValue());
      Assert.assertNull(map.put(floatW, input.get(key)));
      keySum += key.longValue();
    }
    Assert.assertEquals(input.size(), map.size());
    // iterator
    float sum = 0;
    Iterator<FloatWritable> iterator = map.fastKeyIterator();
    while (iterator.hasNext()) {
      sum += iterator.next().get();
    }
    Assert.assertEquals(keySum, sum, 1e-6);
    // removal
    for (Float key : input.keySet()) {
      floatW.set(key.longValue());
      Assert.assertEquals(input.get(key), map.get(floatW));
      map.remove(floatW);
    }
    Assert.assertEquals(0, map.size());
  }

  @Test
  public void testLongWritable2Object() {
    Map<Long, String> input = new HashMap<>();
    input.put(-1l, "a");
    input.put(0l, "b");
    input.put(100l, "c");
    input.put(26256l, "d");
    input.put(-1367367l, "a");
    input.put(-35635l, "e");
    input.put(1234567l, "f");
    testLongWritable2Object(input);
  }

  @Test
  public void testFloatWritable2Object() {
    Map<Float, String> input = new HashMap<>();
    input.put(-1f, "a");
    input.put(0f, "b");
    input.put(1.23f, "c");
    input.put(-12.34f, "d");
    input.put(-1367367.45f, "a");
    input.put(-3.456f, "e");
    input.put(12.78f, "f");
    testFloatWritable2Object(input);
  }

  private <K, V> V getConcurrently(Basic2ObjectMap<K, V> map, K key, V defaultValue) {
    synchronized (map) {
      V value = map.get(key);

      if (value == null) {
        value = defaultValue;
        map.put(key, value);
      }
      return value;
    }
  }

  private <K, V> void removeConcurrently(Basic2ObjectMap<K, V> map, K key) {
    synchronized (map) {
      map.remove(key);
    }
  }

  @Test
  public void testLongWritable2ObjectConcurrent() throws InterruptedException {
    final int numThreads = 10;
    final int numValues = 100000;

    final Map<Integer, Double> map = new ConcurrentHashMap<>();
    for (int i = 0; i < numValues; i++) {
      double value = ThreadLocalRandom.current().nextDouble();
      map.put(i, value);
    }

    final int PARTS = 8;
    final Basic2ObjectMap<IntWritable, Double>[] basicMaps = new Basic2ObjectMap[PARTS];
    for (int i = 0; i < PARTS; i++) {
      basicMaps[i] = BasicCollectionsUtils.create2ObjectMap(IntWritable.class);
    }

    long startTime = System.currentTimeMillis();

    // adding in several threads
    Thread[] threads = new Thread[numThreads];
    for (int t = 0; t < threads.length; t++) {
      threads[t] = new Thread(new Runnable() {
        @Override
        public void run() {
          IntWritable intW = new IntWritable();
          for (int i = 0; i < numValues; i++) {
            intW.set(i);
            double value = getConcurrently(basicMaps[(i * 123 + 17) % PARTS], intW, map.get(i));
            Assert.assertEquals(map.get(i).doubleValue(), value, 1e-6);
          }
        }
      });
      threads[t].start();
    }
    for (Thread t : threads) {
      t.join();
    }
    int totalSize = 0;
    for (int i = 0; i < PARTS; i++) {
      totalSize += basicMaps[i].size();
    }
    Assert.assertEquals(numValues, totalSize);

    long endTime = System.currentTimeMillis();
    System.out.println("Add Time: " + (endTime - startTime) / 1000.0);

    // removing all objects
    for (int t = 0; t < threads.length; t++) {
      threads[t] = new Thread(new Runnable() {
        @Override
        public void run() {
          IntWritable intW = new IntWritable();
          for (int i = 0; i < numValues; i++) {
            intW.set(i);
            removeConcurrently(basicMaps[(i * 123 + 17) % PARTS], intW);
          }
        }
      });
      threads[t].start();
    }
    for (Thread t : threads) {
      t.join();
    }
    for (int i = 0; i < PARTS; i++) {
      Assert.assertEquals(0, basicMaps[i].size());
    }
  }
}
