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
package org.apache.giraph.writable.kryo;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.giraph.types.ops.collections.array.WLongArrayList;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;


/**
 * Tests some subtle cases of kryo serialization.
 */
@RunWith(Theories.class)
public class KryoWritableTest {
  public static @DataPoints
  int [] methodIndex= {0, 1};

  interface CopyMethod {
    <T extends Writable> void copy(T from, T to, boolean checkOverRead);
  }

  List<CopyMethod> methods = new ArrayList<>();

  public KryoWritableTest() {
    methods.add(new CopyMethod() {
      @Override
      public <T extends Writable> void copy(T from, T to, boolean checkOverRead) {
        WritableUtils.copyInto(from, to, checkOverRead);
      }
    });

    methods.add(new CopyMethod() {
      @Override
      public <T extends Writable> void copy(T from, T to, boolean checkOverRead) {
        WritableUtils.copyIntoExtended(from, to, checkOverRead);
      }
    });
  }

  public static class TestClassA extends KryoWritable {
    final String testObject;
    final List list;
    final int something;

    public TestClassA(String testObject, List list, int something) {
      this.testObject = testObject;
      this.list = list;
      this.something = something;
    }

    public TestClassA() {
      this.testObject = null;
      this.list = null;
      this.something = -1;
    }
  }

  @Theory
  public void testTestClassA(int index) throws Exception {
    String testObject = "Hello World!";
    TestClassA res = new TestClassA();
    CopyMethod method = methods.get(index);

    method.copy(
        new TestClassA(testObject, Arrays.asList(1, 2, 3), 5), res, true);

    assertEquals(testObject, res.testObject);

    assertEquals(3, res.list.size());
    assertEquals(1, res.list.get(0));
    assertEquals(2, res.list.get(1));
    assertEquals(3, res.list.get(2));

    assertEquals(5, res.something);
  }

  public static class LongKryoWritable extends KryoWritable {
    private long value;

    public LongKryoWritable(long value) {
      this.value = value;
    }

    public long get() {
      return value;
    }

    public void set(long value) {
      this.value = value;
    }
  }


  int multiplier = 10; // use 5000 for profiling
  int longTestTimes = 1000 * multiplier;

  @Theory
  public void testLongKryoWritable(int index) throws Exception {
    LongKryoWritable from = new LongKryoWritable(0);
    LongKryoWritable to = new LongKryoWritable(0);
    CopyMethod method = methods.get(index);
    for (int i = 0; i < longTestTimes; i++) {
      from.set(i);
      method.copy(from, to, true);
      assertEquals(i, to.get());
    }
  }

  @Theory
  public void testLongWritable(int index) throws Exception {
    LongWritable from = new LongWritable(0);
    LongWritable to = new LongWritable(0);
    CopyMethod method = methods.get(index);
    for (int i = 0; i < longTestTimes; i++) {
      from.set(i);
      method.copy(from, to, true);
      assertEquals(i, to.get());
    }
  }

  public static class LongListKryoWritable extends KryoWritable {
    public LongArrayList value;

    public LongListKryoWritable(LongArrayList value) {
      this.value = value;
    }
  }

  int longListTestTimes = 1 * multiplier;
  int longListTestSize = 100000;

  @Theory
  public void testLongListKryoWritable(int index) throws Exception {
    LongArrayList list = new LongArrayList(longListTestSize);
    for (int i = 0; i < longListTestSize; i++) {
      list.add(i);
    }

    LongListKryoWritable from = new LongListKryoWritable(list);
    LongListKryoWritable to = new LongListKryoWritable(null);
    CopyMethod method = methods.get(index);
    for (int i = 0; i < longListTestTimes; i++) {
      from.value.set((2 * i) % longListTestSize, 0);
      method.copy(from, to, true);
    }
  }

  @Theory
  public void testLongListWritable(int index) throws Exception {
    WLongArrayList from = new WLongArrayList(longListTestSize);
    LongWritable value = new LongWritable();

    for (int i = 0; i < longListTestSize; i++) {
      value.set(i);
      from.addW(value);
    }

    WLongArrayList to = new WLongArrayList(longListTestSize);
    value.set(0);

    CopyMethod method = methods.get(index);
    for (int i = 0; i < longListTestTimes; i++) {
      from.setW((2 * i) % longListTestSize, value);
      method.copy(from, to, true);
    }
  }

  public static class NestedKryoWritable<T> extends KryoWritable {
    public LongKryoWritable value1;
    public T value2;

    public NestedKryoWritable(LongKryoWritable value1, T value2) {
      this.value1 = value1;
      this.value2 = value2;
    }
  }

  @Theory
  public void testNestedKryoWritable(int index) throws Exception {
    LongKryoWritable inner = new LongKryoWritable(5);
    NestedKryoWritable<LongKryoWritable> res = new NestedKryoWritable<>(null, null);
    CopyMethod method = methods.get(index);
    method.copy(
        new NestedKryoWritable<>(inner, inner), res, true);

    assertEquals(5, res.value1.get());
    Assert.assertTrue(res.value1 == res.value2);
  }

  @Theory
  public void testRecursiveKryoWritable(int index) throws Exception {
    LongKryoWritable inner = new LongKryoWritable(5);
    NestedKryoWritable wanted = new NestedKryoWritable<>(inner, null);
    CopyMethod method = methods.get(index);
    wanted.value2 = wanted;

    NestedKryoWritable res = new NestedKryoWritable<>(null, null);
    method.copy(wanted, res, true);

    assertEquals(5, res.value1.get());
    Assert.assertTrue(res == res.value2);
  }

  @Test
  public void testKryoImmutableMap() throws Exception {
    Long2IntOpenHashMap map = new Long2IntOpenHashMap();
    map.put(1, 2);
    map.put(10, 20);
    ImmutableMap<Long, Integer> copy =
        WritableUtils.createCopy(
            new KryoWritableWrapper<>(ImmutableMap.copyOf(map))).get();
    Assert.assertEquals(2, copy.size());
    Assert.assertEquals(2, copy.get(1L).intValue());
    Assert.assertEquals(20, copy.get(10L).intValue());
  }
}
