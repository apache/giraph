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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.types.ops.collections.array.WLongArrayList;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.writable.kryo.markers.NonKryoWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import it.unimi.dsi.fastutil.chars.Char2ObjectMap;
import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.Int2BooleanMap;
import it.unimi.dsi.fastutil.ints.Int2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;



/**
 * Tests some subtle cases of kryo serialization.
 */
public class KryoWritableWrapperTest {
  public static <T> T kryoSerDeser(T t) throws IOException {
    KryoWritableWrapper<T> wrapped = new KryoWritableWrapper<>(t);
    KryoWritableWrapper<T> deser = new KryoWritableWrapper<>();
    WritableUtils.copyInto(wrapped, deser, true);
    return deser.get();
  }

  @Test
  public void testArraysAsList() throws IOException {
    List res = kryoSerDeser(Arrays.asList(1, 2, 3));

    assertEquals(3, res.size());
    assertEquals(1, res.get(0));
    assertEquals(2, res.get(1));
    assertEquals(3, res.get(2));
  }


  @Test
  public void testArraysAsListMultiRef() throws IOException {
    List list = Arrays.asList(1, 2, 3);
    Object obj = new Object();
    List wanted = Arrays.asList(list, list, obj, obj, null);
    wanted.set(4, wanted);
    List res = kryoSerDeser(wanted);

    assertTrue(res.get(0) == res.get(1));
    assertTrue(res.get(2) == res.get(3));
    // TODO see if this can be supported, though this is a rare case:
    // assertTrue(res == res.get(4));
  }

  @Test
  public void testCollectionsNCopiesList() throws IOException {
    List res = kryoSerDeser(Collections.nCopies(3, 42));

    assertEquals(3, res.size());
    assertEquals(42, res.get(0));
    assertEquals(42, res.get(1));
    assertEquals(42, res.get(2));
  }

  @Test
  public void testCollectionsNCopiesObjectList() throws IOException {
    String testObject = "Hello World!";
    List<String> res = kryoSerDeser(Collections.nCopies(3, testObject));

    assertEquals(3, res.size());
    assertEquals(testObject, res.get(0));
    assertEquals(testObject, res.get(1));
    assertEquals(testObject, res.get(2));
  }

  @Test
  public void testUnmodifiableIterator() throws IOException {
    Iterator<Integer> in = Iterables.concat(
        Arrays.asList(0, 1),
        Arrays.asList(2, 3),
        Arrays.asList(4, 5)).iterator();

    in.next();
    in.next();
    in.next();
    Iterator res = kryoSerDeser(in);

    int cnt = 3;
    for(; res.hasNext(); cnt++) {
      assertEquals(cnt, res.next());
    }
    assertEquals(6, cnt);
  }

  @Test
  public void testIteratorsConcat() throws IOException {
    Iterator<Integer> in = Iterators.concat(
        Arrays.asList(0, 1).iterator(),
        Arrays.asList(2, 3).iterator(),
        Arrays.asList(4, 5).iterator());

    in.next();
    in.next();
    in.next();

    Iterator res = kryoSerDeser(in);

    int cnt = 3;
    for(; res.hasNext(); cnt++) {
      assertEquals(cnt, res.next());
    }
    assertEquals(6, cnt);

  }

  @Test
  public void testImmutableList() throws IOException {
    {
      List res = kryoSerDeser(ImmutableList.of(1, 2));
      assertEquals(2, res.size());
      assertEquals(1, res.get(0));
      assertEquals(2, res.get(1));
    }

    {
      List list = ImmutableList.of(1, 2, 3);
      Object obj = new Object();
      List wanted = ImmutableList.of(list, list, obj, obj);
      List res = kryoSerDeser(wanted);

      assertTrue(res.get(0) == res.get(1));
      assertTrue(res.get(2) == res.get(3));
    }
  }

  @Test
  public void testImmutableMapSerialization() throws IOException {
    Map original = ImmutableMap.of("x", "y", "y", "z");
    Map copy = kryoSerDeser(original);
    assertEquals(original, copy);
  }

  @Test
  public void testImmutableMapSinglePairSerialization() throws IOException {
    Map original = ImmutableMap.of("x", "y");
    Map copy = kryoSerDeser(original);
    assertEquals(original, copy);
  }

  @Test
  public void testImmutableBiMap() throws IOException {
    Map original = ImmutableBiMap.of("x", "y", "z", "w");
    Map copy = kryoSerDeser(original);
    assertEquals(original, copy);
  }

  @Test
  public void testSingletonImmutableBiMapSerialization() throws IOException {
    Map original = ImmutableBiMap.of("x", "y");
    Map copy = kryoSerDeser(original);
    assertEquals(original, copy);
  }

  @Test
  public void testEmptyImmutableBiMap() throws IOException {
    Map original = ImmutableBiMap.of();
    Map copy = kryoSerDeser(original);
    assertEquals(original, copy);
  }

  @Test
  public void testFastutilSet() throws ClassNotFoundException, IOException {
    LongOpenHashSet set = new LongOpenHashSet();
    set.add(6);
    LongOpenHashSet deser = kryoSerDeser(set);
    deser.add(5);
    set.add(5);
    Assert.assertEquals(set, deser);
  }

  @Test
  public void testFastutilLongList() throws ClassNotFoundException, IOException {
    LongArrayList list = new LongArrayList();
    list.add(6);
    LongArrayList deser = kryoSerDeser(list);
    deser.add(5);
    list.add(5);
    Assert.assertEquals(list, deser);
  }

  @Test
  public void testWFastutilLongList() throws ClassNotFoundException, IOException {
    WLongArrayList list = new WLongArrayList();
    list.add(6);
    WLongArrayList deser = kryoSerDeser(list);
    deser.add(5);
    list.add(5);
    Assert.assertEquals(list, deser);
  }


  @Test
  public void testFastutilFloatList() throws ClassNotFoundException, IOException {
    FloatArrayList list = new FloatArrayList();
    list.add(6L);
    FloatArrayList deser = kryoSerDeser(list);
    deser.add(5L);
    list.add(5L);
    Assert.assertEquals(list, deser);
  }

  @Test
  public void testFastutilMap() throws ClassNotFoundException, IOException {
    Int2BooleanMap list = new Int2BooleanOpenHashMap();
    list.put(6, true);
    Int2BooleanMap deser = kryoSerDeser(list);
    deser.put(5, false);
    list.put(5, false);
    Assert.assertEquals(list, deser);
  }

  @Test
  public void testFastutil2ObjMap() throws ClassNotFoundException, IOException {
    Char2ObjectMap<IntWritable> list = new Char2ObjectOpenHashMap<>();
    list.put('a', new IntWritable(6));
    list.put('q', new IntWritable(7));
    list.put('w', new IntWritable(8));
    list.put('e', new IntWritable(9));
    list.put('r', new IntWritable(7));
    list.put('c', null);
    Char2ObjectMap<IntWritable> deser = kryoSerDeser(list);
    deser.put('b', null);
    list.put('b', null);

    Assert.assertEquals(list, deser);
  }

  @Test
  @Ignore("Long test used for profiling compiling speed")
  public void testLongFastutilListProfile() throws ClassNotFoundException, IOException {
    int n = 100;
    int rounds = 2000000;
    LongArrayList list = new LongArrayList(n);
    for (int i = 0; i < n; i++) {
      list.add(i);
    }

    for (int round = 0; round < rounds; round ++) {
      LongArrayList deser = kryoSerDeser(list);
      deser.add(round);
      list.add(round);
      Assert.assertEquals(list.size(), deser.size());
      Assert.assertArrayEquals(list.elements(), deser.elements());

      list.popLong();
    }
  }

  @Test(expected=RuntimeException.class)
  public void testRandom() throws ClassNotFoundException, IOException {
    kryoSerDeser(new Random()).nextBoolean();
  }

  private static class TestConf implements GiraphConfigurationSettable {
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    }
  }

  @Test(expected=RuntimeException.class)
  public void testConfiguration() throws ClassNotFoundException, IOException {
    kryoSerDeser(new Configuration());
  }

  @Test(expected=RuntimeException.class)
  public void testConfigurable() throws ClassNotFoundException, IOException {
    kryoSerDeser(new TestConf());
  }

  @Test(expected=RuntimeException.class)
  public void testVertexReceiver() throws ClassNotFoundException, IOException {
    kryoSerDeser(new NonKryoWritable() {
    });
  }


  @Test
  public void testBlacklistedClasses() throws ClassNotFoundException, IOException {
    Assert.assertEquals(kryoSerDeser(Random.class), Random.class);
    Assert.assertEquals(kryoSerDeser(TestConf.class), TestConf.class);
    Assert.assertEquals(kryoSerDeser(GiraphConfiguration.class), GiraphConfiguration.class);
  }

  @Test(expected=RuntimeException.class)
  public void testRecursive() throws ClassNotFoundException, IOException {
    kryoSerDeser(new KryoWritableWrapper<>(new Object())).get().hashCode();
  }

  @Test
  public void testNull() throws ClassNotFoundException, IOException {
    Assert.assertNull(kryoSerDeser(null));
  }
}
