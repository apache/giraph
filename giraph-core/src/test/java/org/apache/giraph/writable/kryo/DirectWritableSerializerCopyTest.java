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

import org.apache.giraph.types.ops.collections.array.WDoubleArrayList;
import org.apache.giraph.writable.kryo.serializers.DirectWritableSerializer;
import org.apache.hadoop.io.DoubleWritable;
import org.junit.Assert;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;

public class DirectWritableSerializerCopyTest {
  @Test
  public void test1() {
    DoubleWritable value = new DoubleWritable(5.9999);
    DirectWritableSerializer<DoubleWritable> serializer = new DirectWritableSerializer<>();
    Kryo kryo = new Kryo();
    DoubleWritable copy = serializer.copy(kryo, value);
    Assert.assertEquals(value.get(), copy.get(), 0);
  }

  @Test
  public void test2() {
    WDoubleArrayList list = new WDoubleArrayList();
    list.addW(new DoubleWritable(0.11111111));
    list.addW(new DoubleWritable(1000.9));
    list.addW(new DoubleWritable(99999999.99999999));
    DirectWritableSerializer<WDoubleArrayList> serializer =
      new DirectWritableSerializer<>();
    Kryo kryo = new Kryo();
    WDoubleArrayList copy = serializer.copy(kryo, list);
    DoubleWritable reusable = new DoubleWritable();
    copy.getIntoW(0, reusable);
    Assert.assertEquals(0.11111111, reusable.get(), 0);
    copy.getIntoW(1, reusable);
    Assert.assertEquals(1000.9, reusable.get(), 0);
    copy.getIntoW(2, reusable);
    Assert.assertEquals(99999999.99999999, reusable.get(), 0);
  }
}
