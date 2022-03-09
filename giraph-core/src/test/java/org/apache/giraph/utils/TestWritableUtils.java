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

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test case for WritableUtils.
 */
public class TestWritableUtils {

  /**
   * Tests readList and writeList functions in writable utils.
   * @throws IOException
   */
  @Test
  public void testListSerialization() throws IOException {
    List<Writable> list = new ArrayList<>();
    list.add(new LongWritable(1));
    list.add(new LongWritable(2));
    list.add(null);
    list.add(new FloatWritable(3));
    list.add(new FloatWritable(4));
    list.add(new LongWritable(5));
    list.add(new LongWritable(6));

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    WritableUtils.writeList(list, dos);
    dos.close();

    byte[] data = bos.toByteArray();

    DataInputStream input =
        new DataInputStream(new ByteArrayInputStream(data));

    List<Writable> result = (List<Writable>) WritableUtils.readList(input);

    Assert.assertEquals(list, result);

  }

  @Test
  public void testIntArray() throws IOException {
    int[] array = new int[] {1, 2, 3, 4, 5};
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    WritableUtils.writeIntArray(array, dos);
    dos.close();

    byte[] data = bos.toByteArray();

    DataInputStream input =
        new DataInputStream(new ByteArrayInputStream(data));

    int[] result = WritableUtils.readIntArray(input);

    Assert.assertArrayEquals(array, result);
  }

  @Test
  public void testLongArray() throws IOException {
    long[] array = new long[] {1, 2, 3, 4, 5};
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    WritableUtils.writeLongArray(dos, array);
    dos.close();

    byte[] data = bos.toByteArray();

    DataInputStream input =
        new DataInputStream(new ByteArrayInputStream(data));

    long[] result = WritableUtils.readLongArray(input);

    Assert.assertArrayEquals(array, result);
  }



}
