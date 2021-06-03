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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

public class TestVarint {
  private long[] genLongs(int n) {
    long[] res = new long[n];
    for (int i = 0; i < n; i++) {
      res[i] = ThreadLocalRandom.current().nextLong();
    }
    return res;
  }

  private int[] genInts(int n) {
    int[] res = new int[n];
    for (int i = 0; i < n; i++) {
      res[i] = ThreadLocalRandom.current().nextInt();
    }
    return res;
  }

  private void writeLongs(DataOutput out, long[] array) throws IOException {
    for (int i = 0; i < array.length; i++) {
      Varint.writeSignedVarLong(array[i], out);
    }
  }

  private void writeInts(DataOutput out, int[] array) throws IOException {
    for (int i = 0; i < array.length; i++) {
      Varint.writeSignedVarInt(array[i], out);
    }
  }

  private void readLongs(DataInput in, long[] array) throws IOException {
    for (int i = 0; i < array.length; i++) {
      array[i] = Varint.readSignedVarLong(in);
    }
  }

  private void readInts(DataInput in, int[] array) throws IOException {
    for (int i = 0; i < array.length; i++) {
      array[i] = Varint.readSignedVarInt(in);
    }
  }

  private void testVarLong(long value) throws IOException {
    UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream();
    Varint.writeSignedVarLong(value, os);

    UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream(os.getByteArray());
    long newValue = Varint.readSignedVarLong(is);

    Assert.assertEquals(Varint.sizeOfSignedVarLong(value), os.getPos());
    Assert.assertEquals(value, newValue);

    if (value >= 0) {
      os = new UnsafeByteArrayOutputStream();
      Varint.writeUnsignedVarLong(value, os);
      is = new UnsafeByteArrayInputStream(os.getByteArray());
      newValue = Varint.readUnsignedVarLong(is);
      Assert.assertEquals(Varint.sizeOfUnsignedVarLong(value), os.getPos());
      Assert.assertEquals(value, newValue);
    }
  }

  private void testVarInt(int value) throws IOException {
    UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream();
    Varint.writeSignedVarInt(value, os);

    UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream(os.getByteArray());
    int newValue = Varint.readSignedVarInt(is);

    Assert.assertEquals(Varint.sizeOfSignedVarLong(value), os.getPos());
    Assert.assertEquals(value, newValue);

    if (value >= 0) {
      os = new UnsafeByteArrayOutputStream();
      Varint.writeUnsignedVarInt(value, os);
      is = new UnsafeByteArrayInputStream(os.getByteArray());
      newValue = Varint.readUnsignedVarInt(is);
      Assert.assertEquals(Varint.sizeOfUnsignedVarInt(value), os.getPos());
      Assert.assertEquals(value, newValue);
    }
  }

  @Test
  public void testVars() throws IOException {
    testVarLong(0);
    testVarLong(Long.MIN_VALUE);
    testVarLong(Long.MAX_VALUE);
    testVarLong(-123456789999l);
    testVarLong(12342356789999l);
    testVarInt(0);
    testVarInt(4);
    testVarInt(-1);
    testVarInt(1);
    testVarInt(Integer.MIN_VALUE);
    testVarInt(Integer.MAX_VALUE);
    testVarInt(Integer.MAX_VALUE - 1);
  }

  @Test
  public void testVarLongSmall() throws IOException {
    long[] array = new long[] {1, 2, 3, -5, 0, 12345678987l, Long.MIN_VALUE};
    UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream();
    writeLongs(os, array);

    long[] resArray = new long[array.length];
    UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream(os.getByteArray());
    readLongs(is, resArray);

    Assert.assertArrayEquals(array, resArray);
  }

  @Test
  public void testVarIntSmall() throws IOException {
    int[] array = new int[] {13, -2, 3, 0, 123456789, Integer.MIN_VALUE, Integer.MAX_VALUE};
    UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream();
    writeInts(os, array);

    int[] resArray = new int[array.length];
    UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream(os.getByteArray());
    readInts(is, resArray);

    Assert.assertArrayEquals(array, resArray);
  }

  @Test
  public void testVarLongLarge() throws IOException {
    int n = 1000000;
    long[] array = genLongs(n);
    UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream();

    long startTime = System.currentTimeMillis();
    writeLongs(os, array);
    long endTime = System.currentTimeMillis();
    System.out.println("Write time: " + (endTime - startTime) / 1000.0);

    long[] resArray = new long[array.length];
    UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream(os.getByteArray());
    startTime = System.currentTimeMillis();
    readLongs(is, resArray);
    endTime = System.currentTimeMillis();
    System.out.println("Read time: " + (endTime - startTime) / 1000.0);

    Assert.assertArrayEquals(array, resArray);
  }

  @Test
  public void testVarIntLarge() throws IOException {
    int n = 1000000;
    int[] array = genInts(n);
    UnsafeByteArrayOutputStream os = new UnsafeByteArrayOutputStream();

    long startTime = System.currentTimeMillis();
    writeInts(os, array);
    long endTime = System.currentTimeMillis();
    System.out.println("Write time: " + (endTime - startTime) / 1000.0);

    int[] resArray = new int[array.length];
    UnsafeByteArrayInputStream is = new UnsafeByteArrayInputStream(os.getByteArray());
    startTime = System.currentTimeMillis();
    readInts(is, resArray);
    endTime = System.currentTimeMillis();
    System.out.println("Read time: " + (endTime - startTime) / 1000.0);

    Assert.assertArrayEquals(array, resArray);
  }

  @Test
  public void testSmall() throws IOException {
    for (int i = -100000; i <= 100000; i++) {
      testVarInt(i);
      testVarLong(i);
    }
  }

}
