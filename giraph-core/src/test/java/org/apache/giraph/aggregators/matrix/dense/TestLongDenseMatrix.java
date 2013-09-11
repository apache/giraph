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

package org.apache.giraph.aggregators.matrix.dense;

import org.apache.giraph.aggregators.matrix.dense.LongDenseVector;
import static org.junit.Assert.assertEquals;

import org.apache.giraph.utils.WritableUtils;
import org.junit.Test;

public class TestLongDenseMatrix {

  @Test
  public void testVectorSingleton() {
    LongDenseVector vec1 = new LongDenseVector(10);
    vec1.set(0, 1);
    vec1.set(6, 14);

    LongDenseVector vec2 = new LongDenseVector();
    vec2.setSingleton(6, 10);
    vec1.add(vec2);
    assertEquals(24, vec1.get(6));

    vec2.setSingleton(15, 15);
    vec1.add(vec2);
    assertEquals(15, vec1.get(15));
  }

  @Test
  public void testVectorAdd() {
    // The default value should be 0
    LongDenseVector vec1 = new LongDenseVector(10);
    assertEquals(0, vec1.get(0));

    // Basic get/set
    vec1.set(0, 1);
    vec1.set(6, 14);
    assertEquals(1, vec1.get(0));
    assertEquals(0, vec1.get(4));
    assertEquals(14, vec1.get(6));
    assertEquals(0, vec1.get(15));

    // Add another vector
    LongDenseVector vec2 = new LongDenseVector(20);
    vec2.set(0, 5);
    vec2.set(5, 17);

    vec1.add(vec2);
    assertEquals(6, vec1.get(0));
    assertEquals(17, vec1.get(5));
    assertEquals(14, vec1.get(6));
    assertEquals(0, vec1.get(15));
  }

  @Test
  public void testVectorSerialize() throws Exception {
    int size = 100;

    // Serialize from
    LongDenseVector from = new LongDenseVector(size);
    from.set(0, 100);
    from.set(10, 50);
    from.set(12, 10);
    byte[] data = WritableUtils.writeToByteArray(from, from);

    // De-serialize to
    LongDenseVector to1 = new LongDenseVector();
    LongDenseVector to2 = new LongDenseVector();
    WritableUtils.readFieldsFromByteArray(data, to1, to2);

    // The vectors should be equal
    for (int i = 0; i < size; ++i) {
      assertEquals(from.get(i), to1.get(i));
      assertEquals(from.get(i), to2.get(i));
    }
  }

  @Test
  public void testVectorSerializeSingleton() throws Exception {
    LongDenseVector from = new LongDenseVector();
    from.setSingleton(3, 10);

    byte[] data = WritableUtils.writeToByteArray(from, from);

    LongDenseVector to1 = new LongDenseVector();
    LongDenseVector to2 = new LongDenseVector();
    WritableUtils.readFieldsFromByteArray(data, to1, to2);

    assertEquals(from.getSingletonIndex(), to1.getSingletonIndex());
    assertEquals(from.getSingletonIndex(), to2.getSingletonIndex());
    assertEquals(from.getSingletonValue(), to2.getSingletonValue());
    assertEquals(from.getSingletonValue(), to2.getSingletonValue());
  }
}
