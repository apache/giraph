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

import org.apache.giraph.aggregators.matrix.dense.DoubleDenseVector;
import static org.junit.Assert.assertEquals;

import org.apache.giraph.utils.WritableUtils;
import org.junit.Test;

public class TestDoubleDenseMatrix {
  private static double E = 0.0001f;

  @Test
  public void testVectorSingleton() {
    DoubleDenseVector vec1 = new DoubleDenseVector(10);
    vec1.set(0, 0.1);
    vec1.set(6, 1.4);

    DoubleDenseVector vec2 = new DoubleDenseVector();
    vec2.setSingleton(6, 1.0);
    vec1.add(vec2);
    assertEquals(2.4, vec1.get(6), E);

    vec2.setSingleton(15, 1.5);
    vec1.add(vec2);
    assertEquals(1.5, vec1.get(15), E);
  }

  @Test
  public void testVectorAdd() {
    // The default value should be 0
    DoubleDenseVector vec1 = new DoubleDenseVector(10);
    assertEquals(0.0, vec1.get(0), E);

    // Basic get/set
    vec1.set(0, 0.1);
    vec1.set(6, 1.4);
    assertEquals(0.1, vec1.get(0), E);
    assertEquals(0.0, vec1.get(4), E);
    assertEquals(1.4, vec1.get(6), E);
    assertEquals(0.0, vec1.get(15), E);

    // Add another vector
    DoubleDenseVector vec2 = new DoubleDenseVector(20);
    vec2.set(0, 0.5);
    vec2.set(5, 1.7);

    vec1.add(vec2);
    assertEquals(0.6, vec1.get(0), E);
    assertEquals(1.7, vec1.get(5), E);
    assertEquals(1.4, vec1.get(6), E);
    assertEquals(0.0, vec1.get(15), E);
  }

  @Test
  public void testVectorSerialize() throws Exception {
    int size = 100;

    // Serialize from
    DoubleDenseVector from = new DoubleDenseVector(size);
    from.set(0, 10.0);
    from.set(10, 5.0);
    from.set(12, 1.0);
    byte[] data = WritableUtils.writeToByteArray(from, from);

    // De-serialize to
    DoubleDenseVector to1 = new DoubleDenseVector();
    DoubleDenseVector to2 = new DoubleDenseVector();
    WritableUtils.readFieldsFromByteArray(data, to1, to2);

    // The vectors should be equal
    for (int i = 0; i < size; ++i) {
      assertEquals(from.get(i), to1.get(i), E);
      assertEquals(from.get(i), to2.get(i), E);
    }
  }

  @Test
  public void testVectorSerializeSingleton() throws Exception {
    DoubleDenseVector from = new DoubleDenseVector();
    from.setSingleton(3, 10.0);

    byte[] data = WritableUtils.writeToByteArray(from, from);

    DoubleDenseVector to1 = new DoubleDenseVector();
    DoubleDenseVector to2 = new DoubleDenseVector();
    WritableUtils.readFieldsFromByteArray(data, to1, to2);

    assertEquals(from.getSingletonIndex(), to1.getSingletonIndex());
    assertEquals(from.getSingletonIndex(), to2.getSingletonIndex());
    assertEquals(from.getSingletonValue(), to2.getSingletonValue(), E);
    assertEquals(from.getSingletonValue(), to2.getSingletonValue(), E);
  }
}
