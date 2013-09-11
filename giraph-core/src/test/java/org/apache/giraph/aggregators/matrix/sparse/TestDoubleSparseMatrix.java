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

package org.apache.giraph.aggregators.matrix.sparse;

import static org.junit.Assert.assertEquals;

import org.apache.giraph.utils.WritableUtils;
import org.junit.Test;

public class TestDoubleSparseMatrix {
  private static double E = 0.0001f;

  @Test
  public void testVectorAdd() {
    // The default value should be 0
    DoubleSparseVector vec1 = new DoubleSparseVector();
    assertEquals(0.0, vec1.get(0), E);

    // Basic get/set
    vec1.set(0, 0.1);
    vec1.set(10, 1.4);
    assertEquals(0.1, vec1.get(0), E);
    assertEquals(0.0, vec1.get(5), E);
    assertEquals(1.4, vec1.get(10), E);

    // Add another vector
    DoubleSparseVector vec2 = new DoubleSparseVector();
    vec2.set(0, 0.5);
    vec2.set(5, 1.7);

    vec1.add(vec2);
    assertEquals(0.6, vec1.get(0), E);
    assertEquals(1.7, vec1.get(5), E);
    assertEquals(1.4, vec1.get(10), E);
    assertEquals(0.0, vec1.get(15), E);
  }

  @Test
  public void testVectorSerialize() throws Exception {
    int size = 100;

    // Serialize from
    DoubleSparseVector from = new DoubleSparseVector(size);
    from.set(0, 10.0);
    from.set(10, 5.0);
    from.set(12, 1.0);
    byte[] data = WritableUtils.writeToByteArray(from);

    // De-serialize to
    DoubleSparseVector to = new DoubleSparseVector();
    WritableUtils.readFieldsFromByteArray(data, to);

    // The vectors should be equal
    for (int i = 0; i < size; ++i) {
      assertEquals(from.get(i), to.get(i), E);
    }
  }
}
