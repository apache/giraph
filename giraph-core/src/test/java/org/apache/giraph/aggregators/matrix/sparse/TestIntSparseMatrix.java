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

public class TestIntSparseMatrix {

  @Test
  public void testVectorAdd() {
    // The default value should be 0
    IntSparseVector vec1 = new IntSparseVector();
    assertEquals(0, vec1.get(0));

    // Basic get/set
    vec1.set(0, 1);
    vec1.set(10, 14);
    assertEquals(1, vec1.get(0));
    assertEquals(0, vec1.get(5));
    assertEquals(14, vec1.get(10));

    // Add another vector
    IntSparseVector vec2 = new IntSparseVector();
    vec2.set(0, 5);
    vec2.set(5, 17);

    vec1.add(vec2);
    assertEquals(6, vec1.get(0));
    assertEquals(17, vec1.get(5));
    assertEquals(14, vec1.get(10));
    assertEquals(0, vec1.get(15));
  }

  @Test
  public void testVectorSerialize() throws Exception {
    int size = 100;

    // Serialize from
    IntSparseVector from = new IntSparseVector(size);
    from.set(0, 10);
    from.set(10, 5);
    from.set(12, 1);
    byte[] data = WritableUtils.writeToByteArray(from);

    // De-serialize to
    IntSparseVector to = new IntSparseVector();
    WritableUtils.readFieldsFromByteArray(data, to);

    // The vectors should be equal
    for (int i = 0; i < size; ++i) {
      assertEquals(from.get(i), to.get(i));
    }
  }
}
