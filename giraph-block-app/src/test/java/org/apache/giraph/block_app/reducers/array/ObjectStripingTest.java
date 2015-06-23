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
package org.apache.giraph.block_app.reducers.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.giraph.block_app.reducers.array.HugeArrayUtils.ObjectStriping;
import org.junit.Test;

public class ObjectStripingTest {

  private void testStriping(int size, int splits) {
    ObjectStriping striping = new ObjectStriping(size, splits);

    int numPerSplit = size / splits;

    int prevSplitIndex = 0;
    int prevInsideIndex = -1;

    assertEquals(0, striping.getSplitStart(0));

    for (int i = 0; i < size; i++) {
      int splitIndex = striping.getSplitIndex(i);
      int insideIndex = striping.getInsideIndex(i);


      if (prevInsideIndex + 1 == striping.getSplitSize(prevSplitIndex)) {
        assertEquals(i, striping.getSplitStart(splitIndex));
        assertEquals(splitIndex, prevSplitIndex + 1);
        assertEquals(insideIndex, 0);
      } else {
        assertEquals(splitIndex, prevSplitIndex);
        assertEquals(insideIndex, prevInsideIndex + 1);
      }

      int splitSize = striping.getSplitSize(splitIndex);
      if (splitSize != numPerSplit && splitSize != numPerSplit + 1) {
        fail(splitSize + " " + numPerSplit);
      }
      prevSplitIndex = splitIndex;
      prevInsideIndex = insideIndex;
    }

    assertEquals(prevSplitIndex + 1, splits);
    assertEquals(prevInsideIndex + 1, striping.getSplitSize(prevSplitIndex));
  }

  @Test
  public void test() {
    testStriping(5, 5);
    testStriping(6, 5);
    testStriping(7, 5);
    testStriping(9, 5);
    testStriping(10, 5);
    testStriping(100, 5);
    testStriping(101, 5);
    testStriping(104, 5);
  }
}
