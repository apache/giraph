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
