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
package org.apache.giraph.block_app.framework.block;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.giraph.block_app.framework.piece.AbstractPiece;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.function.Supplier;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * Tests repeatUntilBlock's correctness
 */
public class TestRepeatUntilBlock {

  public static final int REPEAT_TIMES = 5;

  private static final Supplier<Boolean> falseSupplier = new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return false;
      }
  };

  @Test
  public void testRepeatUntilBlockBasic() throws Exception {
    Piece piece1 = new Piece();
    Piece piece2 = new Piece();
    Block innerBlock = new SequenceBlock(piece1, piece2);
    Block repeatBlock = new RepeatUntilBlock(
      REPEAT_TIMES,
      innerBlock,
      falseSupplier
    );
    BlockTestingUtils.testIndependence(
      Iterables.concat(Collections.nCopies(REPEAT_TIMES, Arrays.asList(piece1, piece2))),
      repeatBlock);
  }

  @Test
  public void testNestedRepeatUntilBlock() throws Exception {
    Piece piece1 = new Piece();
    Piece piece2 = new Piece();
    Block innerBlock = new SequenceBlock(piece1, piece2);
    Block repeatBlock = new RepeatUntilBlock(
      REPEAT_TIMES,
      innerBlock,
      falseSupplier
    );
    BlockTestingUtils.testNestedRepeatBlock(
      Iterables.concat(Collections.nCopies(REPEAT_TIMES, Arrays.asList(piece1, piece2))),
      repeatBlock);
  }

  @Test
  public void testRepeatUntilBlockUnlimited() throws Exception {
    Block innerBlock = new SequenceBlock(new Piece());
    // Can't test with testIndependence - spin up our own test inline
    Supplier<Boolean> countingSupplier = new Supplier<Boolean>() {
      private int i = 0;

      @Override
      public Boolean get() {
        i++;
        return i > REPEAT_TIMES;
      }
    };
    Block repeatBlock = RepeatUntilBlock.unlimited(
      innerBlock,
      countingSupplier
    );
    int count = 0;
    Iterator<AbstractPiece> it = repeatBlock.iterator();
    while (it.hasNext()) {
      it.next();
      count++;
    }
    assertEquals("Count must be equal to REPEAT_TIMES", REPEAT_TIMES, count);
  }

}
