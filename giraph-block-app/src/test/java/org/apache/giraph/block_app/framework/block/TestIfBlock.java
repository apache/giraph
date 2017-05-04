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


import java.util.Arrays;

import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.function.Supplier;
import org.junit.Assert;
import org.junit.Test;

public class TestIfBlock {

  private static final Supplier<Boolean> TRUE_SUPPLIER = new Supplier<Boolean>() {
    @Override
    public Boolean get() {
      return true;
    }
  };

  private static final Supplier<Boolean> FALSE_SUPPLIER = new Supplier<Boolean>() {
    @Override
    public Boolean get() {
      return false;
    }
  };

  @Test
  // Test short-circuiting the if -> then
  public void testIfBlockThen() throws Exception {
    Piece piece1 = new Piece();
    Piece piece2 = new Piece();
    Block ifBlock = new IfBlock(
      TRUE_SUPPLIER,
      new SequenceBlock(piece1, piece2)
    );

    BlockTestingUtils.testIndependence(
        Arrays.asList(piece1, piece2),
        ifBlock);
    Assert.assertFalse(ifBlock.getPieceCount().isKnown());
  }

  @Test
  // Test short-circuiting the if -> else
  public void testIfBlockElse() throws Exception {
    Piece piece1 = new Piece();
    Piece piece2 = new Piece();
    Block ifBlock = new IfBlock(
      FALSE_SUPPLIER,
      new EmptyBlock(),
      new SequenceBlock(piece1, piece2)
    );

    BlockTestingUtils.testIndependence(
        Arrays.asList(piece1, piece2),
        ifBlock);
    Assert.assertFalse(ifBlock.getPieceCount().isKnown());
  }

  @Test
  public void testIfNestedInRepeat() throws Exception {
    Piece piece1 = new Piece();
    Piece piece2 = new Piece();
    Block ifBlock = new IfBlock(
      TRUE_SUPPLIER,
      new SequenceBlock(piece1, piece2)
    );

    BlockTestingUtils.testNestedRepeatBlock(
            Arrays.asList(piece1, piece2),
            ifBlock);
    Assert.assertFalse(ifBlock.getPieceCount().isKnown());
  }

  @Test
  public void testIfThenElsePieceCount() {
    Piece piece1 = new Piece();
    Piece piece2 = new Piece();
    Block ifBlock = new IfBlock(
        TRUE_SUPPLIER,
        piece1,
        piece2
    );
    Assert.assertTrue(ifBlock.getPieceCount().isKnown());
    Assert.assertEquals(1, ifBlock.getPieceCount().getCount());
  }
}
